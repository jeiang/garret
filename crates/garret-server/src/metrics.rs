//! Prometheus metrics on a dedicated internal listener (spec 08-observability).
//! The public listener never exposes them.

use std::time::Instant;

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
    routing::get,
};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

/// Bucket bounds from spec 08: bytes 64 KiB…8 GiB, latency 1 ms…60 s.
fn byte_buckets() -> Vec<f64> {
    (16..=33).map(|p| (1u64 << p) as f64).collect()
}

fn latency_buckets() -> Vec<f64> {
    vec![
        0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
    ]
}

pub fn install(service: &str) -> Result<PrometheusHandle> {
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_bytes".into()), &byte_buckets())?
        .set_buckets_for_metric(
            Matcher::Suffix("_duration_seconds".into()),
            &latency_buckets(),
        )?
        .install_recorder()
        .context("installing the Prometheus recorder")?;

    metrics::gauge!(
        "garret_build_info",
        "service" => service.to_owned(),
        "version" => env!("CARGO_PKG_VERSION"),
    )
    .set(1.0);
    Ok(handle)
}

/// Serves `/metrics` and `/healthz` on its own listener, forever.
pub async fn serve(handle: PrometheusHandle, listen: &str) -> Result<()> {
    let app = Router::new()
        .route(
            "/metrics",
            get(move || {
                let handle = handle.clone();
                async move { handle.render() }
            }),
        )
        .route("/healthz", get(|| async { "ok" }));

    let addr: std::net::SocketAddr = listen.parse().context("invalid metrics listen address")?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("metrics listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Per-request metrics with bounded labels only: the matched route template,
/// never the concrete path (a store path hash as a label would explode series).
pub async fn track_http(request: Request, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".into());
    let method = request.method().to_string();

    metrics::gauge!("garret_http_requests_in_flight").increment(1.0);
    let started = Instant::now();
    let response = next.run(request).await;
    metrics::gauge!("garret_http_requests_in_flight").decrement(1.0);

    let status = status_class(response.status().as_u16());
    metrics::counter!(
        "garret_http_requests_total",
        "route" => route.clone(), "method" => method.clone(), "status" => status,
    )
    .increment(1);
    metrics::histogram!(
        "garret_http_request_duration_seconds",
        "route" => route, "method" => method,
    )
    .record(started.elapsed());
    response
}

/// Status *class*, not code — one series per class keeps cardinality bounded.
fn status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_collapse_to_classes() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(307), "3xx");
        assert_eq!(status_class(429), "4xx");
        assert_eq!(status_class(503), "5xx");
    }

    #[test]
    fn buckets_span_the_ranges_the_spec_asks_for() {
        let bytes = byte_buckets();
        assert_eq!(bytes.first(), Some(&(64.0 * 1024.0)));
        assert_eq!(bytes.last(), Some(&(8.0 * 1024.0 * 1024.0 * 1024.0)));
        let latency = latency_buckets();
        assert_eq!(latency.first(), Some(&0.001));
        assert_eq!(latency.last(), Some(&60.0));
    }
}
