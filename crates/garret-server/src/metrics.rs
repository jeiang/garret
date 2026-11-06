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

/// Bucket bounds from spec 08: bytes 64 KiB…8 GiB, latency 100 us…15 min.
fn byte_buckets() -> Vec<f64> {
    (16..=33).map(|p| (1u64 << p) as f64).collect()
}

fn latency_buckets() -> Vec<f64> {
    vec![
        0.0001, 0.00025, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        30.0, 60.0, 120.0, 300.0, 900.0,
    ]
}

/// Installs the global Prometheus recorder with the spec's bucket bounds and
/// sets `garret_build_info`. Call once, before any metric is touched.
pub fn install(service: &str) -> Result<PrometheusHandle> {
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_bytes".into()), &byte_buckets())?
        .set_buckets_for_metric(Matcher::Suffix("_wait_seconds".into()), &latency_buckets())?
        .set_buckets_for_metric(
            Matcher::Suffix("_duration_seconds".into()),
            &latency_buckets(),
        )?
        .set_buckets_for_metric(
            Matcher::Full("garret_negotiation_batch_size".into()),
            &[
                1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
            ],
        )?
        .set_buckets_for_metric(
            Matcher::Full("garret_negotiation_missing_ratio".into()),
            &[0.0, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0],
        )?
        .install_recorder()
        .context("installing the Prometheus recorder")?;

    metrics::gauge!(
        "garret_build_info",
        "service" => service.to_owned(),
        "version" => env!("CARGO_PKG_VERSION"),
        "revision" => option_env!("GARRET_REVISION").unwrap_or("unknown"),
    )
    .set(1.0);
    // Captured here, not read from `/proc` (`/proc/stat` is hidden under
    // `ProcSubset=pid`), so it is exported on every platform.
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    metrics::gauge!("process_start_time_seconds").set(started);
    Ok(handle)
}

/// Serves `/metrics` and `/healthz` on the pre-bound `listener`, forever.
/// Binding is the caller's job, so a bad address or a taken port fails
/// startup rather than a detached task.
pub(crate) async fn serve(
    handle: PrometheusHandle,
    listener: tokio::net::TcpListener,
) -> Result<()> {
    let app = Router::new()
        .route(
            "/metrics",
            get(move || {
                let handle = handle.clone();
                async move {
                    record_process_metrics();
                    handle.render()
                }
            }),
        )
        .route("/healthz", get(|| async { "ok" }));

    crate::runtime::serve(listener, app, std::future::pending()).await?;
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
    let method = method_label(request.method());

    let started = Instant::now();
    let response = {
        let _in_flight = InFlight::enter();
        next.run(request).await
    };

    let status = status_class(response.status().as_u16());
    metrics::counter!(
        "garret_http_requests_total",
        "route" => route.clone(), "method" => method, "status" => status,
    )
    .increment(1);
    metrics::histogram!(
        "garret_http_request_duration_seconds",
        "route" => route, "method" => method,
    )
    .record(started.elapsed());
    response
}

/// Counts a request in `garret_http_requests_in_flight` while it lives, so a
/// handler future dropped mid-request (client disconnect) still gives it back.
struct InFlight;

impl InFlight {
    fn enter() -> Self {
        metrics::gauge!("garret_http_requests_in_flight").increment(1.0);
        Self
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        metrics::gauge!("garret_http_requests_in_flight").decrement(1.0);
    }
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

/// Bounded method label: anything but the standard five collapses to `OTHER`,
/// so a client sending arbitrary extension methods cannot mint series.
fn method_label(method: &axum::http::Method) -> &'static str {
    match *method {
        axum::http::Method::GET => "GET",
        axum::http::Method::HEAD => "HEAD",
        axum::http::Method::POST => "POST",
        axum::http::Method::PUT => "PUT",
        axum::http::Method::DELETE => "DELETE",
        _ => "OTHER",
    }
}

/// Refreshes the standard `process_*` metrics from `/proc` (Linux; elsewhere
/// nothing is readable, so nothing is recorded). Called on every `/metrics`
/// render, so there is no background task. Anything unreadable is skipped,
/// never an error.
fn record_process_metrics() {
    // USER_HZ is 100 on every Linux target this runs on.
    const TICKS_PER_SEC: f64 = 100.0;
    use std::fs;

    if let Ok(stat) = fs::read_to_string("/proc/self/stat") {
        // Fields after the parenthesised comm, which may itself contain spaces.
        if let Some((_, rest)) = stat.rsplit_once(')') {
            let f: Vec<&str> = rest.split_whitespace().collect();
            let ticks = |i: usize| f.get(i).and_then(|v| v.parse::<f64>().ok());
            if let (Some(utime), Some(stime)) = (ticks(11), ticks(12)) {
                metrics::gauge!("process_cpu_seconds_total").set((utime + stime) / TICKS_PER_SEC);
            }
        }
    }
    let status_kb = fs::read_to_string("/proc/self/status").ok().and_then(|s| {
        s.lines()
            .find_map(|l| l.strip_prefix("VmRSS:"))
            .and_then(|v| v.split_whitespace().next()?.parse::<f64>().ok())
    });
    if let Some(kb) = status_kb {
        metrics::gauge!("process_resident_memory_bytes").set(kb * 1024.0);
    }
    if let Ok(dir) = fs::read_dir("/proc/self/fd") {
        metrics::gauge!("process_open_fds").set(dir.count() as f64);
    }
    let max_fds = fs::read_to_string("/proc/self/limits").ok().and_then(|s| {
        s.lines()
            .find_map(|l| l.strip_prefix("Max open files"))
            .and_then(|v| v.split_whitespace().next()?.parse::<f64>().ok())
    });
    if let Some(max) = max_fds {
        metrics::gauge!("process_max_fds").set(max);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Labels stay bounded: the route template, not the path; the
    /// status class, not the code; `OTHER` for an extension method.
    #[tokio::test]
    async fn request_labels_are_bounded() {
        use tower::ServiceExt;
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let app = Router::new()
            .route(
                "/item/{id}",
                axum::routing::any(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }),
            )
            .layer(axum::middleware::from_fn(track_http));
        let request = axum::http::Request::builder()
            .method(axum::http::Method::from_bytes(b"BREW").unwrap())
            .uri("/item/0123456789abcdef")
            .body(axum::body::Body::empty())
            .unwrap();
        app.oneshot(request).await.unwrap();
        let out = handle.render();
        assert!(
            out.contains(
                r#"garret_http_requests_total{route="/item/{id}",method="OTHER",status="5xx"} 1"#
            ),
            "{out}"
        );
        assert!(!out.contains("0123456789abcdef"), "{out}");
    }

    #[test]
    fn render_has_extended_and_negotiation_buckets() {
        let handle = install("test").unwrap();
        metrics::histogram!("garret_x_duration_seconds").record(1.0);
        metrics::histogram!("garret_negotiation_batch_size").record(3.0);
        metrics::histogram!("garret_negotiation_missing_ratio").record(0.5);
        let out = handle.render();
        assert!(out.contains("garret_x_duration_seconds_bucket{le=\"900\"}"));
        assert!(out.contains("garret_negotiation_batch_size_bucket{le=\"10000\"}"));
        assert!(out.contains("garret_negotiation_missing_ratio_bucket{le=\"0.75\"}"));
    }

    /// A request future dropped mid-flight (client gone) leaves the gauge at 0.
    #[tokio::test]
    async fn dropped_request_gives_back_in_flight_count() {
        use tower::ServiceExt;
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let app = Router::new()
            .route("/hang", get(std::future::pending::<&'static str>))
            .layer(axum::middleware::from_fn(track_http));
        let request = || {
            axum::http::Request::get("/hang")
                .body(axum::body::Body::empty())
                .unwrap()
        };
        let timed_out = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            app.clone().oneshot(request()),
        )
        .await;
        assert!(timed_out.is_err());
        assert!(
            handle.render().contains("garret_http_requests_in_flight 0"),
            "{}",
            handle.render()
        );
    }
}
