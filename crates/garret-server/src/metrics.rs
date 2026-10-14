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

/// Installs the global Prometheus recorder with the spec's bucket bounds and
/// sets `garret_build_info`. Call once, before any metric is touched.
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
        "revision" => option_env!("GARRET_REVISION").unwrap_or("unknown"),
    )
    .set(1.0);
    Ok(handle)
}

/// Serves `/metrics` and `/healthz` on the pre-bound `listener`, forever.
/// Binding is the caller's job, so a bad address or a taken port fails
/// startup rather than a detached task.
pub async fn serve(handle: PrometheusHandle, listener: tokio::net::TcpListener) -> Result<()> {
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

/// Refreshes the standard `process_*` metrics from `/proc` (Linux only; a
/// no-op elsewhere). Called on every `/metrics` render, so there is no
/// background task. Anything unreadable is skipped, never an error.
#[cfg(target_os = "linux")]
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
                metrics::gauge!("process_cpu_seconds_total")
                    .set((utime + stime) / TICKS_PER_SEC);
            }
            let boot = fs::read_to_string("/proc/stat").ok().and_then(|s| {
                s.lines()
                    .find_map(|l| l.strip_prefix("btime "))
                    .and_then(|v| v.trim().parse::<f64>().ok())
            });
            if let (Some(start), Some(boot)) = (ticks(19), boot) {
                metrics::gauge!("process_start_time_seconds").set(boot + start / TICKS_PER_SEC);
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

#[cfg(not(target_os = "linux"))]
fn record_process_metrics() {}

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

    #[test]
    fn extension_methods_collapse_to_other() {
        use axum::http::Method;
        assert_eq!(method_label(&Method::GET), "GET");
        assert_eq!(method_label(&Method::DELETE), "DELETE");
        let custom = Method::from_bytes(b"BREW").unwrap();
        assert_eq!(method_label(&custom), "OTHER");
        assert_eq!(method_label(&Method::PATCH), "OTHER");
    }
}
