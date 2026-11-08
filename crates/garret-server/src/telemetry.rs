//! Optional OpenTelemetry span export: the `tracing` spans the services
//! already emit, bridged to OTel spans and posted to a collector over
//! OTLP/HTTP.
//!
//! Nothing here runs unless `[telemetry] enabled = true`. With it off there is
//! no exporter, no background task and no propagator, and the only cost is the
//! spans themselves — one per request or operation, never per chunk or byte.

use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
};
use opentelemetry::{propagation::Extractor, trace::TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracer, SdkTracerProvider},
};
use tracing::Instrument;

use crate::config::TelemetryConfig;

/// How long one export attempt may take. The batch processor exports on its
/// own thread, so a collector that is down or slow delays spans at worst; it
/// never delays a request.
const EXPORT_TIMEOUT: Duration = Duration::from_secs(5);

/// The span exporter, if one is configured.
///
/// Dropping it without [`Telemetry::shutdown`] loses whatever the batch
/// processor has not sent yet, so a service that exits cleanly shuts it down.
pub struct Telemetry {
    provider: Option<SdkTracerProvider>,
}

impl Telemetry {
    /// No exporter: the process behaves exactly as it does without the
    /// `[telemetry]` section.
    pub fn disabled() -> Self {
        Self { provider: None }
    }

    /// Builds the OTLP exporter and tracer provider when `config` enables
    /// them, and installs the W3C trace-context propagator so an incoming
    /// `traceparent` joins the caller's trace.
    ///
    /// A misconfigured endpoint fails startup, like every other config error;
    /// an unreachable collector does not — export failures are logged and the
    /// spans are dropped.
    pub fn start(config: Option<&TelemetryConfig>, service: &str) -> Result<Self> {
        let Some(config) = config.filter(|config| config.enabled) else {
            return Ok(Self::disabled());
        };
        // The exporter takes the endpoint as the full URL, not the base the
        // OTLP spec's environment variables use, so the signal path is added
        // here.
        let endpoint = format!("{}/v1/traces", config.endpoint.trim_end_matches('/'));
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_timeout(EXPORT_TIMEOUT)
            .build()
            .context("building the OTLP span exporter")?;
        let provider = SdkTracerProvider::builder()
            .with_resource(
                Resource::builder()
                    .with_service_name(config.service_name(service))
                    .build(),
            )
            // ParentBased: a trace is sampled or not as a whole, so a caller's
            // decision (and its `traceparent` flags) is never overridden
            // halfway down.
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                config.sample_ratio,
            ))))
            .with_batch_exporter(exporter)
            .build();
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        Ok(Self {
            provider: Some(provider),
        })
    }

    /// Whether spans are exported, so a caller can skip work that only
    /// matters with a collector attached.
    pub fn enabled(&self) -> bool {
        self.provider.is_some()
    }

    /// The tracer the `tracing` layer bridges into, named after the service.
    pub fn tracer(&self, service: &str) -> Option<SdkTracer> {
        self.provider
            .as_ref()
            .map(|provider| provider.tracer(service.to_owned()))
    }

    /// Flushes queued spans and stops the exporter. Bounded by
    /// [`EXPORT_TIMEOUT`], so a dead collector cannot hold up shutdown.
    ///
    /// Takes `self`: the exporter owns a blocking `reqwest` client, which
    /// carries a private runtime, and shutting it down or dropping it from a
    /// thread inside the async runtime panics ("cannot drop a runtime in a
    /// context where blocking is not allowed"). Both happen on a plain
    /// thread instead.
    pub fn shutdown(mut self) {
        self.stop();
    }

    /// Shuts the provider down on a plain thread and forgets it, so the
    /// `Drop` that follows has nothing left to do.
    fn stop(&mut self) {
        let Some(provider) = self.provider.take() else {
            return;
        };
        let flushed = std::thread::spawn(move || {
            let flushed = provider.shutdown();
            // Dropped here, on the same plain thread, for the reason above.
            drop(provider);
            flushed
        })
        .join();
        match flushed {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!("flushing spans on shutdown: {e}"),
            Err(_) => tracing::warn!("the span exporter's flush thread panicked"),
        }
    }
}

/// A service that fails to start after `runtime::start` returns drops this on
/// an async worker thread (`admin::bind`, `db::open`, a signing key that will
/// not load). Without this the blocking client would be dropped there and
/// panic, hiding the real error.
impl Drop for Telemetry {
    fn drop(&mut self) {
        self.stop();
    }
}

/// One server span per HTTP request, named `{method} {route}` as the OTel
/// HTTP conventions ask, carrying the method, matched route, path and
/// response status under their semantic-convention names.
///
/// The matched route — not the concrete path — is the name, so a store hash
/// never becomes a span name; unmatched requests (a 404) are named
/// `{method} unmatched` and carry their path as `url.path`. A request
/// carrying `traceparent`/`tracestate` is parented to the caller's span, so a
/// push or a pull shows up inside the client's trace.
pub async fn track_http(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_owned());
    // The matched route, not the concrete path: a store hash in a span name
    // is one name per object, which is unbounded cardinality. Unmatched
    // requests (a 404) get a fixed name and keep their path as `url.path`.
    let name = match &route {
        Some(route) => format!("{method} {route}"),
        None => format!("{method} unmatched"),
    };

    // The caller's context is attached only while the span is created: that
    // is where the layer picks a parent, and from then on the span's own
    // context is what the request runs under.
    let span = {
        let parent = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&HeaderCarrier(request.headers()))
        });
        let _guard = parent.attach();
        tracing::debug_span!(
            "http.server.request",
            otel.name = name.as_str(),
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
            http.request.method = %method,
            http.route = tracing::field::Empty,
            url.path = %path,
            http.response.status_code = tracing::field::Empty,
        )
    };
    if let Some(route) = &route {
        span.record("http.route", route.as_str());
    }

    let response = next.run(request).instrument(span.clone()).await;
    let status = response.status().as_u16();
    span.record("http.response.status_code", status);
    if status >= 500 {
        span.record("otel.status_code", "error");
    }
    response
}

/// Reads the W3C trace-context headers off an incoming request.
struct HeaderCarrier<'a>(&'a axum::http::HeaderMap);

impl Extractor for HeaderCarrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Without `enabled = true` nothing is built: no exporter, no tracer.
    #[test]
    fn disabled_telemetry_builds_nothing() {
        let absent = Telemetry::start(None, "pusher").unwrap();
        assert!(!absent.enabled());
        assert!(absent.tracer("pusher").is_none());

        let off: TelemetryConfig = toml::from_str("enabled = false").unwrap();
        let off = Telemetry::start(Some(&off), "pusher").unwrap();
        assert!(!off.enabled());
        off.shutdown();
    }

    /// Enabling it builds a tracer named after the service, and an endpoint
    /// that cannot be parsed fails startup rather than exporting nowhere
    /// silently.
    #[test]
    fn enabled_telemetry_builds_a_tracer() {
        let on: TelemetryConfig =
            toml::from_str("enabled = true\nendpoint = \"http://127.0.0.1:4318\"").unwrap();
        let telemetry = Telemetry::start(Some(&on), "puller").unwrap();
        assert!(telemetry.enabled());
        assert!(telemetry.tracer("puller").is_some());
        telemetry.shutdown();

        let bad: TelemetryConfig =
            toml::from_str("enabled = true\nendpoint = \"not a url\"").unwrap();
        assert!(Telemetry::start(Some(&bad), "puller").is_err());
    }
}
