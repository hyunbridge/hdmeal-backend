//! Tracing + OpenTelemetry 초기화 + 요청 컨텍스트.
//!
//! - [`init`] 가 `tracing-subscriber` 와 OTLP exporter 를 함께 부트스트랩.
//!   `OTEL_EXPORTER_OTLP_ENDPOINT` 가 비어있으면 OTel 은 비활성.
//! - [`RequestContext`] 는 `FromRequestParts` 를 구현해 핸들러가 extractor 로
//!   받을 수 있음. 들어오는 헤더에서 request ID 와 `traceparent` 를 추출/생성.
//! - 응답 헤더 부착은 `transport::http` 의 middleware 가 담당 (header layer).

use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::Sampler;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use std::sync::OnceLock;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::shared::context::{new_request_id, normalize_request_id};

static TRACE_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// 한 요청 동안 핸들러로 전달되는 컨텍스트.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub request_id: String,
    pub parent_cx: opentelemetry::Context,
}

/// OTel + tracing-subscriber 부트스트랩. 한 번만 호출.
pub fn init(app_name: &str, otel_endpoint: Option<&str>) -> anyhow::Result<()> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hdmeal_backend=debug"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_level(true);

    if let Some(endpoint) = otel_endpoint {
        let resource = Resource::builder_empty()
            .with_attributes(vec![
                KeyValue::new("service.name", app_name.to_string()),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            ])
            .build();

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(5))
            .build()?;

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_sampler(Sampler::AlwaysOn)
            .with_resource(resource)
            .build();

        let _ = TRACE_PROVIDER.set(provider.clone());
        let tracer = provider.tracer(app_name.to_string());
        global::set_tracer_provider(provider);
        global::set_text_map_propagator(TraceContextPropagator::new());

        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(otel_layer)
            .try_init()?;
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .try_init()?;
    }

    Ok(())
}

/// Shutdown 시 exporter flush.
pub fn shutdown() {
    if let Some(provider) = TRACE_PROVIDER.get() {
        let _ = provider.shutdown();
    }
}

/// 들어오는 헤더에서 parent OTel context 추출.
pub fn extract_parent_context(headers: &axum::http::HeaderMap) -> opentelemetry::Context {
    global::get_text_map_propagator(|prop| prop.extract(&HeaderExtractor(headers)))
}

/// 현재 span context 를 헤더에 inject (traceparent / tracestate).
pub fn inject_response_headers(headers: &mut axum::http::HeaderMap, cx: &opentelemetry::Context) {
    global::get_text_map_propagator(|prop| {
        prop.inject_context(cx, &mut HeaderMutInjector(headers))
    });
}

/// `X-Request-ID` 와 `X-HDMeal-Req-ID` 헤더에 같은 값 주입.
pub fn write_request_id_headers(headers: &mut axum::http::HeaderMap, request_id: &str) {
    if let Ok(v) = axum::http::HeaderValue::from_str(request_id) {
        headers.insert("X-Request-ID", v.clone());
        headers.insert("X-HDMeal-Req-ID", v);
    }
}

struct HeaderExtractor<'a>(&'a axum::http::HeaderMap);
impl<'a> opentelemetry::propagation::Extractor for HeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

struct HeaderMutInjector<'a>(&'a mut axum::http::HeaderMap);
impl<'a> opentelemetry::propagation::Injector for HeaderMutInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(v)) = (
            key.parse::<axum::http::HeaderName>(),
            axum::http::HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, v);
        }
    }
}

/// axum extractor: 핸들러 시그니처에서 `rc: RequestContext` 로 받는다.
impl<S> axum::extract::FromRequestParts<S> for RequestContext
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        let headers = parts.headers.clone();
        async move {
            let raw = headers
                .get("X-Request-ID")
                .or_else(|| headers.get("X-HDMeal-Req-ID"))
                .or_else(|| headers.get("X-HDMeal-ReqId"))
                .and_then(|v| v.to_str().ok())
                .and_then(normalize_request_id)
                .unwrap_or_else(new_request_id);

            let parent_cx = extract_parent_context(&headers);
            Ok(RequestContext {
                request_id: raw,
                parent_cx,
            })
        }
    }
}
