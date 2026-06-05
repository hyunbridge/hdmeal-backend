//! Warp 라우터 / 필터 조립.

pub mod app_api;
pub mod chatbot;
pub mod dto;

use std::sync::Arc;

use mongodb::Client as MongoClient;
use warp::http::HeaderValue;
use warp::Filter;

use crate::application::AppContext;
use crate::config::AppConfig;
use crate::shared::metrics::Metrics;
use crate::shared::observability::{
    inject_response_headers, request_context_filter, write_request_id_headers, RequestContext,
};

/// CORS preflight 처리. `Allow-Origin` 등을 환경 설정에 맞춰 응답.
pub fn cors_filter(config: &AppConfig) -> warp::cors::Cors {
    let mut cors = warp::cors()
        .allow_methods(vec![
            "GET", "HEAD", "PUT", "PATCH", "POST", "DELETE", "OPTIONS",
        ])
        .allow_headers(vec![
            "Content-Type",
            "Authorization",
            "X-HDMeal-Token",
            "X-Request-ID",
            "X-HDMeal-Req-ID",
            "X-HDMeal-ReqId",
            "traceparent",
            "tracestate",
            "baggage",
        ])
        .expose_headers(vec![
            "X-Request-ID",
            "X-HDMeal-Req-ID",
            "traceparent",
            "tracestate",
        ]);
    if config.allowed_origins.iter().any(|o| o == "*") {
        cors = cors.allow_any_origin();
    } else {
        for o in &config.allowed_origins {
            cors = cors.allow_origin(o.as_str());
        }
    }
    if config.allow_credentials {
        cors = cors.allow_credentials(true);
    }
    cors.build()
}

/// 보안 헤더 (HSTS, X-Content-Type-Options, X-Frame-Options, CSP, …) — Go 의
/// `middleware.SecureWithConfig` 와 동등. [`warp::reply::Response`] 에 직접 merge
/// 해서 사용합니다.
pub fn add_security_headers(headers: &mut warp::http::HeaderMap) {
    headers.insert(
        warp::http::header::STRICT_TRANSPORT_SECURITY,
        HeaderValue::from_static("max-age=31536000"),
    );
    headers.insert(
        warp::http::header::HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        warp::http::header::HeaderName::from_static("x-frame-options"),
        HeaderValue::from_static("DENY"),
    );
    headers.insert(
        warp::http::header::HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("default-src 'none'"),
    );
    headers.insert(
        warp::http::header::HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("no-referrer"),
    );
}

/// 모든 라우터를 합쳐 [`warp::Filter`] 를 만듭니다.
pub fn build_router(
    config: Arc<AppConfig>,
    ctx: Arc<AppContext>,
    metrics: Arc<Metrics>,
    mongo: Arc<MongoClient>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    // 공통: request context (request_id + parent_cx)
    let req_ctx = request_context_filter();

    // /healthz
    let healthz = warp::path("healthz")
        .and(warp::get())
        .and(req_ctx.clone())
        .and_then(handlers::healthz);

    // /livez — 프로세스 살아있음 (readiness 와 분리)
    let livez = warp::path("livez")
        .and(warp::get())
        .and(req_ctx.clone())
        .and_then(handlers::livez);

    // /readyz — Mongo ping 까지 통과해야 ready
    let mongo_for_ready = mongo.clone();
    let readyz = warp::path("readyz")
        .and(warp::get())
        .and(warp::any().map(move || mongo_for_ready.clone()))
        .and(req_ctx.clone())
        .and_then(handlers::readyz);

    // /metrics — Prometheus text format
    let metrics_for_scrape = metrics.clone();
    let metrics_route = warp::path("metrics")
        .and(warp::get())
        .and(warp::any().map(move || metrics_for_scrape.clone()))
        .and(req_ctx.clone())
        .and_then(handlers::metrics);

    // /api/app/* — RequestContext 를 route 안에서 받음
    let app = app_api::routes(ctx.clone());

    // /skill/, /user/settings/, /cache/healthcheck/ — 동일
    let bot = chatbot::routes(ctx.clone());

    let api = livez
        .or(readyz)
        .unify()
        .or(healthz)
        .unify()
        .or(metrics_route)
        .unify()
        .or(app)
        .unify()
        .or(bot)
        .unify();

    // 응답 헤더 부착 + 에러 envelope 매핑.
    fn add_headers<R: warp::reply::Reply>(reply: R) -> warp::reply::Response {
        let mut resp = reply.into_response();
        add_security_headers(resp.headers_mut());
        resp
    }
    let api = api.recover(crate::error::handle_rejection).map(add_headers);

    // 메트릭 기록: warp::log::custom 으로 status + path + method 받음.
    let metrics_for_log = metrics.clone();
    let api = api.with(warp::log::custom(move |info| {
        let path = info.path();
        let method = info.method().as_str();
        let status = info.status().as_u16();
        metrics_for_log.record_request(path, method, status);
    }));

    // CORS 는 가장 바깥에.
    let cors = cors_filter(&config);
    api.with(cors)
}

/// handlers 모듈 — healthz 같은 단순 핸들러.
pub mod handlers {
    use std::convert::Infallible;
    use std::sync::Arc;

    use mongodb::Client as MongoClient;
    use warp::http::StatusCode;
    use warp::reply::{json, with_header, with_status, Reply as _};

    use crate::shared::metrics::Metrics;
    use crate::shared::observability::RequestContext;
    use crate::transport::http::finalize_reply;

    pub async fn healthz(ctx: RequestContext) -> Result<warp::reply::Response, Infallible> {
        let body = serde_json::json!({"status": "ok"});
        let resp = with_status(json(&body), StatusCode::OK).into_response();
        Ok(finalize_reply(&ctx, resp))
    }

    /// Liveness — 프로세스가 살아있으면 200. 외부 의존성 무관.
    pub async fn livez(ctx: RequestContext) -> Result<warp::reply::Response, Infallible> {
        let body = serde_json::json!({"status": "alive"});
        let resp = with_status(json(&body), StatusCode::OK).into_response();
        Ok(finalize_reply(&ctx, resp))
    }

    /// Readiness — Mongo ping 까지 성공해야 ready (503 if not).
    pub async fn readyz(
        mongo: Arc<MongoClient>,
        ctx: RequestContext,
    ) -> Result<warp::reply::Response, Infallible> {
        let ping = mongo
            .database("admin")
            .run_command(mongodb::bson::doc! {"ping": 1})
            .await;
        match ping {
            Ok(_) => {
                let body = serde_json::json!({
                    "status": "ready",
                    "checks": {"mongo": "ok"},
                });
                let resp = with_status(json(&body), StatusCode::OK).into_response();
                Ok(finalize_reply(&ctx, resp))
            }
            Err(e) => {
                tracing::warn!(error = %e, "/readyz mongo ping failed");
                let body = serde_json::json!({
                    "status": "not_ready",
                    "checks": {"mongo": format!("error: {e}")},
                });
                let resp =
                    with_status(json(&body), StatusCode::SERVICE_UNAVAILABLE).into_response();
                Ok(finalize_reply(&ctx, resp))
            }
        }
    }

    /// Prometheus scrape endpoint.
    pub async fn metrics(
        m: Arc<Metrics>,
        ctx: RequestContext,
    ) -> Result<warp::reply::Response, Infallible> {
        let body = m.render();
        let resp = with_header(
            with_status(body, StatusCode::OK),
            "Content-Type",
            "text/plain; version=0.0.4",
        )
        .into_response();
        Ok(finalize_reply(&ctx, resp))
    }
}

/// `RequestContext` 를 받아 응답 직전 X-Request-ID / traceparent 등을 주입.
pub fn finalize_reply(ctx: &RequestContext, reply: warp::reply::Response) -> warp::reply::Response {
    let mut resp = reply;
    let headers = resp.headers_mut();
    write_request_id_headers(headers, &ctx.request_id);
    inject_response_headers(headers, &ctx.parent_cx);
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_security_headers_sets_expected_keys() {
        let mut headers = warp::http::HeaderMap::new();
        add_security_headers(&mut headers);

        assert_eq!(
            headers
                .get(warp::http::header::STRICT_TRANSPORT_SECURITY)
                .and_then(|v| v.to_str().ok()),
            Some("max-age=31536000"),
        );
        assert_eq!(
            headers
                .get(warp::http::header::HeaderName::from_static(
                    "x-content-type-options"
                ))
                .and_then(|v| v.to_str().ok()),
            Some("nosniff"),
        );
        assert_eq!(
            headers
                .get(warp::http::header::HeaderName::from_static(
                    "x-frame-options"
                ))
                .and_then(|v| v.to_str().ok()),
            Some("DENY"),
        );
        assert_eq!(
            headers
                .get(warp::http::header::HeaderName::from_static(
                    "content-security-policy"
                ))
                .and_then(|v| v.to_str().ok()),
            Some("default-src 'none'"),
        );
        assert_eq!(
            headers
                .get(warp::http::header::HeaderName::from_static(
                    "referrer-policy"
                ))
                .and_then(|v| v.to_str().ok()),
            Some("no-referrer"),
        );
    }

    #[test]
    fn cors_filter_allows_x_hdmeal_token_header() {
        let cfg = crate::config::AppConfig {
            allowed_origins: vec!["https://hdmeal.kr".to_string()],
            allow_credentials: false,
            ..test_config_minimal()
        };
        let cors = cors_filter(&cfg);
        let route = warp::options().map(|| "").with(cors);
        let resp = warp::test::request()
            .method("OPTIONS")
            .header("Origin", "https://hdmeal.kr")
            .header("Access-Control-Request-Method", "GET")
            .header(
                "Access-Control-Request-Headers",
                "x-hdmeal-token, content-type",
            )
            .reply(&route);
        let status = futures_executor::block_on(resp).status().as_u16();
        assert!(status < 500, "preflight should not 5xx: got {status}");
    }

    #[test]
    fn cors_filter_with_wildcard_origin() {
        let cfg = crate::config::AppConfig {
            allowed_origins: vec!["*".to_string()],
            allow_credentials: false,
            ..test_config_minimal()
        };
        let cors = cors_filter(&cfg);
        let _ = warp::options().map(|| "").with(cors);
    }

    fn test_config_minimal() -> crate::config::AppConfig {
        use std::time::Duration;
        use url::Url;
        crate::config::AppConfig {
            app_name: "hdmeal".to_string(),
            debug: false,
            port: 8080,
            mongodb_uri: "mongodb://localhost:27017".to_string(),
            mongodb_database: "hdmeal".to_string(),
            neis_openapi_token: "k".to_string(),
            atpt_ofcdc_sc_code: "o".to_string(),
            sd_schul_code: "s".to_string(),
            num_of_grades: 3,
            num_of_classes: 12,
            kma_api_key: "k".to_string(),
            kma_nx: 60,
            kma_ny: 127,
            seoul_data_token: "k".to_string(),
            auth_tokens: vec!["secret".to_string()],
            jwt_secret: "secret".to_string(),
            base_url: Url::parse("http://localhost").unwrap(),
            allowed_origins: vec![],
            allow_credentials: false,
            max_days_range: 31,
            app_version: "test".to_string(),
            app_build: 0,
            cache_health_timetable_ttl: Duration::from_secs(60),
            cache_health_weather_ttl: Duration::from_secs(60),
            cache_health_water_temp_ttl: Duration::from_secs(60),
            otel_endpoint: None,
            otel_service_name: "hdmeal".to_string(),
        }
    }
}
