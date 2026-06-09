//! Axum 라우터 / 미들웨어 조립.

pub mod app_api;
pub mod auth;
pub mod chatbot;
pub mod dto;
pub mod user_settings;

use std::sync::Arc;

use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use mongodb::Client as MongoClient;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tower_http::set_header::SetResponseHeaderLayer;

use crate::application::AppContext;
use crate::config::AppConfig;
use crate::shared::metrics::Metrics;
use crate::shared::observability::{
    build_http_request_context, inject_response_headers, RequestContext,
};

/// 모든 핸들러에 주입되는 라우터 상태.
#[derive(Clone)]
pub struct RouterState {
    pub ctx: Arc<AppContext>,
    pub mongo: Arc<MongoClient>,
    pub metrics: Arc<Metrics>,
}

/// CORS preflight 처리. `Allow-Origin` 등을 환경 설정에 맞춰 응답.
pub fn cors_layer(config: &AppConfig) -> CorsLayer {
    let mut layer = CorsLayer::new()
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::HEAD,
            axum::http::Method::PUT,
            axum::http::Method::PATCH,
            axum::http::Method::POST,
            axum::http::Method::DELETE,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::header::AUTHORIZATION,
            HeaderName::from_static("x-hdmeal-token"),
            HeaderName::from_static("x-request-id"),
            HeaderName::from_static("x-hdmeal-req-id"),
            HeaderName::from_static("x-hdmeal-reqid"),
            HeaderName::from_static("traceparent"),
            HeaderName::from_static("tracestate"),
            HeaderName::from_static("baggage"),
        ])
        .expose_headers([
            HeaderName::from_static("x-request-id"),
            HeaderName::from_static("x-hdmeal-req-id"),
            HeaderName::from_static("traceparent"),
            HeaderName::from_static("tracestate"),
        ])
        .allow_credentials(config.allow_credentials);
    if config.allowed_origins.iter().any(|o| o == "*") {
        layer = layer.allow_origin(tower_http::cors::Any);
    } else {
        let origins: Vec<HeaderValue> = config
            .allowed_origins
            .iter()
            .filter_map(|o| HeaderValue::from_str(o).ok())
            .collect();
        layer = layer.allow_origin(origins);
    }
    layer
}

/// 보안 헤더 (HSTS, X-Content-Type-Options, X-Frame-Options, CSP, …) 상수 정의.
/// 에러 envelope (`HDMealError::into_response`) 에서도 호출되어
/// 4xx/5xx 응답에도 동일하게 부착됨.
pub fn add_security_headers(headers: &mut axum::http::HeaderMap) {
    headers.insert(
        axum::http::header::STRICT_TRANSPORT_SECURITY,
        HeaderValue::from_static("max-age=31536000"),
    );
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        HeaderName::from_static("x-frame-options"),
        HeaderValue::from_static("DENY"),
    );
    headers.insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("default-src 'none'"),
    );
    headers.insert(
        HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("no-referrer"),
    );
}

/// HeaderMap → [`RequestContext`]. 실제 구현은 `shared::observability`
/// 에 단일 출처로 존재합니다.
pub fn build_request_context(headers: &axum::http::HeaderMap) -> RequestContext {
    build_http_request_context(headers)
}

/// `RequestContext` 결정 → request.extensions 에 주입 → task-local scope 진입.
async fn request_id_middleware(mut req: axum::extract::Request, next: Next) -> Response {
    let rc = build_request_context(req.headers());
    req.extensions_mut().insert(rc.clone());
    let id = rc.request_id.clone();
    crate::shared::context::scope_request_id(id, next.run(req)).await
}

/// request_id / traceparent 을 응답 헤더에 주입.
async fn inject_observability_headers(req: axum::extract::Request, next: Next) -> Response {
    let rc = req
        .extensions()
        .get::<RequestContext>()
        .cloned()
        .unwrap_or_else(|| build_request_context(req.headers()));
    let mut response = next.run(req).await;
    let headers = response.headers_mut();
    crate::shared::observability::write_request_id_headers(headers, &rc.request_id);
    inject_response_headers(headers, &rc.parent_cx);
    response
}

/// `MatchedPath` 로 path 정규화, status + method 기록.
async fn metrics_recorder(
    axum::extract::State(state): axum::extract::State<RouterState>,
    req: axum::extract::Request,
    next: Next,
) -> Response {
    let method = req.method().as_str().to_owned();
    let path = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|m| m.as_str().to_owned())
        .unwrap_or_else(|| req.uri().path().to_owned());
    let response = next.run(req).await;
    state
        .metrics
        .record_request(&path, &method, response.status().as_u16());
    response
}

/// 404 envelope fallback.
async fn fallback_404() -> Response {
    let body = crate::error::ErrorEnvelope {
        detail: "요청한 경로를 찾을 수 없습니다.".to_string(),
        request_id: crate::shared::context::current_request_id()
            .unwrap_or_else(crate::shared::context::new_request_id),
    };
    let mut resp = (StatusCode::NOT_FOUND, Json(body)).into_response();
    add_security_headers(resp.headers_mut());
    resp
}

/// 모든 라우터를 합쳐 [`axum::Router`] 를 만듭니다.
///
/// [`axum::serve`] 가 받는 `M: Service<IncomingStream>` 트레이트 바운드는
/// `Router<()>` (즉 [`axum::Router::with_state`] 거친 결과) 에만 구현되어 있어,
/// 마지막에 `with_state(state)` 로 `Router<()>` 로 변환해 반환합니다.
pub fn build_router(
    config: Arc<AppConfig>,
    ctx: Arc<AppContext>,
    metrics: Arc<Metrics>,
    mongo: Arc<MongoClient>,
) -> Router {
    let state = RouterState {
        ctx: ctx.clone(),
        mongo,
        metrics: metrics.clone(),
    };

    // /api/app/* — RequestContext 를 route 안에서 받음
    let app_routes = app_api::router();

    // /skill/, /user/settings/, /cache/healthcheck/
    let bot_routes = chatbot::router().merge(user_settings::router());

    let api: Router<RouterState> = Router::new()
        .route("/healthz", get(handlers::healthz))
        .route("/livez", get(handlers::livez))
        .route("/readyz", get(handlers::readyz))
        .route("/metrics", get(handlers::metrics))
        .merge(app_routes)
        .merge(bot_routes)
        .fallback(fallback_404);

    // layer 추가 순서 = outermost → innermost.
    // request: CORS → security_headers → request_id → inject_observability → metrics → handler
    // response: handler → metrics → inject_observability → request_id → security_headers → CORS
    //
    // 보안 헤더는 `add_security_headers()` 가 에러 envelope 에도 부착되어
    // 있으나, 정상 응답에는 layer 로 일괄 적용하는 것이 더 깔끔하다.
    // `SetResponseHeaderLayer::overriding` 은 이미 존재하는 헤더를
    // 덮어쓰므로 이중 부착 걱정이 없다.
    let security = ServiceBuilder::new()
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::STRICT_TRANSPORT_SECURITY,
            HeaderValue::from_static("max-age=31536000"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("x-content-type-options"),
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("x-frame-options"),
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("content-security-policy"),
            HeaderValue::from_static("default-src 'none'"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("referrer-policy"),
            HeaderValue::from_static("no-referrer"),
        ));

    api.layer(middleware::from_fn_with_state(
        state.clone(),
        metrics_recorder,
    ))
    .layer(middleware::from_fn(inject_observability_headers))
    .layer(middleware::from_fn(request_id_middleware))
    .layer(security)
    .layer(cors_layer(&config))
    .with_state(state)
}

/// 단순 핸들러.
pub mod handlers {
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use axum::Json;
    use serde_json::json;

    use crate::shared::observability::RequestContext;

    /// X-Request-ID/traceparent 헤더 주입은 `inject_observability_headers`
    /// 미들웨어가 처리하므로 핸들러는 본문/상태만 반환.
    pub async fn healthz(_rc: RequestContext) -> Response {
        (StatusCode::OK, Json(json!({"status": "ok"}))).into_response()
    }

    /// Liveness — 프로세스가 살아있으면 200. 외부 의존성 무관.
    pub async fn livez(_rc: RequestContext) -> Response {
        (StatusCode::OK, Json(json!({"status": "alive"}))).into_response()
    }

    /// Readiness — Mongo ping 까지 성공해야 ready (503 if not).
    pub async fn readyz(State(state): State<super::RouterState>, _rc: RequestContext) -> Response {
        let ping = state
            .mongo
            .database("admin")
            .run_command(mongodb::bson::doc! {"ping": 1})
            .await;
        match ping {
            Ok(_) => {
                let body = json!({
                    "status": "ready",
                    "checks": {"mongo": "ok"},
                });
                (StatusCode::OK, Json(body)).into_response()
            }
            Err(e) => {
                tracing::warn!(error = %e, "/readyz mongo ping failed");
                let body = json!({
                    "status": "not_ready",
                    "checks": {"mongo": "error"},
                });
                (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
            }
        }
    }

    /// Prometheus scrape endpoint.
    pub async fn metrics(State(state): State<super::RouterState>, _rc: RequestContext) -> Response {
        let body = state.metrics.render();
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain; version=0.0.4")
            .body(Body::from(body))
            .expect("static response builder")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[test]
    fn add_security_headers_sets_expected_keys() {
        let mut headers = axum::http::HeaderMap::new();
        add_security_headers(&mut headers);

        assert_eq!(
            headers
                .get(axum::http::header::STRICT_TRANSPORT_SECURITY)
                .and_then(|v| v.to_str().ok()),
            Some("max-age=31536000"),
        );
        assert_eq!(
            headers
                .get(HeaderName::from_static("x-content-type-options"))
                .and_then(|v| v.to_str().ok()),
            Some("nosniff"),
        );
        assert_eq!(
            headers
                .get(HeaderName::from_static("x-frame-options"))
                .and_then(|v| v.to_str().ok()),
            Some("DENY"),
        );
        assert_eq!(
            headers
                .get(HeaderName::from_static("content-security-policy"))
                .and_then(|v| v.to_str().ok()),
            Some("default-src 'none'"),
        );
        assert_eq!(
            headers
                .get(HeaderName::from_static("referrer-policy"))
                .and_then(|v| v.to_str().ok()),
            Some("no-referrer"),
        );
    }

    fn test_config() -> AppConfig {
        use url::Url;
        AppConfig {
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
            auth_token_hashes: vec![crate::shared::security::hash_skill_token("secret")],
            jwt_secret: "secret".to_string(),
            base_url: Url::parse("http://localhost").unwrap(),
            allowed_origins: vec![],
            allow_credentials: false,
            max_days_range: 31,
            app_version: "test".to_string(),
            app_build: 0,
            cache_health_timetable_ttl: std::time::Duration::from_secs(60),
            cache_health_weather_ttl: std::time::Duration::from_secs(60),
            cache_health_water_temp_ttl: std::time::Duration::from_secs(60),
            otel_endpoint: None,
            otel_service_name: "hdmeal".to_string(),
        }
    }

    #[tokio::test]
    async fn cors_preflight_with_specific_origin() {
        let cfg = AppConfig {
            allowed_origins: vec!["https://hdmeal.kr".to_string()],
            ..test_config()
        };
        let app = Router::new()
            .route("/test", get(|| async { "" }))
            .layer(cors_layer(&cfg));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/test")
                    .header("Origin", "https://hdmeal.kr")
                    .header("Access-Control-Request-Method", "GET")
                    .header(
                        "Access-Control-Request-Headers",
                        "x-hdmeal-token, content-type",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status().as_u16();
        assert!(status < 500, "preflight should not 5xx: got {status}");
    }

    #[test]
    fn cors_layer_with_wildcard_origin() {
        let cfg = AppConfig {
            allowed_origins: vec!["*".to_string()],
            ..test_config()
        };
        let _ = cors_layer(&cfg);
    }

    #[tokio::test]
    async fn fallback_404_returns_not_found() {
        let app = Router::new().fallback(fallback_404);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/nonexistent-path")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["detail"], "요청한 경로를 찾을 수 없습니다.");
    }

    #[tokio::test]
    async fn fallback_404_through_middleware_chain_sets_headers_and_request_id() {
        let security = ServiceBuilder::new()
            .layer(SetResponseHeaderLayer::overriding(
                axum::http::header::STRICT_TRANSPORT_SECURITY,
                HeaderValue::from_static("max-age=31536000"),
            ))
            .layer(SetResponseHeaderLayer::overriding(
                HeaderName::from_static("x-content-type-options"),
                HeaderValue::from_static("nosniff"),
            ))
            .layer(SetResponseHeaderLayer::overriding(
                HeaderName::from_static("x-frame-options"),
                HeaderValue::from_static("DENY"),
            ))
            .layer(SetResponseHeaderLayer::overriding(
                HeaderName::from_static("content-security-policy"),
                HeaderValue::from_static("default-src 'none'"),
            ))
            .layer(SetResponseHeaderLayer::overriding(
                HeaderName::from_static("referrer-policy"),
                HeaderValue::from_static("no-referrer"),
            ));

        let app = Router::new()
            .route("/ok", get(|| async { "ok" }))
            .fallback(fallback_404)
            .layer(middleware::from_fn(inject_observability_headers))
            .layer(middleware::from_fn(request_id_middleware))
            .layer(security);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/anything")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let headers = resp.headers().clone();
        for key in [
            "strict-transport-security",
            "x-content-type-options",
            "x-frame-options",
            "content-security-policy",
            "referrer-policy",
            "x-request-id",
            "x-hdmeal-req-id",
        ] {
            assert!(headers.contains_key(key), "missing header: {key}");
        }

        let request_id = headers
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .to_string();
        assert_eq!(
            headers.get("x-hdmeal-req-id").and_then(|v| v.to_str().ok()),
            Some(request_id.as_str())
        );

        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["detail"], "요청한 경로를 찾을 수 없습니다.");
        assert_eq!(json["requestId"].as_str(), Some(request_id.as_str()));
    }
}
