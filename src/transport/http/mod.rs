//! Warp 라우터 / 필터 조립.

pub mod app_api;
pub mod chatbot;
pub mod dto;

use std::sync::Arc;

use warp::http::HeaderValue;
use warp::Filter;

use crate::application::AppContext;
use crate::config::AppConfig;
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
    headers.insert(
        warp::http::header::HeaderName::from_static("x-xss-protection"),
        HeaderValue::from_static("1; mode=block"),
    );
}

/// 모든 라우터를 합쳐 [`warp::Filter`] 를 만듭니다.
pub fn build_router(
    config: Arc<AppConfig>,
    ctx: Arc<AppContext>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    // 공통: request context (request_id + parent_cx)
    let req_ctx = request_context_filter();

    // /healthz
    let healthz = warp::path("healthz")
        .and(warp::get())
        .and(req_ctx.clone())
        .and_then(handlers::healthz);

    // /api/app/* — RequestContext 를 route 안에서 받음
    let app = app_api::routes(ctx.clone());

    // /skill/, /user/settings/, /cache/healthcheck/ — 동일
    let bot = chatbot::routes(ctx.clone());

    let api = healthz.or(app).unify().or(bot).unify();

    // 응답 헤더 부착 + 에러 envelope 매핑
    fn add_headers<R: warp::reply::Reply>(reply: R) -> warp::reply::Response {
        let mut resp = reply.into_response();
        add_security_headers(resp.headers_mut());
        resp
    }
    let api = api
        .recover(crate::error::handle_rejection)
        .map(add_headers)
        .with(warp::trace::request());

    // CORS 는 가장 바깥에.
    let cors = cors_filter(&config);
    api.with(cors)
}

/// handlers 모듈 — healthz 같은 단순 핸들러.
pub mod handlers {
    use std::convert::Infallible;

    use warp::http::StatusCode;
    use warp::reply::{json, with_status, Reply as _};

    use crate::shared::observability::RequestContext;
    use crate::transport::http::finalize_reply;

    pub async fn healthz(ctx: RequestContext) -> Result<warp::reply::Response, Infallible> {
        let body = serde_json::json!({"status": "ok"});
        let resp = with_status(json(&body), StatusCode::OK).into_response();
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
