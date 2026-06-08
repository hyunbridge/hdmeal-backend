//! `/skill/`, `/cache/healthcheck/` 핸들러.

use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use tower_http::limit::RequestBodyLimitLayer;

use crate::application::chatbot::types::{KakaoSkillRequest, KakaoSkillResponse};
use crate::error::HDMealError;
use crate::shared::observability::RequestContext;
use crate::shared::security::authorize_skill_token_hashed;
use crate::transport::http::auth::extract_token;
use crate::transport::http::dto::api::CacheHealthcheckResponse;
use crate::transport::http::RouterState;

const SKILL_BODY_LIMIT_BYTES: usize = 64 * 1024;

pub fn router() -> Router<RouterState> {
    let skill_route = post(skill).layer(RequestBodyLimitLayer::new(SKILL_BODY_LIMIT_BYTES));

    Router::new()
        .route("/skill", skill_route.clone())
        .route("/skill/", skill_route)
        .route("/cache/healthcheck", get(cache_healthcheck))
        .route("/cache/healthcheck/", get(cache_healthcheck))
}

async fn skill(
    State(state): State<RouterState>,
    _rc: RequestContext,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    Json(req): Json<KakaoSkillRequest>,
) -> Result<Response, HDMealError> {
    let token = extract_token(&headers, &query, state.ctx.config.debug);
    if !authorize_skill_token_hashed(token.as_deref(), &state.ctx.config.auth_token_hashes) {
        return Err(HDMealError::unauthorized("Unauthorized"));
    }
    let messages = state
        .ctx
        .chatbot
        .dispatch_internal(&req)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "skill dispatch failed");
            e
        })?;
    let resp: KakaoSkillResponse = KakaoSkillResponse::from_messages(messages);
    Ok((StatusCode::OK, Json(resp)).into_response())
}

async fn cache_healthcheck(
    State(state): State<RouterState>,
    _rc: RequestContext,
) -> Result<Response, HDMealError> {
    use crate::shared::freshness::is_fresh;
    use crate::transport::http::dto::api::CacheHealthStatus as CacheHealthcheckStatus;

    let today = crate::shared::timezone::today_kst_date();
    let timetable = match state
        .ctx
        .data
        .get_timetable_by_date(&today.format("%Y-%m-%d").to_string())
        .await
    {
        Ok(Some(t)) if is_fresh(t.created_at, state.ctx.config.cache_health_timetable_ttl) => {
            CacheHealthcheckStatus::Valid
        }
        Ok(Some(_)) => CacheHealthcheckStatus::Expired,
        Ok(None) => CacheHealthcheckStatus::NotFound,
        Err(_) => CacheHealthcheckStatus::NotFound,
    };

    let weather = match state.ctx.data.get_latest_weather().await {
        Ok(Some(w)) if is_fresh(w.created_at, state.ctx.config.cache_health_weather_ttl) => {
            CacheHealthcheckStatus::Valid
        }
        Ok(Some(_)) => CacheHealthcheckStatus::Expired,
        Ok(None) => CacheHealthcheckStatus::NotFound,
        Err(_) => CacheHealthcheckStatus::NotFound,
    };
    let water = match state.ctx.data.get_latest_water_temperature().await {
        Ok(Some(w)) if is_fresh(w.created_at, state.ctx.config.cache_health_water_temp_ttl) => {
            CacheHealthcheckStatus::Valid
        }
        Ok(Some(_)) => CacheHealthcheckStatus::Expired,
        Ok(None) => CacheHealthcheckStatus::NotFound,
        Err(_) => CacheHealthcheckStatus::NotFound,
    };

    let body = CacheHealthcheckResponse {
        timetable,
        weather,
        water_temperature: water,
    };
    Ok((StatusCode::OK, Json(body)).into_response())
}
