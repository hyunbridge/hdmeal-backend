//! `/skill/`, `/user/settings/`, `/cache/healthcheck/` 핸들러.

use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::handler::Handler;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use tower_http::limit::RequestBodyLimitLayer;

use crate::application::chatbot::types::{KakaoSkillRequest, KakaoSkillResponse};
use crate::error::HDMealError;
use crate::shared::observability::RequestContext;
use crate::shared::security::{
    authorize_skill_token, scope, split_uid, validate_user_token, ValidateUserTokenInput,
};
use crate::transport::http::dto::api::CacheHealthcheckResponse;
use crate::transport::http::dto::user_settings::{
    UpdateUserSettingsRequest, UserSettingsMessage, UserSettingsPreferences, UserSettingsResponse,
};
use crate::transport::http::RouterState;

const SKILL_BODY_LIMIT_BYTES: usize = 64 * 1024;
const USER_SETTINGS_BODY_LIMIT_BYTES: usize = 8 * 1024;

pub fn router() -> Router<RouterState> {
    Router::new()
        .route(
            "/skill",
            post(skill).layer(RequestBodyLimitLayer::new(SKILL_BODY_LIMIT_BYTES)),
        )
        .route(
            "/user/settings",
            get(get_user_settings)
                .patch(
                    patch_user_settings
                        .layer(RequestBodyLimitLayer::new(USER_SETTINGS_BODY_LIMIT_BYTES)),
                )
                .delete(delete_user_settings),
        )
        .route("/cache/healthcheck", get(cache_healthcheck))
}

/// 통합 토큰 추출: 모든 인증 엔드포인트에서 동일한 우선순위로 토큰을 찾는다.
///
/// 우선순위 (보안상 안전한 순서):
///   1. `X-HDMeal-Token` 헤더
///   2. `Authorization: Bearer <token>` 헤더
///   3. `?token=` 쿼리 (proxy 로그에 남으므로 최후 수단)
fn extract_token(headers: &HeaderMap, query: &HashMap<String, String>) -> Option<String> {
    let candidates = || -> [Option<&str>; 3] {
        [
            headers.get("X-HDMeal-Token").and_then(|v| v.to_str().ok()),
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| {
                    let mut parts = s.trim().splitn(2, char::is_whitespace);
                    match (parts.next(), parts.next()) {
                        (Some(scheme), Some(token)) if scheme.eq_ignore_ascii_case("bearer") => {
                            Some(token)
                        }
                        _ => None,
                    }
                }),
            query.get("token").map(String::as_str),
        ]
    };
    candidates()
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map(str::to_owned)
}

async fn skill(
    State(state): State<RouterState>,
    _rc: RequestContext,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    Json(req): Json<KakaoSkillRequest>,
) -> Result<Response, HDMealError> {
    let token = extract_token(&headers, &query);
    if !authorize_skill_token(token.as_deref(), &state.ctx.config.auth_tokens) {
        return Err(HDMealError::unauthorized("Unauthorized"));
    }
    let messages = state.ctx.chatbot.dispatch_internal(&req).await?;
    let resp: KakaoSkillResponse = KakaoSkillResponse::from_messages(messages);
    Ok((StatusCode::OK, Json(resp)).into_response())
}

async fn get_user_settings(
    State(state): State<RouterState>,
    _rc: RequestContext,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, HDMealError> {
    let token = extract_token(&headers, &query)
        .ok_or_else(|| HDMealError::unauthorized("토큰이 없습니다."))?;
    let claims = validate_user_token(ValidateUserTokenInput {
        token: &token,
        secret: &state.ctx.config.jwt_secret,
        required_scope: scope::GET_USER_INFO,
    })?;
    let (platform, external_id) = split_uid(&claims.uid)?;
    let user = state
        .ctx
        .user_service
        .ensure_user(platform, external_id)
        .await?;
    let body = UserSettingsResponse {
        grades: (1..=state.ctx.config.num_of_grades as i32).collect(),
        classes: (1..=state.ctx.config.num_of_classes as i32).collect(),
        current_grade: user.grade,
        current_class: user.class_no,
        preferences: UserSettingsPreferences {
            allergy_info: if user.preferences.allergy_info.is_empty() {
                "Number".to_string()
            } else {
                user.preferences.allergy_info.clone()
            },
        },
    };
    Ok((StatusCode::OK, Json(body)).into_response())
}

async fn patch_user_settings(
    State(state): State<RouterState>,
    _rc: RequestContext,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    Json(req): Json<UpdateUserSettingsRequest>,
) -> Result<Response, HDMealError> {
    let token = extract_token(&headers, &query)
        .ok_or_else(|| HDMealError::unauthorized("토큰이 없습니다."))?;
    let claims = validate_user_token(ValidateUserTokenInput {
        token: &token,
        secret: &state.ctx.config.jwt_secret,
        required_scope: scope::MANAGE_USER_INFO,
    })?;
    let (platform, external_id) = split_uid(&claims.uid)?;
    let input = crate::application::user_service::UpdateUserInput {
        grade: Some(Some(req.user_grade)),
        class_no: Some(Some(req.user_class)),
        preferences: req.preferences,
    };
    state
        .ctx
        .user_service
        .update_user(
            platform,
            external_id,
            input,
            state.ctx.config.num_of_grades,
            state.ctx.config.num_of_classes,
        )
        .await?;
    let body = UserSettingsMessage {
        message: "저장했습니다.".to_string(),
    };
    Ok((StatusCode::OK, Json(body)).into_response())
}

async fn delete_user_settings(
    State(state): State<RouterState>,
    _rc: RequestContext,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, HDMealError> {
    let token = extract_token(&headers, &query)
        .ok_or_else(|| HDMealError::unauthorized("토큰이 없습니다."))?;
    let claims = validate_user_token(ValidateUserTokenInput {
        token: &token,
        secret: &state.ctx.config.jwt_secret,
        required_scope: scope::MANAGE_USER_INFO,
    })?;
    let (platform, external_id) = split_uid(&claims.uid)?;
    let ok = state
        .ctx
        .user_service
        .delete_user(platform, external_id)
        .await?;
    if !ok {
        return Err(HDMealError::not_found("사용자 정보가 없습니다."));
    }
    let body = UserSettingsMessage {
        message: "삭제했습니다.".to_string(),
    };
    Ok((StatusCode::OK, Json(body)).into_response())
}

async fn cache_healthcheck(
    State(state): State<RouterState>,
    _rc: RequestContext,
) -> Result<Response, HDMealError> {
    use crate::application::ingestion_service::is_fresh;
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn empty_query() -> HashMap<String, String> {
        HashMap::new()
    }

    #[test]
    fn extract_token_prefers_x_hdmeal_token_header() {
        let h = headers_with(&[("X-HDMeal-Token", "alpha")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q), Some("alpha".to_string()));
    }

    #[test]
    fn extract_token_falls_back_to_bearer_header() {
        let h = headers_with(&[("authorization", "Bearer beta")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q), Some("beta".to_string()));
    }

    #[test]
    fn extract_token_accepts_trimmed_lowercase_bearer_header() {
        let h = headers_with(&[("authorization", "  bEaReR   beta  ")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q), Some("beta".to_string()));
    }

    #[test]
    fn extract_token_falls_back_to_query() {
        let h = headers_with(&[]);
        let mut q = empty_query();
        q.insert("token".to_string(), "gamma".to_string());
        assert_eq!(extract_token(&h, &q), Some("gamma".to_string()));
    }

    #[test]
    fn extract_token_priority_header_over_query() {
        let h = headers_with(&[("X-HDMeal-Token", "alpha")]);
        let mut q = empty_query();
        q.insert("token".to_string(), "gamma".to_string());
        assert_eq!(extract_token(&h, &q), Some("alpha".to_string()));
    }

    #[test]
    fn extract_token_priority_bearer_over_query() {
        let h = headers_with(&[("authorization", "Bearer beta")]);
        let mut q = empty_query();
        q.insert("token".to_string(), "gamma".to_string());
        assert_eq!(extract_token(&h, &q), Some("beta".to_string()));
    }

    #[test]
    fn extract_token_trims_whitespace() {
        let h = headers_with(&[("X-HDMeal-Token", "  alpha  ")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q), Some("alpha".to_string()));
    }

    #[test]
    fn extract_token_rejects_empty() {
        let h = headers_with(&[("X-HDMeal-Token", "   ")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q), None);
    }

    #[test]
    fn extract_token_rejects_bearer_without_prefix() {
        let h = headers_with(&[("authorization", "Basic dXNlcjpwYXNz")]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q), None);
    }

    #[test]
    fn extract_token_returns_none_when_all_empty() {
        let h = headers_with(&[]);
        let q = empty_query();
        assert_eq!(extract_token(&h, &q), None);
    }
}
