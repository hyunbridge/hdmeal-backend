//! `/skill/`, `/user/settings/`, `/cache/healthcheck/` 핸들러.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use warp::http::StatusCode;
use warp::reply::{json, with_status, Reply as _};
use warp::Filter;

use crate::application::chatbot::types::{KakaoSkillRequest, KakaoSkillResponse};
use crate::application::AppContext;
use crate::error::HDMealError;
use crate::shared::security::{
    authorize_skill_token, scope, split_uid, validate_user_token, ValidateUserTokenInput,
};
use crate::transport::http::dto::api::CacheHealthcheckResponse;
use crate::transport::http::dto::user_settings::{
    UpdateUserSettingsRequest, UserSettingsMessage, UserSettingsPreferences, UserSettingsResponse,
};

pub fn routes(
    ctx: Arc<AppContext>,
) -> impl Filter<Extract = (warp::reply::Response,), Error = warp::Rejection> + Clone {
    use crate::shared::observability::request_context_filter;
    let ctx_skill = ctx.clone();
    let ctx_get = ctx.clone();
    let ctx_patch = ctx.clone();
    let ctx_delete = ctx.clone();
    let ctx_health = ctx.clone();

    let skill = warp::path("skill")
        .and(warp::path::end())
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::any().map(move || ctx_skill.clone()))
        .and(warp::header::headers_cloned())
        .and(warp::query::<HashMap<String, String>>())
        .and(request_context_filter())
        .and_then(move |req, ctx, headers, query, rc| async move {
            handle_skill(ctx, headers, query, rc, req).await
        });

    let get_user = warp::path("user")
        .and(warp::path("settings"))
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::any().map(move || ctx_get.clone()))
        .and(warp::header::headers_cloned())
        .and(warp::query::<HashMap<String, String>>())
        .and(request_context_filter())
        .and_then(move |ctx, headers, query, rc| async move {
            handle_get_user_settings(ctx, headers, query, rc).await
        });

    let patch_user = warp::path("user")
        .and(warp::path("settings"))
        .and(warp::path::end())
        .and(warp::patch())
        .and(warp::body::json())
        .and(warp::any().map(move || ctx_patch.clone()))
        .and(warp::header::headers_cloned())
        .and(warp::query::<HashMap<String, String>>())
        .and(request_context_filter())
        .and_then(move |req, ctx, headers, query, rc| async move {
            handle_patch_user_settings(ctx, headers, query, rc, req).await
        });

    let delete_user = warp::path("user")
        .and(warp::path("settings"))
        .and(warp::path::end())
        .and(warp::delete())
        .and(warp::any().map(move || ctx_delete.clone()))
        .and(warp::header::headers_cloned())
        .and(warp::query::<HashMap<String, String>>())
        .and(request_context_filter())
        .and_then(move |ctx, headers, query, rc| async move {
            handle_delete_user_settings(ctx, headers, query, rc).await
        });

    let cache_health = warp::path("cache")
        .and(warp::path("healthcheck"))
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::any().map(move || ctx_health.clone()))
        .and(request_context_filter())
        .and_then(move |ctx, rc| async move { handle_cache_healthcheck(ctx, rc).await });

    skill
        .or(get_user)
        .unify()
        .or(patch_user)
        .unify()
        .or(delete_user)
        .unify()
        .or(cache_health)
        .unify()
}

/// 통합 토큰 추출: 모든 인증 엔드포인트에서 동일한 우선순위로 토큰을 찾는다.
///
/// 우선순위 (보안상 안전한 순서):
///   1. `X-HDMeal-Token` 헤더
///   2. `Authorization: Bearer <token>` 헤더
///   3. `?token=` 쿼리 (proxy 로그에 남으므로 최후 수단)
fn extract_token(
    headers: &warp::http::HeaderMap,
    query: &HashMap<String, String>,
) -> Option<String> {
    if let Some(v) = headers.get("X-HDMeal-Token") {
        if let Ok(s) = v.to_str() {
            let token = s.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }
    if let Some(v) = headers.get("authorization") {
        if let Ok(s) = v.to_str() {
            if let Some(rest) = s.strip_prefix("Bearer ") {
                let token = rest.trim();
                if !token.is_empty() {
                    return Some(token.to_string());
                }
            }
        }
    }
    query
        .get("token")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
}

async fn handle_skill(
    ctx: Arc<AppContext>,
    headers: warp::http::HeaderMap,
    query: HashMap<String, String>,
    rc: RequestContext,
    req: KakaoSkillRequest,
) -> Result<warp::reply::Response, warp::Rejection> {
    let token = extract_token(&headers, &query);
    if !authorize_skill_token(token.as_deref(), &ctx.config.auth_tokens) {
        return Err(HDMealError::unauthorized("Unauthorized").into());
    }
    let messages = ctx.chatbot.dispatch_internal(&req).await?;
    let resp: KakaoSkillResponse = KakaoSkillResponse::from_messages(messages);
    let resp = with_status(json(&resp), StatusCode::OK).into_response();
    Ok(crate::transport::http::finalize_reply(&rc, resp))
}

async fn handle_get_user_settings(
    ctx: Arc<AppContext>,
    headers: warp::http::HeaderMap,
    query: HashMap<String, String>,
    rc: RequestContext,
) -> Result<warp::reply::Response, warp::Rejection> {
    let token = extract_token(&headers, &query)
        .ok_or_else(|| HDMealError::unauthorized("토큰이 없습니다."))?;
    let claims = validate_user_token(ValidateUserTokenInput {
        token: &token,
        secret: &ctx.config.jwt_secret,
        required_scope: scope::GET_USER_INFO,
    })?;
    let (platform, external_id) = split_uid(&claims.uid)?;
    let user = ctx.user_service.ensure_user(platform, external_id).await?;
    let body = UserSettingsResponse {
        grades: (1..=ctx.config.num_of_grades as i32).collect(),
        classes: (1..=ctx.config.num_of_classes as i32).collect(),
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
    Ok(crate::transport::http::finalize_reply(
        &rc,
        with_status(json(&body), StatusCode::OK).into_response(),
    ))
}

async fn handle_patch_user_settings(
    ctx: Arc<AppContext>,
    headers: warp::http::HeaderMap,
    query: HashMap<String, String>,
    rc: RequestContext,
    req: UpdateUserSettingsRequest,
) -> Result<warp::reply::Response, warp::Rejection> {
    let token = extract_token(&headers, &query)
        .ok_or_else(|| HDMealError::unauthorized("토큰이 없습니다."))?;
    let claims = validate_user_token(ValidateUserTokenInput {
        token: &token,
        secret: &ctx.config.jwt_secret,
        required_scope: scope::MANAGE_USER_INFO,
    })?;
    let (platform, external_id) = split_uid(&claims.uid)?;
    let input = crate::application::user_service::UpdateUserInput {
        grade: Some(Some(req.user_grade)),
        class_no: Some(Some(req.user_class)),
        preferences: req.preferences,
    };
    ctx.user_service
        .update_user(
            platform,
            external_id,
            input,
            ctx.config.num_of_grades,
            ctx.config.num_of_classes,
        )
        .await?;
    let body = UserSettingsMessage {
        message: "저장했습니다.".to_string(),
    };
    Ok(crate::transport::http::finalize_reply(
        &rc,
        with_status(json(&body), StatusCode::OK).into_response(),
    ))
}

async fn handle_delete_user_settings(
    ctx: Arc<AppContext>,
    headers: warp::http::HeaderMap,
    query: HashMap<String, String>,
    rc: RequestContext,
) -> Result<warp::reply::Response, warp::Rejection> {
    let token = extract_token(&headers, &query)
        .ok_or_else(|| HDMealError::unauthorized("토큰이 없습니다."))?;
    let claims = validate_user_token(ValidateUserTokenInput {
        token: &token,
        secret: &ctx.config.jwt_secret,
        required_scope: scope::MANAGE_USER_INFO,
    })?;
    let (platform, external_id) = split_uid(&claims.uid)?;
    let ok = ctx.user_service.delete_user(platform, external_id).await?;
    if !ok {
        return Err(HDMealError::not_found("사용자 정보가 없습니다.").into());
    }
    let body = UserSettingsMessage {
        message: "삭제했습니다.".to_string(),
    };
    Ok(crate::transport::http::finalize_reply(
        &rc,
        with_status(json(&body), StatusCode::OK).into_response(),
    ))
}

async fn handle_cache_healthcheck(
    ctx: Arc<AppContext>,
    rc: RequestContext,
) -> Result<warp::reply::Response, warp::Rejection> {
    use crate::application::ingestion_service::is_fresh;

    let now = Utc::now();
    let today = crate::shared::timezone::today_kst_date();
    let timetable = match ctx
        .data
        .get_timetable_by_date(&today.format("%Y-%m-%d").to_string())
        .await
    {
        Ok(Some(t)) if is_fresh(t.created_at, ctx.config.cache_health_timetable_ttl) => {
            CacheHealthcheckStatus::Valid
        }
        Ok(Some(_)) => CacheHealthcheckStatus::Expired,
        Ok(None) => CacheHealthcheckStatus::NotFound,
        Err(_) => CacheHealthcheckStatus::NotFound,
    };

    let weather = match ctx.data.get_latest_weather(now).await {
        Ok(Some(w)) if is_fresh(w.created_at, ctx.config.cache_health_weather_ttl) => {
            CacheHealthcheckStatus::Valid
        }
        Ok(Some(_)) => CacheHealthcheckStatus::Expired,
        Ok(None) => CacheHealthcheckStatus::NotFound,
        Err(_) => CacheHealthcheckStatus::NotFound,
    };
    let water = match ctx.data.get_latest_water_temperature(now).await {
        Ok(Some(w)) if is_fresh(w.created_at, ctx.config.cache_health_water_temp_ttl) => {
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
    Ok(crate::transport::http::finalize_reply(
        &rc,
        with_status(json(&body), StatusCode::OK).into_response(),
    ))
}

use crate::shared::observability::RequestContext;
use crate::transport::http::dto::api::CacheHealthStatus as CacheHealthcheckStatus;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use warp::http::HeaderMap;
    use warp::http::HeaderValue;

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                warp::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
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
