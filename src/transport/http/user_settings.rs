//! `/user/settings` 핸들러.

use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::handler::Handler;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use tower_http::limit::RequestBodyLimitLayer;

use crate::error::HDMealError;
use crate::shared::observability::RequestContext;
use crate::shared::security::{scope, split_uid, validate_user_token, ValidateUserTokenInput};
use crate::transport::http::auth::extract_token;
use crate::transport::http::dto::user_settings::{
    UpdateUserSettingsRequest, UserSettingsMessage, UserSettingsPreferences, UserSettingsResponse,
};
use crate::transport::http::RouterState;

const USER_SETTINGS_BODY_LIMIT_BYTES: usize = 8 * 1024;

pub fn router() -> Router<RouterState> {
    let route = get(get_user_settings)
        .patch(
            patch_user_settings.layer(RequestBodyLimitLayer::new(USER_SETTINGS_BODY_LIMIT_BYTES)),
        )
        .delete(delete_user_settings);

    Router::new()
        .route("/user/settings", route.clone())
        .route("/user/settings/", route)
}

async fn get_user_settings(
    State(state): State<RouterState>,
    _rc: RequestContext,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, HDMealError> {
    let token = extract_token(&headers, &query, state.ctx.config.debug)
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
    let token = extract_token(&headers, &query, state.ctx.config.debug)
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
    let token = extract_token(&headers, &query, state.ctx.config.debug)
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
