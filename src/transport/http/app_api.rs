//! `/api/app/*` 핸들러.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use crate::error::HDMealError;
use crate::shared::observability::RequestContext;
use crate::shared::timezone::today_kst_date;
use crate::transport::http::dto::api::*;
use crate::transport::http::RouterState;

use super::dto::api::{parse_date_param, parse_optional_date_param};

pub fn router() -> Router<RouterState> {
    Router::new()
        .route("/api/app/days", get(days))
        .route("/api/app/days/:day", get(day))
        .route("/api/app/meta", get(meta))
}

async fn days(
    State(state): State<RouterState>,
    rc: RequestContext,
    Query(q): Query<DaysQuery>,
) -> Result<Response, HDMealError> {
    let ctx = state.ctx;
    let today = today_kst_date();
    let default_start = today - chrono::Duration::days(1);
    let default_end = today + chrono::Duration::days(7);

    let start = parse_optional_date_param(q.from.as_deref())
        .map_err(HDMealError::bad_request)?
        .unwrap_or(default_start);
    let end = parse_optional_date_param(q.to.as_deref())
        .map_err(HDMealError::bad_request)?
        .unwrap_or(default_end);

    if start > end {
        return Err(HDMealError::bad_request("시작일이 종료일보다 늦습니다."));
    }
    let max_days = ctx.config.max_days_range as i64;
    if (end - start).num_days() + 1 > max_days {
        return Err(HDMealError::bad_request(format!(
            "최대 조회 기간은 {max_days}일입니다."
        )));
    }

    let start_str = start.format("%Y-%m-%d").to_string();
    let end_str = end.format("%Y-%m-%d").to_string();

    let _ = ctx.ingestion.try_sync_range_short(start, end).await;

    let (meals, schedules, timetables) = tokio::join!(
        ctx.data.get_meals_in_range(&start_str, &end_str),
        ctx.data.get_schedules_in_range(&start_str, &end_str),
        ctx.data.get_timetables_in_range(&start_str, &end_str),
    );

    let meals = meals?;
    let schedules = schedules?;
    let timetables = timetables?;

    let meals_by: std::collections::HashMap<_, _> =
        meals.into_iter().map(|m| (m.date.clone(), m)).collect();
    let schedules_by: std::collections::HashMap<_, _> =
        schedules.into_iter().map(|s| (s.date.clone(), s)).collect();
    let timetables_by: std::collections::HashMap<_, _> = timetables
        .into_iter()
        .map(|t| (t.date.clone(), t))
        .collect();

    let mut data = Vec::new();
    let mut d = start;
    while d <= end {
        let key = d.format("%Y-%m-%d").to_string();
        let meal = meals_by.get(&key).cloned().map(|m| m.into_view());
        let schedule = schedules_by
            .get(&key)
            .cloned()
            .map(|s| s.into_view())
            .unwrap_or_default();
        let timetable = match timetables_by.get(&key).cloned() {
            Some(t) => t.into_view(),
            None => TimetableView {
                lessons: ctx.data.empty_timetable(),
                updated_at: None,
            },
        };
        data.push(DayEntry {
            date: key,
            meal,
            schedule,
            timetable,
        });
        let Some(next) = d.succ_opt() else {
            break;
        };
        d = next;
    }

    let body = DaysResponse {
        request_id: rc.request_id.clone(),
        range: DateRange {
            from: start_str.clone(),
            to: end_str.clone(),
        },
        data,
    };
    let mut resp = (StatusCode::OK, Json(body)).into_response();
    if let Ok(v) = HeaderValue::from_str(&format!("{start_str}~{end_str}")) {
        resp.headers_mut().insert("X-HDMeal-Range", v);
    }
    Ok(resp)
}

async fn day(
    State(state): State<RouterState>,
    rc: RequestContext,
    Path(day): Path<String>,
) -> Result<Response, HDMealError> {
    let ctx = state.ctx;
    let d = parse_date_param(&day, "day").map_err(HDMealError::bad_request)?;
    let d_str = d.format("%Y-%m-%d").to_string();
    let _ = ctx.ingestion.try_sync_range_short(d, d).await;
    let (meal, schedule, timetable) = tokio::join!(
        ctx.data.get_meal_by_date(&d_str),
        ctx.data.get_schedule_by_date(&d_str),
        ctx.data.get_timetable_by_date(&d_str),
    );
    let meal = meal?;
    let schedule = schedule?;
    let timetable = timetable?;

    let timetable = match timetable {
        Some(t) => t.into_view(),
        None => TimetableView {
            lessons: ctx.data.empty_timetable(),
            updated_at: None,
        },
    };

    let body = DayResponse {
        request_id: rc.request_id.clone(),
        data: DayEntry {
            date: d_str.clone(),
            meal: meal.map(|m| m.into_view()),
            schedule: schedule.map(|s| s.into_view()).unwrap_or_default(),
            timetable,
        },
    };
    let mut resp = (StatusCode::OK, Json(body)).into_response();
    if let Ok(v) = HeaderValue::from_str(&format!("{d_str}~{d_str}")) {
        resp.headers_mut().insert("X-HDMeal-Range", v);
    }
    Ok(resp)
}

async fn meta(
    State(state): State<RouterState>,
    rc: RequestContext,
) -> Result<Response, HDMealError> {
    let ctx = state.ctx;
    let body = MetaResponse {
        request_id: rc.request_id.clone(),
        data: MetaData {
            version: ctx.config.app_version.clone(),
            build: ctx.config.app_build,
            debug: cfg!(debug_assertions).then_some(ctx.config.debug),
        },
    };
    let resp = (StatusCode::OK, Json(body)).into_response();
    Ok(resp)
}
