//! `/api/app/*` 핸들러.

use std::sync::Arc;

use chrono::Duration as ChronoDuration;
use warp::http::HeaderValue;
use warp::http::StatusCode;
use warp::reply::{json, with_status, Reply as _};
use warp::Filter;

use crate::application::AppContext;
use crate::error::HDMealError;
use crate::shared::observability::{request_context_filter, RequestContext};
use crate::shared::timezone::today_kst_date;
use crate::transport::http::dto::api::*;

use super::dto::api::{parse_date_param, parse_optional_date_param};

pub fn routes(
    ctx: Arc<AppContext>,
) -> impl Filter<Extract = (warp::reply::Response,), Error = warp::Rejection> + Clone {
    let req_ctx = request_context_filter();
    let ctx_for_days = ctx.clone();
    let ctx_for_day = ctx.clone();
    let ctx_for_meta = ctx.clone();

    let days = warp::path!("api" / "app" / "days")
        .and(warp::get())
        .and(warp::query::<DaysQuery>())
        .and(warp::any().map(move || ctx_for_days.clone()))
        .and(req_ctx)
        .and_then(|q, ctx, rc| async move { handle_days(ctx, rc, q).await });

    let day = warp::path!("api" / "app" / "days" / String)
        .and(warp::get())
        .and(warp::any().map(move || ctx_for_day.clone()))
        .and(request_context_filter())
        .and_then(|d, ctx, rc| async move { handle_day(ctx, rc, d).await });

    let meta = warp::path!("api" / "app" / "meta")
        .and(warp::get())
        .and(warp::any().map(move || ctx_for_meta.clone()))
        .and(request_context_filter())
        .and_then(|ctx, rc| async move { handle_meta(ctx, rc).await });

    days.or(day).unify().or(meta).unify()
}

async fn handle_days(
    ctx: Arc<AppContext>,
    rc: RequestContext,
    q: DaysQuery,
) -> Result<warp::reply::Response, warp::Rejection> {
    let today = today_kst_date();
    let default_start = today - ChronoDuration::days(1);
    let default_end = today + ChronoDuration::days(7);

    let start = parse_optional_date_param(q.from.as_deref())
        .map_err(HDMealError::bad_request)?
        .unwrap_or(default_start);
    let end = parse_optional_date_param(q.to.as_deref())
        .map_err(HDMealError::bad_request)?
        .unwrap_or(default_end);

    if start > end {
        return Err(HDMealError::bad_request("시작일이 종료일보다 늦습니다.").into());
    }
    let max_days = ctx.config.max_days_range as i64;
    if (end - start).num_days() + 1 > max_days {
        return Err(
            HDMealError::bad_request(format!("최대 조회 기간은 {max_days}일입니다.")).into(),
        );
    }

    let start_str = start.format("%Y-%m-%d").to_string();
    let end_str = end.format("%Y-%m-%d").to_string();

    let _ = ctx.ingestion.try_sync_range_short(start, end).await;

    let (meals, schedules, timetables) = tokio::join!(
        ctx.data.get_meals_in_range(&start_str, &end_str),
        ctx.data.get_schedules_in_range(&start_str, &end_str),
        ctx.data.get_timetables_in_range(&start_str, &end_str),
    );

    let meals = meals.map_err(HDMealError::from)?;
    let schedules = schedules.map_err(HDMealError::from)?;
    let timetables = timetables.map_err(HDMealError::from)?;

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
        d = d.succ_opt().unwrap_or(d);
    }

    let body = DaysResponse {
        request_id: rc.request_id.clone(),
        range: DateRange {
            from: start_str.clone(),
            to: end_str.clone(),
        },
        data,
    };
    let mut resp = with_status(json(&body), StatusCode::OK).into_response();
    if let Ok(v) = HeaderValue::from_str(&format!("{start_str}~{end_str}")) {
        resp.headers_mut().insert("X-HDMeal-Range", v);
    }
    Ok(crate::transport::http::finalize_reply(&rc, resp))
}

async fn handle_day(
    ctx: Arc<AppContext>,
    rc: RequestContext,
    day: String,
) -> Result<warp::reply::Response, warp::Rejection> {
    let d = parse_date_param(&day, "day").map_err(HDMealError::bad_request)?;
    let d_str = d.format("%Y-%m-%d").to_string();
    let _ = ctx.ingestion.try_sync_range_short(d, d).await;
    let (meal, schedule, timetable) = tokio::join!(
        ctx.data.get_meal_by_date(&d_str),
        ctx.data.get_schedule_by_date(&d_str),
        ctx.data.get_timetable_by_date(&d_str),
    );
    let meal = meal.map_err(HDMealError::from)?;
    let schedule = schedule.map_err(HDMealError::from)?;
    let timetable = timetable.map_err(HDMealError::from)?;

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
    let mut resp = with_status(json(&body), StatusCode::OK).into_response();
    if let Ok(v) = HeaderValue::from_str(&format!("{d_str}~{d_str}")) {
        resp.headers_mut().insert("X-HDMeal-Range", v);
    }
    Ok(crate::transport::http::finalize_reply(&rc, resp))
}

async fn handle_meta(
    ctx: Arc<AppContext>,
    rc: RequestContext,
) -> Result<warp::reply::Response, warp::Rejection> {
    let body = MetaResponse {
        request_id: rc.request_id.clone(),
        data: MetaData {
            version: ctx.config.app_version.clone(),
            build: ctx.config.app_build,
            debug: ctx.config.debug,
        },
    };
    let resp = with_status(json(&body), StatusCode::OK).into_response();
    Ok(crate::transport::http::finalize_reply(&rc, resp))
}
