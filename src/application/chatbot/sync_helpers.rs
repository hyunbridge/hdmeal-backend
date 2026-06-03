//! 챗봇의 "ensure" 패턴: freshness 확인 → 짧은 timeout sync → background spawn.
//!
//! - [`ensure_weather`], [`ensure_water_temperature`]: TTL 안에 있으면 그대로,
//!   아니면 짧은 timeout 으로 fetch 시도 후 background 로 fallback.
//! - [`preload_day_bundle`]: 특정 날짜의 (meal, schedule, timetable) 을 가져옴.

use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};

use crate::application::chatbot::Service;
use crate::domain::{
    MealDocument, ScheduleDocument, TimetableDocument, WaterTemperatureDocument, WeatherDocument,
};

use super::super::ingestion_service::is_fresh;

pub async fn ensure_weather(svc: &Service, ttl: Duration) -> Option<WeatherDocument> {
    let before = Utc::now();
    if let Ok(Some(w)) = svc.data.get_latest_weather(before).await {
        if is_fresh(w.created_at, ttl) {
            return Some(w);
        }
    }

    // 짧은 timeout 으로 fetch 시도
    let kma = svc.kma.clone();
    let data = svc.data.clone();
    let res = tokio::time::timeout(Duration::from_secs(2), async move {
        match kma.fetch_weather().await {
            Ok(view) => {
                let payload = crate::repository::WeatherUpsert {
                    temp: view.temp.clone(),
                    temp_min: view.temp_min.clone(),
                    temp_max: view.temp_max.clone(),
                    sky: view.sky.clone(),
                    pty: view.pty.clone(),
                    precip_probability: view.precip_probability.clone(),
                    humidity: view.humidity.clone(),
                    first_hour: view.first_hour.clone(),
                };
                let ts = view.timestamp;
                data.upsert_weather_at(ts, payload).await.ok()
            }
            Err(e) => {
                tracing::warn!(error = %e, "kma fetch_weather failed");
                None
            }
        }
    })
    .await
    .ok()
    .flatten();

    if res.is_some() {
        return res;
    }

    // background spawn
    let svc_clone = svc.clone();
    tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(5 * 60), async move {
            let view = svc_clone.kma.fetch_weather().await.ok()?;
            let payload = crate::repository::WeatherUpsert {
                temp: view.temp.clone(),
                temp_min: view.temp_min.clone(),
                temp_max: view.temp_max.clone(),
                sky: view.sky.clone(),
                pty: view.pty.clone(),
                precip_probability: view.precip_probability.clone(),
                humidity: view.humidity.clone(),
                first_hour: view.first_hour.clone(),
            };
            let ts = view.timestamp;
            let _ = svc_clone.data.upsert_weather_at(ts, payload).await;
            Some(())
        })
        .await;
    });

    svc.data.get_latest_weather(Utc::now()).await.ok().flatten()
}

pub async fn ensure_water_temperature(
    svc: &Service,
    ttl: Duration,
) -> Option<WaterTemperatureDocument> {
    let before = Utc::now();
    if let Ok(Some(w)) = svc.data.get_latest_water_temperature(before).await {
        if is_fresh(w.created_at, ttl) {
            return Some(w);
        }
    }

    let sw = svc.seoul_water.clone();
    let data = svc.data.clone();
    let res = tokio::time::timeout(Duration::from_secs(2), async move {
        match sw.fetch().await {
            Ok(reading) => data
                .upsert_water_temperature_at(reading.timestamp, reading.temperature_c)
                .await
                .ok(),
            Err(e) => {
                tracing::warn!(error = %e, "seoul water fetch failed");
                None
            }
        }
    })
    .await
    .ok()
    .flatten();

    if res.is_some() {
        return res;
    }

    let svc_clone = svc.clone();
    tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(5 * 60), async move {
            let reading = svc_clone.seoul_water.fetch().await.ok()?;
            let _ = svc_clone
                .data
                .upsert_water_temperature_at(reading.timestamp, reading.temperature_c)
                .await;
            Some(())
        })
        .await;
    });

    svc.data
        .get_latest_water_temperature(Utc::now())
        .await
        .ok()
        .flatten()
}

/// (meal, schedule, timetable) 묶음을 가져옵니다.
/// 모두 있으면 그대로, 없으면 3 초 timeout 으로 range sync 시도, 그래도 없으면
/// background sync spawn.
pub async fn preload_day_bundle(
    svc: &Service,
    date: NaiveDate,
) -> (
    Option<MealDocument>,
    Option<ScheduleDocument>,
    Option<TimetableDocument>,
) {
    let date_str = date.format("%Y-%m-%d").to_string();
    let (m, s, t) = tokio::join!(
        svc.data.get_meal_by_date(&date_str),
        svc.data.get_schedule_by_date(&date_str),
        svc.data.get_timetable_by_date(&date_str),
    );
    let mut meal = m.ok().flatten();
    let mut schedule = s.ok().flatten();
    let mut timetable = t.ok().flatten();

    if meal.is_some() && schedule.is_some() && timetable.is_some() {
        return (meal, schedule, timetable);
    }

    // 짧은 timeout 으로 sync 시도
    let _ = svc.ingestion.try_sync_range_short(date, date).await;

    let (m, s, t) = tokio::join!(
        svc.data.get_meal_by_date(&date_str),
        svc.data.get_schedule_by_date(&date_str),
        svc.data.get_timetable_by_date(&date_str),
    );
    meal = m.ok().flatten();
    schedule = s.ok().flatten();
    timetable = t.ok().flatten();

    if meal.is_none() || schedule.is_none() || timetable.is_none() {
        // background spawn (chatbot sync helper 와 동일)
        let _ = svc.ingestion.spawn_background_range_sync(date, date);
    }
    (meal, schedule, timetable)
}

/// UTC timestamp → "오전 12시" / "오후 3시" 등으로 변환 (KST 기준).
pub fn hour_label_from_ts(ts: DateTime<Utc>) -> String {
    use chrono::Timelike;
    let kst = ts.with_timezone(&crate::shared::timezone::KST);
    crate::shared::timezone::format_hour(kst.hour())
}
