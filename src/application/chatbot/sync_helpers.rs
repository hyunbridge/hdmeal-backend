//! 챗봇의 "ensure" 패턴: freshness 확인 → 짧은 timeout sync → background spawn.
//!
//! - [`ensure_weather`], [`ensure_water_temperature`]: TTL 안에 있으면 그대로,
//!   아니면 짧은 timeout 으로 fetch 시도 후 background 로 fallback.
//! - [`preload_day_bundle`]: 특정 날짜의 (meal, schedule, timetable) 을 가져옴.

use std::borrow::Cow;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};

use crate::application::chatbot::Service;
use crate::domain::{
    MealDocument, ScheduleDocument, TimetableDocument, WaterTemperatureDocument, WeatherDocument,
};
use crate::infrastructure::neis::auxiliary::WeatherView;
use crate::repository::WeatherUpsert;

use crate::shared::freshness::is_fresh;

pub async fn ensure_weather(svc: &Service, ttl: Duration) -> Option<WeatherDocument> {
    if let Ok(Some(w)) = svc.data.get_latest_weather().await {
        if is_fresh(w.created_at, ttl) {
            return Some(w);
        }
    }

    let kma = svc.kma.clone();
    let data = svc.data.clone();
    let res = tokio::time::timeout(Duration::from_secs(2), async move {
        match kma.fetch_weather().await {
            Ok(view) => {
                let (timestamp, payload) = weather_upsert_from_view(view);
                data.upsert_weather_at(timestamp, payload).await.ok()
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

    let svc_clone = svc.clone();
    tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(5 * 60), async move {
            let view = svc_clone.kma.fetch_weather().await.ok()?;
            let (timestamp, payload) = weather_upsert_from_view(view);
            let _ = svc_clone.data.upsert_weather_at(timestamp, payload).await;
            Some(())
        })
        .await;
    });

    svc.data.get_latest_weather().await.ok().flatten()
}

pub async fn ensure_water_temperature(
    svc: &Service,
    ttl: Duration,
) -> Option<WaterTemperatureDocument> {
    if let Ok(Some(w)) = svc.data.get_latest_water_temperature().await {
        if is_fresh(w.created_at, ttl) {
            return Some(w);
        }
    }

    let sw = svc.seoul_water.clone();
    let data = svc.data.clone();
    let res = tokio::time::timeout(Duration::from_secs(2), async move {
        match sw.fetch().await {
            Ok(reading) => match data
                .upsert_water_temperature_at(reading.timestamp, reading.temperature_c)
                .await
            {
                Ok(doc) => Some(doc),
                Err(e) => {
                    tracing::warn!(error = %e, "seoul water cache upsert failed");
                    Some(water_temperature_document_from_reading(&reading))
                }
            },
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
            if let Err(e) = svc_clone
                .data
                .upsert_water_temperature_at(reading.timestamp, reading.temperature_c)
                .await
            {
                tracing::warn!(error = %e, "seoul water cache upsert failed");
            }
            Some(())
        })
        .await;
    });

    svc.data.get_latest_water_temperature().await.ok().flatten()
}

/// (meal, schedule, timetable) 묶음을 가져옵니다.
/// 이미 DB 에 있으면 바로 반환, 없으면 3초 timeout sync 후 한 번 더 DB 조회.
/// 그래도 없으면 background sync spawn 후 현재 있는 것만 반환.
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
    let meal = m.ok().flatten();
    let schedule = s.ok().flatten();
    let timetable = t.ok().flatten();

    if meal.is_some() && schedule.is_some() && timetable.is_some() {
        return (meal, schedule, timetable);
    }

    if svc.ingestion.try_sync_range_short(date, date).await {
        let (m, s, t) = tokio::join!(
            svc.data.get_meal_by_date(&date_str),
            svc.data.get_schedule_by_date(&date_str),
            svc.data.get_timetable_by_date(&date_str),
        );
        let meal = m.ok().flatten();
        let schedule = s.ok().flatten();
        let timetable = t.ok().flatten();

        if meal.is_some() && schedule.is_some() && timetable.is_some() {
            return (meal, schedule, timetable);
        }

        if meal.is_none() || schedule.is_none() || timetable.is_none() {
            drop(svc.ingestion.spawn_background_range_sync(date, date));
        }
        return (meal, schedule, timetable);
    }

    // timeout 시 background spawn 후 기존 결과 반환 (재조회 생략)
    if meal.is_none() || schedule.is_none() || timetable.is_none() {
        drop(svc.ingestion.spawn_background_range_sync(date, date));
    }
    (meal, schedule, timetable)
}

/// UTC timestamp → "오전 12시" / "오후 3시" 등으로 변환 (KST 기준).
pub fn hour_label_from_ts(ts: DateTime<Utc>) -> Cow<'static, str> {
    use chrono::Timelike;
    let kst = ts.with_timezone(&crate::shared::timezone::KST);
    crate::shared::timezone::format_hour(kst.hour())
}

fn water_temperature_document_from_reading(
    reading: &crate::infrastructure::neis::auxiliary::SeoulWaterReading,
) -> WaterTemperatureDocument {
    WaterTemperatureDocument {
        id: format!("water-{}", reading.timestamp.timestamp()),
        timestamp: reading.timestamp,
        temperature_c: reading.temperature_c,
        created_at: Utc::now(),
    }
}

fn weather_upsert_from_view(view: WeatherView) -> (DateTime<Utc>, WeatherUpsert) {
    let WeatherView {
        timestamp,
        temp,
        temp_min,
        temp_max,
        sky,
        pty,
        precip_probability,
        humidity,
        first_hour,
    } = view;
    (
        timestamp,
        WeatherUpsert {
            temp,
            temp_min,
            temp_max,
            sky,
            pty,
            precip_probability,
            humidity,
            first_hour,
        },
    )
}
