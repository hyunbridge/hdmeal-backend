//! DataService: 모든 MongoDB CRUD 와 인덱스 관리.
//!
//! - 날짜(`date`) 필드는 문자열 (`YYYY-MM-DD`) 로 저장.
//! - `lessons` / `entries` 의 `HashMap` 은 `BTreeMap` 으로 직렬화해
//!   결정적 JSON 출력을 보장.

use std::collections::BTreeMap;
use std::sync::Arc;

use bson::doc;
use chrono::{DateTime, Utc};
use mongodb::options::{IndexOptions, ReturnDocument};
use mongodb::IndexModel;
use parking_lot::Mutex;

use crate::config::AppConfig;
use crate::domain::{
    MealDocument, ScheduleDocument, TimetableDocument, UserDocument, WaterTemperatureDocument,
    WeatherDocument,
};
use crate::error::HDMealResult;

use super::{get_collections, Client, Collections};

/// 모든 컬렉션에 대한 CRUD + 인덱스 관리.
#[derive(Clone)]
pub struct DataService {
    pub coll: Collections,
    empty_timetable: Arc<BTreeMap<String, BTreeMap<String, Vec<String>>>>,
    indexes_ready: Arc<Mutex<bool>>,
}

impl DataService {
    pub async fn new(client: Arc<Client>, config: &AppConfig) -> HDMealResult<Self> {
        let coll = get_collections(&client, &config.mongodb_database);
        let empty_timetable = build_empty_timetable(config.num_of_grades, config.num_of_classes);
        let svc = Self {
            coll,
            empty_timetable: Arc::new(empty_timetable),
            indexes_ready: Arc::new(Mutex::new(false)),
        };
        svc.ensure_indexes().await?;
        Ok(svc)
    }

    /// 모든 컬렉션에 unique 인덱스 생성. 멱등.
    pub async fn ensure_indexes(&self) -> HDMealResult<()> {
        {
            let ready = self.indexes_ready.lock();
            if *ready {
                return Ok(());
            }
        }
        let opts = IndexOptions::builder().unique(true).build();
        let idx_date = IndexModel::builder()
            .keys(doc! {"date": 1})
            .options(opts.clone())
            .build();
        let idx_timestamp = IndexModel::builder()
            .keys(doc! {"timestamp": -1})
            .options(opts.clone())
            .build();
        let idx_user = IndexModel::builder()
            .keys(doc! {"platform": 1, "external_id": 1})
            .options(opts)
            .build();

        self.coll.meals.create_index(idx_date.clone()).await?;
        self.coll.schedules.create_index(idx_date.clone()).await?;
        self.coll.timetables.create_index(idx_date.clone()).await?;
        self.coll
            .weather
            .create_index(idx_timestamp.clone())
            .await?;
        self.coll
            .water_temperatures
            .create_index(idx_timestamp.clone())
            .await?;
        self.coll.users.create_index(idx_user).await?;

        let mut ready = self.indexes_ready.lock();
        *ready = true;
        Ok(())
    }

    // ---------- Meals ----------

    pub async fn get_meal_by_date(&self, date: &str) -> HDMealResult<Option<MealDocument>> {
        Ok(self.coll.meals.find_one(doc! {"date": date}).await?)
    }

    pub async fn get_meals_in_range(
        &self,
        start: &str,
        end: &str,
    ) -> HDMealResult<Vec<MealDocument>> {
        let filter = doc! {"date": {"$gte": start, "$lte": end}};
        let mut cur = self.coll.meals.find(filter).await?;
        let mut out = Vec::new();
        use futures::TryStreamExt;
        while let Some(m) = cur.try_next().await? {
            out.push(m);
        }
        Ok(out)
    }

    pub async fn upsert_meal(&self, meal: &MealDocument) -> HDMealResult<()> {
        let filter = doc! {"date": &meal.date};
        self.coll
            .meals
            .find_one_and_replace(filter, meal)
            .upsert(true)
            .await?;
        Ok(())
    }

    /// 여러 [`MealDocument`] 를 배치 upsert — `delete_many(date range)` + `insert_many`.
    ///
    /// N×1 round-trip → 2 round-trip. warmup 과 sync_window 의 주된 성능
    /// 병목 (NEIS 12일치 × 3 컬렉션 = 36 round-trip) 을 ~6 round-trip 으로 축소.
    /// mongodb 8.0+ 의 `Client::bulk_write` 는 도입 시 한 단계 더 줄일 여지가 있다.
    ///
    /// 트레이드오프:
    /// - 장점: MongoDB 6.x+ 모두 호환 (8.0 종속 없음).
    /// - 단점: `date` 가 unique 인덱스이므로 같은 `_id` 를 가진 다른 collection 의
    ///   `users` 와는 충돌 없음. 단 `date` range 가 정확히 일치해야 함 (sync_window
    ///   가 항상 `start..=end` 전체를 다시 쓰는 warmup semantics 와 일치).
    pub async fn upsert_meals_batch(&self, meals: &[MealDocument]) -> HDMealResult<()> {
        if meals.is_empty() {
            return Ok(());
        }
        let dates: Vec<&str> = meals.iter().map(|m| m.date.as_str()).collect();
        let filter = doc! {"date": {"$in": &dates}};
        self.coll.meals.delete_many(filter).await?;
        self.coll.meals.insert_many(meals).ordered(false).await?;
        Ok(())
    }

    // ---------- Schedules ----------

    pub async fn get_schedule_by_date(&self, date: &str) -> HDMealResult<Option<ScheduleDocument>> {
        Ok(self.coll.schedules.find_one(doc! {"date": date}).await?)
    }

    pub async fn get_schedules_in_range(
        &self,
        start: &str,
        end: &str,
    ) -> HDMealResult<Vec<ScheduleDocument>> {
        let filter = doc! {"date": {"$gte": start, "$lte": end}};
        let mut cur = self.coll.schedules.find(filter).await?;
        let mut out = Vec::new();
        use futures::TryStreamExt;
        while let Some(m) = cur.try_next().await? {
            out.push(m);
        }
        Ok(out)
    }

    pub async fn upsert_schedule(&self, schedule: &ScheduleDocument) -> HDMealResult<()> {
        let filter = doc! {"date": &schedule.date};
        self.coll
            .schedules
            .find_one_and_replace(filter, schedule)
            .upsert(true)
            .await?;
        Ok(())
    }

    /// [`upsert_meals_batch`] 와 동일 패턴. 12일치 일정.
    pub async fn upsert_schedules_batch(&self, schedules: &[ScheduleDocument]) -> HDMealResult<()> {
        if schedules.is_empty() {
            return Ok(());
        }
        let dates: Vec<&str> = schedules.iter().map(|s| s.date.as_str()).collect();
        let filter = doc! {"date": {"$in": &dates}};
        self.coll.schedules.delete_many(filter).await?;
        self.coll
            .schedules
            .insert_many(schedules)
            .ordered(false)
            .await?;
        Ok(())
    }

    // ---------- Timetables ----------

    pub async fn get_timetable_by_date(
        &self,
        date: &str,
    ) -> HDMealResult<Option<TimetableDocument>> {
        Ok(self.coll.timetables.find_one(doc! {"date": date}).await?)
    }

    pub async fn get_timetables_in_range(
        &self,
        start: &str,
        end: &str,
    ) -> HDMealResult<Vec<TimetableDocument>> {
        let filter = doc! {"date": {"$gte": start, "$lte": end}};
        let mut cur = self.coll.timetables.find(filter).await?;
        let mut out = Vec::new();
        use futures::TryStreamExt;
        while let Some(m) = cur.try_next().await? {
            out.push(m);
        }
        Ok(out)
    }

    pub async fn upsert_timetable(&self, timetable: &TimetableDocument) -> HDMealResult<()> {
        let filter = doc! {"date": &timetable.date};
        self.coll
            .timetables
            .find_one_and_replace(filter, timetable)
            .upsert(true)
            .await?;
        Ok(())
    }

    /// [`upsert_meals_batch`] 와 동일 패턴. 12일치 시간표.
    pub async fn upsert_timetables_batch(
        &self,
        timetables: &[TimetableDocument],
    ) -> HDMealResult<()> {
        if timetables.is_empty() {
            return Ok(());
        }
        let dates: Vec<&str> = timetables.iter().map(|t| t.date.as_str()).collect();
        let filter = doc! {"date": {"$in": &dates}};
        self.coll.timetables.delete_many(filter).await?;
        self.coll
            .timetables
            .insert_many(timetables)
            .ordered(false)
            .await?;
        Ok(())
    }

    /// 비어있는 시간표 스켈레톤. 매번 deep copy 한 인스턴스를 반환.
    pub fn empty_timetable(&self) -> BTreeMap<String, BTreeMap<String, Vec<String>>> {
        self.empty_timetable
            .iter()
            .map(|(g, inner)| {
                (
                    g.clone(),
                    inner.keys().map(|c| (c.clone(), Vec::new())).collect(),
                )
            })
            .collect()
    }

    // ---------- Weather ----------

    pub async fn get_latest_weather(&self) -> HDMealResult<Option<WeatherDocument>> {
        let opts = mongodb::options::FindOneOptions::builder()
            .sort(doc! {"timestamp": -1})
            .build();
        Ok(self
            .coll
            .weather
            .find_one(doc! {})
            .with_options(opts)
            .await?)
    }

    pub async fn upsert_weather_at(
        &self,
        ts: DateTime<Utc>,
        payload: WeatherUpsert,
    ) -> HDMealResult<WeatherDocument> {
        let now = Utc::now();
        let mut update_doc = doc! {
            "$set": {
                "temp": &payload.temp,
                "temp_min": &payload.temp_min,
                "temp_max": &payload.temp_max,
                "sky": &payload.sky,
                "pty": &payload.pty,
                "precip_probability": &payload.precip_probability,
                "humidity": &payload.humidity,
                "first_hour": &payload.first_hour,
                "created_at": bson::DateTime::from_chrono(now),
            }
        };
        update_doc.insert(
            "$setOnInsert",
            doc! {"timestamp": bson::DateTime::from_chrono(ts)},
        );

        let opts = mongodb::options::FindOneAndUpdateOptions::builder()
            .upsert(true)
            .return_document(ReturnDocument::After)
            .build();
        let updated = self
            .coll
            .weather
            .find_one_and_update(
                doc! {"timestamp": bson::DateTime::from_chrono(ts)},
                update_doc,
            )
            .with_options(opts)
            .await?
            .ok_or_else(|| crate::error::HDMealError::internal("weather upsert failed"))?;
        Ok(updated)
    }

    // ---------- Water Temperature ----------

    pub async fn get_latest_water_temperature(
        &self,
    ) -> HDMealResult<Option<WaterTemperatureDocument>> {
        let opts = mongodb::options::FindOneOptions::builder()
            .sort(doc! {"timestamp": -1})
            .build();
        Ok(self
            .coll
            .water_temperatures
            .find_one(doc! {})
            .with_options(opts)
            .await?)
    }

    pub async fn upsert_water_temperature_at(
        &self,
        ts: DateTime<Utc>,
        temperature_c: f64,
    ) -> HDMealResult<WaterTemperatureDocument> {
        let now = Utc::now();
        let filter = doc! {"timestamp": bson::DateTime::from_chrono(ts)};
        let update = doc! {
            "$set": {
                "temperature_c": temperature_c,
                "created_at": bson::DateTime::from_chrono(now),
            },
            "$setOnInsert": {"timestamp": bson::DateTime::from_chrono(ts)},
        };
        let opts = mongodb::options::FindOneAndUpdateOptions::builder()
            .upsert(true)
            .return_document(ReturnDocument::After)
            .build();
        let updated = self
            .coll
            .water_temperatures
            .find_one_and_update(filter, update)
            .with_options(opts)
            .await?
            .ok_or_else(|| crate::error::HDMealError::internal("water upsert failed"))?;
        Ok(updated)
    }

    // ---------- Users ----------

    pub async fn ensure_user(
        &self,
        platform: &str,
        external_id: &str,
    ) -> HDMealResult<UserDocument> {
        let now = Utc::now();
        let filter = doc! {"platform": platform, "external_id": external_id};
        let update = doc! {
            "$setOnInsert": {
                "platform": platform,
                "external_id": external_id,
                "grade": bson::Bson::Null,
                "class_no": bson::Bson::Null,
                "preferences": {"AllergyInfo": "Number"},
                "created_at": bson::DateTime::from_chrono(now),
                "updated_at": bson::DateTime::from_chrono(now),
            }
        };
        let opts = mongodb::options::FindOneAndUpdateOptions::builder()
            .upsert(true)
            .return_document(ReturnDocument::After)
            .build();
        let user = self
            .coll
            .users
            .find_one_and_update(filter, update)
            .with_options(opts)
            .await?
            .ok_or_else(|| crate::error::HDMealError::internal("ensure_user failed"))?;
        Ok(user)
    }

    pub async fn get_user(
        &self,
        platform: &str,
        external_id: &str,
    ) -> HDMealResult<Option<UserDocument>> {
        Ok(self
            .coll
            .users
            .find_one(doc! {"platform": platform, "external_id": external_id})
            .await?)
    }

    /// grade / class / preferences 업데이트. None 으로 두면 변경하지 않습니다.
    /// preferences.AllergyInfo 가 `None` 이면 기본값 `"Number"` 가 적용됩니다.
    pub async fn update_user(
        &self,
        platform: &str,
        external_id: &str,
        grade: Option<Option<i32>>,
        class_no: Option<Option<i32>>,
        allergy_info: Option<String>,
    ) -> HDMealResult<UserDocument> {
        let _ = self.ensure_user(platform, external_id).await?;

        let now = Utc::now();
        let filter = doc! {"platform": platform, "external_id": external_id};
        let mut set = doc! {"updated_at": bson::DateTime::from_chrono(now)};
        if let Some(g) = grade {
            set.insert(
                "grade",
                g.map(bson::Bson::Int32).unwrap_or(bson::Bson::Null),
            );
        }
        if let Some(c) = class_no {
            set.insert(
                "class_no",
                c.map(bson::Bson::Int32).unwrap_or(bson::Bson::Null),
            );
        }
        if let Some(a) = allergy_info {
            set.insert("preferences", doc! {"AllergyInfo": a});
        }

        let opts = mongodb::options::FindOneAndUpdateOptions::builder()
            .return_document(ReturnDocument::After)
            .build();
        let updated = self
            .coll
            .users
            .find_one_and_update(filter, doc! {"$set": set})
            .with_options(opts)
            .await?
            .ok_or_else(|| crate::error::HDMealError::not_found("사용자 정보가 없습니다."))?;
        Ok(updated)
    }

    pub async fn delete_user(&self, platform: &str, external_id: &str) -> HDMealResult<bool> {
        let res = self
            .coll
            .users
            .delete_one(doc! {"platform": platform, "external_id": external_id})
            .await?;
        Ok(res.deleted_count > 0)
    }
}

fn build_empty_timetable(
    num_grades: u32,
    num_classes: u32,
) -> BTreeMap<String, BTreeMap<String, Vec<String>>> {
    (1..=num_grades)
        .map(|g| {
            (
                g.to_string(),
                (1..=num_classes)
                    .map(|c| (c.to_string(), Vec::new()))
                    .collect(),
            )
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct WeatherUpsert {
    pub temp: String,
    pub temp_min: String,
    pub temp_max: String,
    pub sky: String,
    pub pty: String,
    pub precip_probability: String,
    pub humidity: String,
    pub first_hour: String,
}
