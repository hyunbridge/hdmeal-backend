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
    MealDocument, ScheduleDocument, ScheduleEntry, TimetableDocument, UserDocument,
    UserPreferences, WaterTemperatureDocument, WeatherDocument,
};
use crate::error::HDMealResult;

use super::{get_collections, Collections};

/// 인덱스 생성은 한 번만 수행하도록 보장.
static INDEXES_READY: Mutex<bool> = Mutex::new(false);

/// 모든 컬렉션에 대한 CRUD + 인덱스 관리.
#[derive(Clone)]
pub struct DataService {
    pub coll: Collections,
    empty_timetable: Arc<BTreeMap<String, BTreeMap<String, Vec<String>>>>,
}

impl DataService {
    pub async fn new(config: &AppConfig) -> HDMealResult<Self> {
        let coll = get_collections(config).await?;
        let empty_timetable = build_empty_timetable(config.num_of_grades, config.num_of_classes);
        let svc = Self {
            coll,
            empty_timetable: Arc::new(empty_timetable),
        };
        svc.ensure_indexes().await?;
        Ok(svc)
    }

    /// 모든 컬렉션에 unique 인덱스 생성. 멱등.
    pub async fn ensure_indexes(&self) -> HDMealResult<()> {
        {
            let ready = INDEXES_READY.lock();
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

        let mut ready = INDEXES_READY.lock();
        *ready = true;
        Ok(())
    }

    // ---------- Meals ----------

    pub async fn get_meal_by_date(&self, date: &str) -> HDMealResult<Option<MealDocument>> {
        let res = self.coll.meals.find_one(doc! {"date": date}).await?;
        Ok(res)
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

    pub async fn upsert_meal(&self, doc: &MealDocument) -> HDMealResult<()> {
        let filter = doc! {"date": &doc.date};
        let replacement_doc = bson::to_document(doc)?;
        self.coll
            .meals
            .find_one_and_replace(filter, doc)
            .upsert(true)
            .await?;
        let _ = replacement_doc;
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

    /// 비어있는 시간표 스켈레톤. 매번 deep copy 한 인스턴스를 반환.
    pub fn empty_timetable(&self) -> BTreeMap<String, BTreeMap<String, Vec<String>>> {
        let mut out = BTreeMap::new();
        for (g, inner) in self.empty_timetable.iter() {
            let mut new_inner = BTreeMap::new();
            for (c, _) in inner.iter() {
                new_inner.insert(c.clone(), Vec::new());
            }
            out.insert(g.clone(), new_inner);
        }
        out
    }

    // ---------- Weather ----------

    pub async fn get_latest_weather(
        &self,
        _before: DateTime<Utc>,
    ) -> HDMealResult<Option<WeatherDocument>> {
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
        _before: DateTime<Utc>,
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
        // 먼저 EnsureUser 로 존재 보장.
        let _ = self.ensure_user(platform, external_id).await?;

        let now = Utc::now();
        let filter = doc! {"platform": platform, "external_id": external_id};
        let mut set = doc! {"updated_at": bson::DateTime::from_chrono(now)};
        if let Some(g) = grade {
            set.insert(
                "grade",
                g.map(|v| bson::Bson::Int32(v)).unwrap_or(bson::Bson::Null),
            );
        }
        if let Some(c) = class_no {
            set.insert(
                "class_no",
                c.map(|v| bson::Bson::Int32(v)).unwrap_or(bson::Bson::Null),
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

    // `MealUpsert` / `ScheduleUpsert` / `TimetableUpsert` / `WeatherUpsert` 같은
    // 입력 helper 가 아래에 정의되어 있습니다.
}

fn build_empty_timetable(
    num_grades: u32,
    num_classes: u32,
) -> BTreeMap<String, BTreeMap<String, Vec<String>>> {
    let mut out = BTreeMap::new();
    for g in 1..=num_grades {
        let mut inner = BTreeMap::new();
        for c in 1..=num_classes {
            inner.insert(c.to_string(), Vec::new());
        }
        out.insert(g.to_string(), inner);
    }
    out
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

#[allow(dead_code)]
fn _unused() {
    let _ = ScheduleDocument {
        id: String::new(),
        date: String::new(),
        entries: vec![ScheduleEntry {
            name: String::new(),
            grades: vec![],
        }],
        summary: None,
        created_at: Utc::now(),
    };
    let _ = UserPreferences::default();
}
