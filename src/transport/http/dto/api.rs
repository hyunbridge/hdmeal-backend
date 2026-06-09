//! `/api/app/*` 와 챗봇 응답 DTO.

use std::collections::BTreeMap;

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::domain::{MealDocument, ScheduleDocument, TimetableDocument};

// ---------- /api/app/days ----------

#[derive(Debug, Deserialize)]
pub struct DaysQuery {
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DaysResponse {
    #[serde(rename = "requestId")]
    pub request_id: String,
    pub range: DateRange,
    pub data: Vec<DayEntry>,
}

#[derive(Debug, Serialize)]
pub struct DateRange {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Serialize)]
pub struct DayEntry {
    pub date: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meal: Option<MealView>,
    #[serde(default)]
    pub schedule: Vec<ScheduleEntryView>,
    pub timetable: TimetableView,
}

#[derive(Debug, Serialize)]
pub struct MealView {
    pub items: Vec<MealItemView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kcal: Option<f64>,
    #[serde(rename = "updatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MealItemView {
    pub name: String,
    pub allergies: Vec<i32>,
}

#[derive(Debug, Serialize)]
pub struct ScheduleEntryView {
    pub name: String,
    pub grades: Vec<i32>,
}

#[derive(Debug, Serialize)]
pub struct TimetableView {
    pub lessons: BTreeMap<String, BTreeMap<String, Vec<String>>>,
    #[serde(rename = "updatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

// ---------- /api/app/days/{day} ----------

#[derive(Debug, Serialize)]
pub struct DayResponse {
    #[serde(rename = "requestId")]
    pub request_id: String,
    pub data: DayEntry,
}

// ---------- /api/app/meta ----------

#[derive(Debug, Serialize)]
pub struct MetaResponse {
    #[serde(rename = "requestId")]
    pub request_id: String,
    pub data: MetaData,
}

#[derive(Debug, Serialize)]
pub struct MetaData {
    pub version: String,
    pub build: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub debug: Option<bool>,
}

// ---------- /cache/healthcheck ----------

#[derive(Debug, Serialize)]
pub struct CacheHealthcheckResponse {
    pub timetable: CacheHealthStatus,
    pub weather: CacheHealthStatus,
    pub water_temperature: CacheHealthStatus,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum CacheHealthStatus {
    Valid,
    Expired,
    NotFound,
}

// ---------- Converters ----------

impl MealDocument {
    pub fn into_view(self) -> MealView {
        let updated_at = crate::shared::timezone::to_kst_iso(&self.created_at);
        MealView {
            items: self
                .menus
                .into_iter()
                .map(|m| MealItemView {
                    name: m.name,
                    allergies: m.allergies,
                })
                .collect(),
            kcal: self.calories,
            updated_at: Some(updated_at),
        }
    }
}

impl ScheduleDocument {
    pub fn into_view(self) -> Vec<ScheduleEntryView> {
        self.entries
            .into_iter()
            .map(|e| ScheduleEntryView {
                name: e.name,
                grades: e.grades,
            })
            .collect()
    }
}

impl TimetableDocument {
    pub fn into_view(self) -> TimetableView {
        TimetableView {
            lessons: self.lessons,
            updated_at: Some(crate::shared::timezone::to_kst_iso(&self.created_at)),
        }
    }
}

// ---------- Date parsing ----------

pub fn parse_date_param(s: &str, field: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|_| format!("'{field}' 형식이 올바르지 않습니다. (YYYY-MM-DD)"))
}

pub fn parse_optional_date_param(s: Option<&str>) -> Result<Option<NaiveDate>, String> {
    match s {
        None | Some("") => Ok(None),
        Some(value) => NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(Some)
            .map_err(|_| "잘못된 날짜 형식입니다. (YYYY-MM-DD)".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        MealDocument, MealMenuItem, ScheduleDocument, ScheduleEntry, TimetableDocument,
    };
    use chrono::Utc;
    use std::collections::BTreeMap;

    #[test]
    fn parse_date_param_accepts_iso() {
        let d = parse_date_param("2026-06-04", "from").unwrap();
        assert_eq!(d.to_string(), "2026-06-04");
    }

    #[test]
    fn parse_date_param_rejects_wrong_format() {
        let err = parse_date_param("06-04-2026", "from").unwrap_err();
        assert!(err.contains("from"), "field name leaked: {err}");
        assert!(err.contains("YYYY-MM-DD"), "format hint missing: {err}");
    }

    #[test]
    fn parse_date_param_rejects_garbage() {
        assert!(parse_date_param("not-a-date", "to").is_err());
    }

    #[test]
    fn parse_optional_date_param_none() {
        assert_eq!(parse_optional_date_param(None).unwrap(), None);
    }

    #[test]
    fn parse_optional_date_param_empty_string() {
        assert_eq!(parse_optional_date_param(Some("")).unwrap(), None);
    }

    #[test]
    fn parse_optional_date_param_valid() {
        let d = parse_optional_date_param(Some("2026-01-15"))
            .unwrap()
            .unwrap();
        assert_eq!(d.to_string(), "2026-01-15");
    }

    #[test]
    fn parse_optional_date_param_invalid() {
        assert!(parse_optional_date_param(Some("20260115")).is_err());
    }

    #[test]
    fn meal_document_into_view() {
        let doc = MealDocument {
            id: "m1".to_string(),
            date: "2026-06-09".to_string(),
            menus: vec![MealMenuItem {
                name: "밥".to_string(),
                allergies: vec![1, 2],
            }],
            menus_plain: vec![],
            calories: Some(800.5),
            source_hash: None,
            created_at: Utc::now(),
        };
        let view = doc.into_view();
        assert_eq!(view.items.len(), 1);
        assert_eq!(view.items[0].name, "밥");
        assert_eq!(view.items[0].allergies, vec![1, 2]);
        assert_eq!(view.kcal, Some(800.5));
        assert!(view.updated_at.is_some());
    }

    #[test]
    fn schedule_document_into_view() {
        let doc = ScheduleDocument {
            id: "s1".to_string(),
            date: "2026-06-09".to_string(),
            entries: vec![ScheduleEntry {
                name: "중간고사".to_string(),
                grades: vec![1, 2],
            }],
            summary: Some("중간고사".to_string()),
            created_at: Utc::now(),
        };
        let view = doc.into_view();
        assert_eq!(view.len(), 1);
        assert_eq!(view[0].name, "중간고사");
        assert_eq!(view[0].grades, vec![1, 2]);
    }

    #[test]
    fn schedule_document_into_view_empty_entries() {
        let doc = ScheduleDocument {
            id: "s1".to_string(),
            date: "2026-06-09".to_string(),
            entries: vec![],
            summary: None,
            created_at: Utc::now(),
        };
        let view = doc.into_view();
        assert!(view.is_empty());
    }

    #[test]
    fn timetable_document_into_view() {
        let mut lessons = BTreeMap::new();
        let mut class_map = BTreeMap::new();
        class_map.insert("1".to_string(), vec!["국어".to_string()]);
        lessons.insert("1".to_string(), class_map);
        let doc = TimetableDocument {
            id: "t1".to_string(),
            date: "2026-06-09".to_string(),
            lessons,
            created_at: Utc::now(),
        };
        let view = doc.into_view();
        assert!(view.lessons.contains_key("1"));
        assert!(view.updated_at.is_some());
    }

    #[test]
    fn cache_health_status_serializes_pascal_case() {
        let resp = CacheHealthcheckResponse {
            timetable: CacheHealthStatus::Valid,
            weather: CacheHealthStatus::Expired,
            water_temperature: CacheHealthStatus::NotFound,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["timetable"], "Valid");
        assert_eq!(json["weather"], "Expired");
        assert_eq!(json["water_temperature"], "NotFound");
    }

    #[test]
    fn day_entry_serializes_optional_meal() {
        let entry = DayEntry {
            date: "2026-06-09".to_string(),
            meal: None,
            schedule: vec![],
            timetable: TimetableView {
                lessons: BTreeMap::new(),
                updated_at: None,
            },
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert!(json.get("meal").is_none() || json["meal"].is_null());
    }

    #[test]
    fn meta_data_skips_none_debug() {
        let meta = MetaData {
            version: "1.0".to_string(),
            build: 1,
            debug: None,
        };
        let json = serde_json::to_value(&meta).unwrap();
        assert!(json.get("debug").is_none());
    }
}
