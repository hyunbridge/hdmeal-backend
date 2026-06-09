//! PATCH /user/settings/ 요청 DTO.
//!
//! `user_grade` / `user_class` 는 int 또는 string ("1학년") 둘 다 허용 —
//! loose int deserializer 로 정규화.

use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Deserialize)]
pub struct UpdateUserSettingsRequest {
    #[serde(rename = "user_grade", deserialize_with = "deserialize_loose_int")]
    pub user_grade: i32,

    #[serde(rename = "user_class", deserialize_with = "deserialize_loose_int")]
    pub user_class: i32,

    #[serde(default)]
    pub preferences: HashMap<String, String>,
}

fn deserialize_loose_int<'de, D>(d: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let v = serde_json::Value::deserialize(d)?;
    match v {
        serde_json::Value::Number(n) => n
            .as_i64()
            .and_then(|x| i32::try_from(x).ok())
            .ok_or_else(|| D::Error::custom("invalid number")),
        serde_json::Value::String(s) => {
            let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
            digits
                .parse::<i32>()
                .map_err(|_| D::Error::custom(format!("invalid grade/class: {s}")))
        }
        _ => Err(D::Error::custom("expected number or string")),
    }
}

#[derive(Debug, Serialize)]
pub struct UserSettingsResponse {
    pub classes: Vec<i32>,
    pub grades: Vec<i32>,
    pub current_grade: Option<i32>,
    pub current_class: Option<i32>,
    pub preferences: UserSettingsPreferences,
}

#[derive(Debug, Serialize)]
pub struct UserSettingsPreferences {
    #[serde(rename = "AllergyInfo")]
    pub allergy_info: String,
}

#[derive(Debug, Serialize)]
pub struct UserSettingsMessage {
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_loose_int_from_number() {
        let json = r#"{"user_grade": 2, "user_class": 3}"#;
        let req: UpdateUserSettingsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.user_grade, 2);
        assert_eq!(req.user_class, 3);
    }

    #[test]
    fn deserialize_loose_int_from_korean_string() {
        let json = r#"{"user_grade": "2학년", "user_class": "3반"}"#;
        let req: UpdateUserSettingsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.user_grade, 2);
        assert_eq!(req.user_class, 3);
    }

    #[test]
    fn deserialize_loose_int_from_plain_string() {
        let json = r#"{"user_grade": "1", "user_class": "5"}"#;
        let req: UpdateUserSettingsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.user_grade, 1);
        assert_eq!(req.user_class, 5);
    }

    #[test]
    fn deserialize_loose_int_rejects_non_numeric_string() {
        let json = r#"{"user_grade": "abc", "user_class": 1}"#;
        assert!(serde_json::from_str::<UpdateUserSettingsRequest>(json).is_err());
    }

    #[test]
    fn deserialize_preferences_default_empty() {
        let json = r#"{"user_grade": 1, "user_class": 1}"#;
        let req: UpdateUserSettingsRequest = serde_json::from_str(json).unwrap();
        assert!(req.preferences.is_empty());
    }

    #[test]
    fn deserialize_preferences_provided() {
        let json =
            r#"{"user_grade": 1, "user_class": 1, "preferences": {"AllergyInfo": "Number"}}"#;
        let req: UpdateUserSettingsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.preferences.get("AllergyInfo").unwrap(), "Number");
    }

    #[test]
    fn user_settings_response_serializes() {
        let resp = UserSettingsResponse {
            classes: vec![1, 2, 3],
            grades: vec![1, 2],
            current_grade: Some(2),
            current_class: Some(3),
            preferences: UserSettingsPreferences {
                allergy_info: "Number".to_string(),
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["grades"][0], 1);
        assert_eq!(json["grades"][1], 2);
        assert_eq!(json["classes"][0], 1);
        assert_eq!(json["classes"][2], 3);
        assert_eq!(json["current_grade"], 2);
        assert_eq!(json["preferences"]["AllergyInfo"], "Number");
    }
}
