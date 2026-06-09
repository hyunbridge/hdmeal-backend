//! UserService: 사용자 정보 upsert/update/delete + preferences 검증.
//!

use std::collections::HashMap;
use std::sync::Arc;

use crate::domain::{UserDocument, ALLOWED_PREFERENCE_KEYS};
use crate::error::{HDMealError, HDMealResult};
use crate::repository::DataService;

const MAX_PREFERENCE_VALUE_LEN: usize = 64;

pub struct UserService {
    data: Arc<DataService>,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateUserInput {
    pub grade: Option<Option<i32>>,
    pub class_no: Option<Option<i32>>,
    pub preferences: HashMap<String, String>,
}

impl UserService {
    pub fn new(data: Arc<DataService>) -> Self {
        Self { data }
    }

    pub async fn ensure_user(
        &self,
        platform: &str,
        external_id: &str,
    ) -> HDMealResult<UserDocument> {
        self.data.ensure_user(platform, external_id).await
    }

    pub async fn get_user(
        &self,
        platform: &str,
        external_id: &str,
    ) -> HDMealResult<Option<UserDocument>> {
        self.data.get_user(platform, external_id).await
    }

    /// 입력값의 `grade` / `class_no` / `preferences` 를 검증한 뒤 업데이트.
    pub async fn update_user(
        &self,
        platform: &str,
        external_id: &str,
        input: UpdateUserInput,
        num_grades: u32,
        num_classes: u32,
    ) -> HDMealResult<UserDocument> {
        let allergy = validate_update_user_input(&input, num_grades, num_classes)?;

        self.data
            .update_user(platform, external_id, input.grade, input.class_no, allergy)
            .await
    }

    pub async fn delete_user(&self, platform: &str, external_id: &str) -> HDMealResult<bool> {
        self.data.delete_user(platform, external_id).await
    }
}

fn validate_update_user_input(
    input: &UpdateUserInput,
    num_grades: u32,
    num_classes: u32,
) -> HDMealResult<Option<String>> {
    if let Some(Some(g)) = input.grade {
        if !(1..=num_grades as i32).contains(&g) {
            return Err(HDMealError::bad_request("올바르지 않은 요청입니다."));
        }
    }
    if let Some(Some(c)) = input.class_no {
        if !(1..=num_classes as i32).contains(&c) {
            return Err(HDMealError::bad_request("올바르지 않은 요청입니다."));
        }
    }
    if input.preferences.len() > ALLOWED_PREFERENCE_KEYS.len() {
        return Err(HDMealError::bad_request("올바르지 않은 요청입니다."));
    }

    let mut allergy: Option<String> = None;
    for (k, v) in &input.preferences {
        if !ALLOWED_PREFERENCE_KEYS.contains(&k.as_str()) {
            return Err(HDMealError::bad_request("올바르지 않은 요청입니다."));
        }
        if v.len() > MAX_PREFERENCE_VALUE_LEN {
            return Err(HDMealError::bad_request("올바르지 않은 요청입니다."));
        }
        if k == "AllergyInfo" && !crate::domain::is_valid_allergy_info(v) {
            return Err(HDMealError::bad_request("올바르지 않은 요청입니다."));
        }
        if k == "AllergyInfo" {
            allergy = Some(v.clone());
        }
    }

    Ok(allergy)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_user_input_default() {
        let input = UpdateUserInput::default();
        assert!(input.grade.is_none());
        assert!(input.class_no.is_none());
        assert!(input.preferences.is_empty());
    }

    #[test]
    fn validate_update_user_input_accepts_empty_preferences() {
        let input = UpdateUserInput {
            grade: Some(Some(2)),
            class_no: Some(Some(5)),
            preferences: HashMap::new(),
        };
        let allergy = validate_update_user_input(&input, 3, 12).unwrap();
        assert!(allergy.is_none());
    }

    #[test]
    fn validate_update_user_input_accepts_allergy_info() {
        let mut preferences = HashMap::new();
        preferences.insert("AllergyInfo".to_string(), "FullText".to_string());
        let input = UpdateUserInput {
            grade: Some(Some(1)),
            class_no: Some(Some(1)),
            preferences,
        };
        let allergy = validate_update_user_input(&input, 3, 12).unwrap();
        assert_eq!(allergy.as_deref(), Some("FullText"));
    }

    #[test]
    fn validate_update_user_input_rejects_grade_out_of_bounds() {
        let input = UpdateUserInput {
            grade: Some(Some(0)),
            class_no: Some(Some(1)),
            preferences: HashMap::new(),
        };
        assert!(validate_update_user_input(&input, 3, 12).is_err());
    }

    #[test]
    fn validate_update_user_input_rejects_class_out_of_bounds() {
        let input = UpdateUserInput {
            grade: Some(Some(1)),
            class_no: Some(Some(13)),
            preferences: HashMap::new(),
        };
        assert!(validate_update_user_input(&input, 3, 12).is_err());
    }

    #[test]
    fn validate_update_user_input_rejects_too_many_preferences() {
        let mut prefs = HashMap::new();
        prefs.insert("AllergyInfo".to_string(), "Number".to_string());
        prefs.insert("ExtraKey".to_string(), "value".to_string());
        let input = UpdateUserInput {
            grade: Some(Some(1)),
            class_no: Some(Some(1)),
            preferences: prefs,
        };
        assert!(validate_update_user_input(&input, 3, 12).is_err());
    }

    #[test]
    fn validate_update_user_input_rejects_unknown_preference_key() {
        let mut prefs = HashMap::new();
        prefs.insert("UnknownKey".to_string(), "value".to_string());
        let input = UpdateUserInput {
            grade: Some(Some(1)),
            class_no: Some(Some(1)),
            preferences: prefs,
        };
        assert!(validate_update_user_input(&input, 3, 12).is_err());
    }

    #[test]
    fn validate_update_user_input_rejects_long_preference_value() {
        let mut prefs = HashMap::new();
        prefs.insert("AllergyInfo".to_string(), "x".repeat(65));
        let input = UpdateUserInput {
            grade: Some(Some(1)),
            class_no: Some(Some(1)),
            preferences: prefs,
        };
        assert!(validate_update_user_input(&input, 3, 12).is_err());
    }

    #[test]
    fn validate_update_user_input_rejects_invalid_allergy_info() {
        let mut prefs = HashMap::new();
        prefs.insert("AllergyInfo".to_string(), "Other".to_string());
        let input = UpdateUserInput {
            grade: Some(Some(1)),
            class_no: Some(Some(1)),
            preferences: prefs,
        };
        assert!(validate_update_user_input(&input, 3, 12).is_err());
    }
}
