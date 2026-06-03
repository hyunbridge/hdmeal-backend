//! UserService: 사용자 정보 upsert/update/delete + preferences 검증.
//!

use std::collections::HashMap;
use std::sync::Arc;

use crate::domain::{UserDocument, UserPreferences, ALLOWED_PREFERENCE_KEYS};
use crate::error::{HDMealError, HDMealResult};
use crate::repository::DataService;

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
        // grade 검증
        if let Some(Some(g)) = input.grade {
            if !(1..=num_grades as i32).contains(&g) {
                return Err(HDMealError::bad_request("올바르지 않은 요청입니다."));
            }
        }
        // class 검증
        if let Some(Some(c)) = input.class_no {
            if !(1..=num_classes as i32).contains(&c) {
                return Err(HDMealError::bad_request("올바르지 않은 요청입니다."));
            }
        }
        // preferences 검증
        let mut allergy: Option<String> = None;
        for (k, v) in &input.preferences {
            if !ALLOWED_PREFERENCE_KEYS.contains(&k.as_str()) {
                return Err(HDMealError::bad_request("올바르지 않은 요청입니다."));
            }
            if k == "AllergyInfo" && !UserPreferences::is_valid_allergy_info(v) {
                return Err(HDMealError::bad_request("올바르지 않은 요청입니다."));
            }
            if k == "AllergyInfo" {
                allergy = Some(v.clone());
            }
        }

        self.data
            .update_user(platform, external_id, input.grade, input.class_no, allergy)
            .await
    }

    pub async fn delete_user(&self, platform: &str, external_id: &str) -> HDMealResult<bool> {
        self.data.delete_user(platform, external_id).await
    }
}
