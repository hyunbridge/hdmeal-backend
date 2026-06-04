//! NEIS OpenAPI 클라이언트.
//!
//! - `fetch_meals` (`mealServiceDietInfo`) — 중식(코드 2)만.
//! - `fetch_schedules` (`SchoolSchedule`).
//! - `fetch_timetables` (`hisTimetable`) — 페이지네이션 포함, 병렬 fetch.
//! - `fetch_all` — 위 셋을 병렬 호출.
//!
//! 메뉴 파싱:
//!   1. `<br/>` → `\n` 치환 후 줄 단위 분리.
//!   2. `re.compile(r"(\d+)\.")` 로 알레르기 번호 후보 추출.
//!   3. 1..18 범위만 유지.
//!   4. 알레르기 접미사 제거, 말미 `[ #&*-.=@_]+` 제거, `()` 제거.
//!   5. `data/delicious.txt` 의 키워드를 포함하면 `⭐` 접두.

use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;
use std::sync::LazyLock;

use regex::Regex;
use serde::Deserialize;

use super::http_client::HttpClient;
use crate::config::AppConfig;
use crate::domain::{
    MealDocument, MealMenuItem, ScheduleDocument, ScheduleEntry, TimetableDocument,
};
use crate::error::{HDMealError, HDMealResult};

/// "⭐" 마킹할 키워드. 컴파일 타임에 `data/delicious.txt` 를 임베드.
const DELICIOUS_KEYWORDS: &str = include_str!("../../../data/delicious.txt");

static DELICIOUS_SET: LazyLock<BTreeSet<&'static str>> = LazyLock::new(|| {
    DELICIOUS_KEYWORDS
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect()
});

static ALLERGY_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(\d+)\.").unwrap());
static TRAILING_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[ #&*\-.\-=@_]+$").unwrap());

const NEIS_BASE: &str = "https://open.neis.go.kr/hub";
const MMEAL_SC_LUNCH: &str = "2";
const TIMETABLE_PAGE_SIZE: u32 = 1000;

/// NEIS 클라이언트.
#[derive(Clone)]
pub struct NeisClient {
    pub config: AppConfig,
    pub http: HttpClient,
}

impl NeisClient {
    pub fn new(config: AppConfig, http: HttpClient) -> Self {
        Self { config, http }
    }

    fn common_params(&self) -> Vec<(&'static str, String)> {
        vec![
            ("KEY", self.config.neis_openapi_token.clone()),
            ("Type", "json".to_string()),
            ("ATPT_OFCDC_SC_CODE", self.config.atpt_ofcdc_sc_code.clone()),
            ("SD_SCHUL_CODE", self.config.sd_schul_code.clone()),
        ]
    }

    /// 중식만 (코드 2) 가져오기.
    pub async fn fetch_meals(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> HDMealResult<Vec<MealDocument>> {
        let mut params = self.common_params();
        params.push(("MMEAL_SC_CODE", MMEAL_SC_LUNCH.to_string()));
        params.push(("MLSV_FROM_YMD", start.format("%Y%m%d").to_string()));
        params.push(("MLSV_TO_YMD", end.format("%Y%m%d").to_string()));
        let url = format!("{NEIS_BASE}/mealServiceDietInfo");
        let raw: serde_json::Value = self.http.get_json_with_params(&url, &params).await?;
        let info = extract_service(&raw, "mealServiceDietInfo")?;
        let rows = info
            .get("row")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        let mut out = Vec::new();
        for row in rows {
            let row: MealRow = serde_json::from_value(row)?;
            let date = parse_neis_ymd(&row.MLSV_YMD)
                .ok_or_else(|| HDMealError::internal("invalid MLSV_YMD"))?;
            let (menus, plain) = parse_ddish_nm(&row.DDISH_NM);
            let calories = row
                .CAL_INFO
                .as_deref()
                .and_then(|s| s.replace(" Kcal", "").trim().parse::<f64>().ok());
            out.push(MealDocument {
                id: date.clone(),
                date,
                menus,
                menus_plain: plain,
                calories,
                source_hash: None,
                created_at: chrono::Utc::now(),
            });
        }
        Ok(out)
    }

    pub async fn fetch_schedules(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> HDMealResult<Vec<ScheduleDocument>> {
        let mut params = self.common_params();
        params.push(("AA_FROM_YMD", start.format("%Y%m%d").to_string()));
        params.push(("AA_TO_YMD", end.format("%Y%m%d").to_string()));
        let url = format!("{NEIS_BASE}/SchoolSchedule");
        let raw: serde_json::Value = self.http.get_json_with_params(&url, &params).await?;
        let info = extract_service(&raw, "SchoolSchedule")?;
        let rows = info
            .get("row")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        let mut grouped: BTreeMap<String, Vec<ScheduleEntry>> = BTreeMap::new();
        for row in rows {
            let row: ScheduleRow = serde_json::from_value(row)?;
            if row.EVENT_NM == "토요휴업일" {
                continue;
            }
            let date = parse_neis_ymd(&row.AA_YMD)
                .ok_or_else(|| HDMealError::internal("invalid AA_YMD"))?;
            let mut grades = Vec::new();
            if row.ONE_GRADE_EVENT_YN == "Y" {
                grades.push(1);
            }
            if row.TW_GRADE_EVENT_YN == "Y" {
                grades.push(2);
            }
            if row.THREE_GRADE_EVENT_YN == "Y" {
                grades.push(3);
            }
            if row.FR_GRADE_EVENT_YN == "Y" {
                grades.push(4);
            }
            if row.FIV_GRADE_EVENT_YN == "Y" {
                grades.push(5);
            }
            if row.SIX_GRADE_EVENT_YN == "Y" {
                grades.push(6);
            }
            grouped.entry(date).or_default().push(ScheduleEntry {
                name: row.EVENT_NM,
                grades,
            });
        }

        let mut out = Vec::new();
        for (date, entries) in grouped {
            let summary_lines: Vec<String> = entries
                .iter()
                .map(|entry| {
                    let suffix = if entry.grades.is_empty() {
                        String::new()
                    } else {
                        format!(
                            "({})",
                            entry
                                .grades
                                .iter()
                                .map(|g| format!("{g}학년"))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    };
                    format!("{}{}", entry.name, suffix)
                })
                .collect();
            let summary = if summary_lines.is_empty() {
                None
            } else {
                Some(summary_lines.join("\n").replace("()", ""))
            };
            out.push(ScheduleDocument {
                id: date.clone(),
                date,
                entries,
                summary,
                created_at: chrono::Utc::now(),
            });
        }
        Ok(out)
    }

    /// 페이지네이션된 시간표를 병렬로 가져와 merge.
    pub async fn fetch_timetables(&self, date: NaiveDate) -> HDMealResult<Vec<TimetableDocument>> {
        // 1) 첫 페이지로 총 개수 파악.
        let mut first_params = self.common_params();
        first_params.push(("ALL_TI_YMD", date.format("%Y%m%d").to_string()));
        first_params.push(("pIndex", "1".to_string()));
        first_params.push(("pSize", TIMETABLE_PAGE_SIZE.to_string()));
        let url = format!("{NEIS_BASE}/hisTimetable");
        let first: serde_json::Value = self.http.get_json_with_params(&url, &first_params).await?;
        let info = extract_service(&first, "hisTimetable")?;
        let total = extract_list_total(info);
        if total == 0 {
            return Ok(Vec::new());
        }
        let total_pages = total.div_ceil(TIMETABLE_PAGE_SIZE);

        // 2) 나머지 페이지를 병렬로.
        let pages: Vec<u32> = (2..=total_pages).collect();
        let mut handles = Vec::with_capacity(pages.len());
        for p in pages {
            let mut p_params = self.common_params();
            p_params.push(("ALL_TI_YMD", date.format("%Y%m%d").to_string()));
            p_params.push(("pIndex", p.to_string()));
            p_params.push(("pSize", TIMETABLE_PAGE_SIZE.to_string()));
            let http = self.http.clone();
            let url = url.clone();
            handles.push(tokio::spawn(async move {
                let raw: serde_json::Value = http.get_json_with_params(&url, &p_params).await?;
                let info = extract_service(&raw, "hisTimetable")?;
                let rows = info
                    .get("row")
                    .and_then(|r| r.as_array())
                    .cloned()
                    .unwrap_or_default();
                Ok::<_, HDMealError>(rows)
            }));
        }

        // 3) 첫 페이지 + 나머지 merge.
        let mut all_rows: Vec<serde_json::Value> = info
            .get("row")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        for h in handles {
            match h.await {
                Ok(Ok(rows)) => all_rows.extend(rows),
                Ok(Err(e)) => tracing::warn!(error = %e, "timetable page fetch failed"),
                Err(e) => tracing::warn!(error = %e, "timetable page task join failed"),
            }
        }

        // 4) row → lessons
        let date_str = date.format("%Y-%m-%d").to_string();
        let mut lessons: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
        for row in all_rows {
            let row: TimetableRow = serde_json::from_value(row)?;
            if row.ITRT_CNTNT == "토요휴업일" {
                continue;
            }
            let g = row.GRADE.to_string();
            let c = row.CLASS_NM.to_string();
            let perio = row.PERIO;
            let subject = row.ITRT_CNTNT;

            let inner = lessons.entry(g).or_default();
            let arr = inner.entry(c).or_default();
            let idx = perio.saturating_sub(1) as usize;
            while arr.len() <= idx {
                arr.push(String::new());
            }
            arr[idx] = subject;
        }
        Ok(vec![TimetableDocument {
            id: date_str.clone(),
            date: date_str,
            lessons,
            created_at: chrono::Utc::now(),
        }])
    }

    /// 세 호출을 병렬로.
    pub async fn fetch_all(&self, start: NaiveDate, end: NaiveDate) -> HDMealResult<NeisFetchAll> {
        let meals = self.fetch_meals(start, end);
        let schedules = self.fetch_schedules(start, end);
        // 시간표는 각 날짜별로 호출.
        let timetable_dates: Vec<NaiveDate> = date_range(start, end);
        let neis = self.clone();
        let timetables = async move {
            let mut out = Vec::new();
            for d in timetable_dates {
                match neis.fetch_timetables(d).await {
                    Ok(mut v) => out.append(&mut v),
                    Err(e) => tracing::warn!(error = %e, "timetable fetch failed for {}", d),
                }
            }
            Ok::<_, HDMealError>(out)
        };

        let (meals, schedules, timetables) = tokio::join!(meals, schedules, timetables);
        Ok(NeisFetchAll {
            meals: meals?,
            schedules: schedules?,
            timetables: timetables?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct NeisFetchAll {
    pub meals: Vec<MealDocument>,
    pub schedules: Vec<ScheduleDocument>,
    pub timetables: Vec<TimetableDocument>,
}

// ----------------- HTTP envelope / raw models -----------------

/// NEIS 응답 envelope.
/// `{"<serviceName>": [{"head": [...], "row": [...]}]}` 형태를 그대로 매핑.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct NeisRawService {
    #[serde(default)]
    head: Vec<NeisHead>,
    #[serde(default)]
    row: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct NeisHead {
    #[serde(rename = "list_total_count")]
    list_total_count: Option<u32>,
    #[serde(rename = "RESULT")]
    result: Option<NeisResultInfo>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct NeisResultInfo {
    code: String,
    message: String,
}

/// raw JSON envelope 에서 serviceName 의 head / row 추출 + RESULT.INFO-000 검증.
fn extract_service<'a>(v: &'a serde_json::Value, key: &str) -> HDMealResult<&'a serde_json::Value> {
    let info = v
        .get(key)
        .and_then(|x| x.as_array())
        .and_then(|arr| arr.first())
        .ok_or_else(|| HDMealError::not_found(format!("missing NEIS key {key}")))?;
    if let Some(head) = info
        .get("head")
        .and_then(|h| h.as_array())
        .and_then(|a| a.first())
    {
        if let Some(r) = head.get("RESULT") {
            let code = r.get("code").and_then(|c| c.as_str()).unwrap_or("");
            if code != "INFO-000" {
                let msg = r
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("NEIS error");
                return Err(HDMealError::service_unavailable(format!("NEIS: {msg}")));
            }
        }
    }
    Ok(info)
}

fn extract_list_total(v: &serde_json::Value) -> u32 {
    v.get("head")
        .and_then(|h| h.as_array())
        .and_then(|a| a.first())
        .and_then(|h| h.get("list_total_count"))
        .and_then(|x| x.as_u64())
        .unwrap_or(0) as u32
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[allow(non_snake_case)]
struct MealRow {
    #[serde(rename = "DDISH_NM")]
    DDISH_NM: String,
    #[serde(rename = "CAL_INFO")]
    CAL_INFO: Option<String>,
    #[serde(rename = "MLSV_YMD")]
    MLSV_YMD: String,
    #[serde(rename = "MMEAL_SC_NM", default)]
    _MMEAL_SC_NM: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[allow(non_snake_case)]
struct ScheduleRow {
    #[serde(rename = "AA_YMD")]
    AA_YMD: String,
    #[serde(rename = "EVENT_NM")]
    EVENT_NM: String,
    #[serde(rename = "ONE_GRADE_EVENT_YN")]
    ONE_GRADE_EVENT_YN: String,
    #[serde(rename = "TW_GRADE_EVENT_YN")]
    TW_GRADE_EVENT_YN: String,
    #[serde(rename = "THREE_GRADE_EVENT_YN")]
    THREE_GRADE_EVENT_YN: String,
    #[serde(rename = "FR_GRADE_EVENT_YN")]
    FR_GRADE_EVENT_YN: String,
    #[serde(rename = "FIV_GRADE_EVENT_YN")]
    FIV_GRADE_EVENT_YN: String,
    #[serde(rename = "SIX_GRADE_EVENT_YN")]
    SIX_GRADE_EVENT_YN: String,
    #[serde(rename = "SBTR_DD_SC_NM", default)]
    #[allow(dead_code)]
    SBTR_DD_SC_NM: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[allow(non_snake_case)]
struct TimetableRow {
    #[serde(rename = "GRADE")]
    GRADE: u32,
    #[serde(rename = "CLASS_NM")]
    CLASS_NM: u32,
    #[serde(rename = "PERIO")]
    PERIO: usize,
    #[serde(rename = "ITRT_CNTNT")]
    ITRT_CNTNT: String,
}

// ----------------- helpers -----------------

fn date_range(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut out = Vec::new();
    let mut d = start;
    while d <= end {
        out.push(d);
        d = d.succ_opt().unwrap_or(d);
    }
    out
}

fn parse_neis_ymd(s: &str) -> Option<String> {
    if s.len() != 8 {
        return None;
    }
    let y = s[0..4].parse::<i32>().ok()?;
    let m = s[4..6].parse::<u32>().ok()?;
    let d = s[6..8].parse::<u32>().ok()?;
    NaiveDate::from_ymd_opt(y, m, d).map(|d| d.format("%Y-%m-%d").to_string())
}

/// `DDISH_NM` (e.g. `<br/>` 포함 HTML 문자열) 을 파싱해 (메뉴, 알레르기) 쌍과
/// 평문 메뉴명 배열을 반환.
pub fn parse_ddish_nm(raw: &str) -> (Vec<MealMenuItem>, Vec<String>) {
    let mut menus = Vec::new();
    let mut plain = Vec::new();
    for line in raw.split(['\n', '\r']) {
        let line = line.replace("<br/>", "\n");
        for inner in line.split('\n') {
            let trimmed = inner.trim();
            if trimmed.is_empty() {
                continue;
            }
            // (1) 알레르기 번호 후보 추출 → 1..18 만 채택
            let mut allergies: Vec<i32> = ALLERGY_RE
                .captures_iter(trimmed)
                .filter_map(|cap| cap.get(1).and_then(|m| m.as_str().parse::<i32>().ok()))
                .filter(|n| (1..=18).contains(n))
                .collect();
            allergies.sort_unstable();
            allergies.dedup();

            // (2) 알레르기 접미사 제거
            let mut name = ALLERGY_RE.replace_all(trimmed, "").to_string();
            // (3) 말미 [ #&*-.=@_]+ 제거
            name = TRAILING_RE.replace(&name, "").to_string();
            // (4) "()" 제거
            name = name.replace("()", "");
            let name = name.trim().to_string();
            if name.is_empty() {
                continue;
            }

            // (5) delicious keyword 가 포함되면 ⭐ 마킹
            let contains_delicious = DELICIOUS_SET.iter().any(|kw| name.contains(*kw));
            let final_name = if contains_delicious {
                format!("⭐{name}")
            } else {
                name.clone()
            };

            menus.push(MealMenuItem {
                name: final_name,
                allergies,
            });
            plain.push(name);
        }
    }
    (menus, plain)
}

impl HttpClient {
    /// 쿼리 파라미터를 URL 에 추가해 GET.
    pub async fn get_json_with_params<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        params: &[(&str, String)],
    ) -> HDMealResult<T> {
        let mut url = url.to_string();
        if !params.is_empty() {
            url.push('?');
            let mut first = true;
            for (k, v) in params {
                if !first {
                    url.push('&');
                }
                first = false;
                url.push_str(&format!(
                    "{k}={}",
                    percent_encoding::utf8_percent_encode(v, percent_encoding::NON_ALPHANUMERIC)
                ));
            }
        }
        let resp = self
            .get_with_retry(&url, reqwest::header::HeaderMap::new())
            .await?;
        let val = resp.json::<T>().await?;
        Ok(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ddish_basic() {
        let raw = "밥<br/>김치찌개1.2.5.<br/>돈까스6.10.<br/>";
        let (menus, plain) = parse_ddish_nm(raw);
        assert_eq!(menus.len(), 3);
        assert_eq!(menus[0].allergies, Vec::<i32>::new());
        assert_eq!(menus[1].allergies, vec![1, 2, 5]);
        assert_eq!(menus[2].allergies, vec![6, 10]);
        // 돈까스 contains 까스 (delicious keyword) → ⭐ prefix
        assert!(menus[2].name.starts_with("⭐"));
        assert!(menus[2].name.contains("돈까스"));
        assert_eq!(plain, vec!["밥", "김치찌개", "돈까스"]);
    }

    #[test]
    fn parse_ddish_marks_delicious() {
        let (menus, _) = parse_ddish_nm("돈까스<br/>떡볶이");
        assert!(menus[0].name.starts_with("⭐"));
        assert!(menus[1].name.starts_with("⭐"));
    }

    #[test]
    fn parse_ddish_strips_trailing_punct() {
        // "바나나" is in delicious.txt, so name gets ⭐.
        let (menus, _) = parse_ddish_nm("바나나.#&-");
        assert!(menus[0].name.contains("바나나"));
    }

    #[test]
    fn parse_neis_ymd_valid() {
        assert_eq!(parse_neis_ymd("20240301"), Some("2024-03-01".to_string()));
        assert!(parse_neis_ymd("2024-03-01").is_none());
    }
}
