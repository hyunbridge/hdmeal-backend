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

use std::collections::BTreeMap;
use std::sync::LazyLock;

use chrono::NaiveDate;
use regex::Regex;
use serde::{Deserialize, Deserializer};

use super::http_client::HttpClient;
use crate::config::AppConfig;
use crate::domain::{
    MealDocument, MealMenuItem, ScheduleDocument, ScheduleEntry, TimetableDocument,
};
use crate::error::{HDMealError, HDMealResult};

const DELICIOUS_KEYWORDS: &str = include_str!("../../../data/delicious.txt");

static DELICIOUS_VEC: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    let mut v: Vec<&'static str> = DELICIOUS_KEYWORDS
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();
    v.sort_unstable();
    v.dedup();
    v
});

static ALLERGY_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(\d+)\.").unwrap());
static TRAILING_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[ #&*\-.\-=@_]+$").unwrap());

const NEIS_BASE: &str = "https://open.neis.go.kr/hub";
const MMEAL_SC_LUNCH: &str = "2";
const TIMETABLE_PAGE_SIZE: u32 = 1000;

/// NEIS 클라이언트.
#[derive(Clone)]
pub struct NeisClient {
    config: AppConfig,
    http: HttpClient,
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
        let Some(info) = extract_service(&raw, "mealServiceDietInfo")? else {
            return Ok(Vec::new());
        };
        let rows = info
            .row
            .get("row")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        let mut out = Vec::new();
        for row in rows {
            let row: MealRow = serde_json::from_value(row)?;
            let date = parse_neis_ymd(&row.MLSV_YMD).ok_or_else(|| {
                HDMealError::internal(format!("invalid MLSV_YMD {}", row.MLSV_YMD))
            })?;
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
        let Some(info) = extract_service(&raw, "SchoolSchedule")? else {
            return Ok(Vec::new());
        };
        let rows = info
            .row
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
                .ok_or_else(|| HDMealError::internal(format!("invalid AA_YMD {}", row.AA_YMD)))?;
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
                Some(summary_lines.join("\n"))
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
        let date_str = date.format("%Y-%m-%d").to_string();
        // 1) 첫 페이지로 총 개수 파악.
        let mut first_params = self.common_params();
        first_params.push(("ALL_TI_YMD", date.format("%Y%m%d").to_string()));
        first_params.push(("pIndex", "1".to_string()));
        first_params.push(("pSize", TIMETABLE_PAGE_SIZE.to_string()));
        let url = format!("{NEIS_BASE}/hisTimetable");
        let first: serde_json::Value = self.http.get_json_with_params(&url, &first_params).await?;
        let Some(info) = extract_service(&first, "hisTimetable")? else {
            return Ok(vec![empty_timetable_document(&date_str)]);
        };
        let total = extract_list_total(info.head);
        if total == 0 {
            return Ok(vec![empty_timetable_document(&date_str)]);
        }
        let total_pages = total.div_ceil(TIMETABLE_PAGE_SIZE);

        // 2) 나머지 페이지를 병렬로.
        let mut handles = Vec::with_capacity(total_pages.saturating_sub(1) as usize);
        for p in 2..=total_pages {
            let mut p_params = self.common_params();
            p_params.push(("ALL_TI_YMD", date.format("%Y%m%d").to_string()));
            p_params.push(("pIndex", p.to_string()));
            p_params.push(("pSize", TIMETABLE_PAGE_SIZE.to_string()));
            let http = self.http.clone();
            let url = url.clone();
            handles.push(tokio::spawn(async move {
                let raw: serde_json::Value = http.get_json_with_params(&url, &p_params).await?;
                let rows = match extract_service(&raw, "hisTimetable")? {
                    Some(info) => info
                        .row
                        .get("row")
                        .and_then(|r| r.as_array())
                        .cloned()
                        .unwrap_or_default(),
                    None => Vec::new(),
                };
                Ok::<_, HDMealError>(rows)
            }));
        }

        // 3) 첫 페이지 + 나머지 merge.
        let mut all_rows: Vec<serde_json::Value> = info
            .row
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
        let mut lessons: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
        for row in all_rows {
            let row: TimetableRow = match serde_json::from_value(row) {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(error = %e, date = %date_str, "skipping malformed timetable row");
                    continue;
                }
            };
            if row.ITRT_CNTNT == "토요휴업일" {
                continue;
            }
            let g = row.GRADE.to_string();
            let c = row.CLASS_NM.to_string();
            let perio = row.PERIO;
            let subject = row.ITRT_CNTNT;
            if perio == 0 {
                tracing::warn!("skipping timetable row with PERIO=0 for {}", date_str);
                continue;
            }

            let inner = lessons.entry(g).or_default();
            let arr = inner.entry(c).or_default();
            let idx = perio.saturating_sub(1) as usize;
            arr.resize(idx + 1, String::new());
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
            let handles: Vec<_> = timetable_dates
                .into_iter()
                .map(|d| {
                    let neis = neis.clone();
                    tokio::spawn(async move { (d, neis.fetch_timetables(d).await) })
                })
                .collect();
            let mut out = Vec::new();
            for handle in handles {
                match handle.await {
                    Ok((_, Ok(mut v))) => out.append(&mut v),
                    Ok((d, Err(e))) => {
                        tracing::warn!(error = %e, "timetable fetch failed for {}", d);
                    }
                    Err(e) => tracing::warn!(error = %e, "timetable date task join failed"),
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

#[derive(Debug, Clone, Copy)]
struct NeisServiceInfo<'a> {
    head: &'a serde_json::Value,
    row: &'a serde_json::Value,
}

/// raw JSON envelope 에서 serviceName 의 `head` / `row` 를 추출하고
/// `RESULT.INFO-000` 을 검증한다.
///
/// NEIS 는 무데이터를 root `RESULT.INFO-200` 으로 반환하므로 `Ok(None)` 으로 표현한다.
fn extract_service<'a>(
    v: &'a serde_json::Value,
    key: &str,
) -> HDMealResult<Option<NeisServiceInfo<'a>>> {
    let Some(items) = v.get(key).and_then(|x| x.as_array()) else {
        if is_no_data_result(v) {
            return Ok(None);
        }
        return Err(HDMealError::not_found(format!("missing NEIS key {key}")));
    };

    let mut head: Option<&serde_json::Value> = None;
    let mut row: Option<&serde_json::Value> = None;
    for item in items {
        if head.is_none() && item.get("head").and_then(|h| h.as_array()).is_some() {
            head = Some(item);
        }
        if row.is_none() && item.get("row").and_then(|r| r.as_array()).is_some() {
            row = Some(item);
        }

        if let Some(head_items) = item.get("head").and_then(|h| h.as_array()) {
            for head_item in head_items {
                let Some(r) = head_item.get("RESULT") else {
                    continue;
                };
                let code = neis_result_field(r, "code", "CODE").unwrap_or("");
                if code == "INFO-200" {
                    return Ok(None);
                }
                if code != "INFO-000" {
                    let msg = r
                        .get("message")
                        .or_else(|| r.get("MESSAGE"))
                        .and_then(|m| m.as_str())
                        .unwrap_or("NEIS error");
                    return Err(HDMealError::service_unavailable(format!("NEIS: {msg}")));
                }
            }
        }
    }

    match (head, row) {
        (Some(head), Some(row)) => Ok(Some(NeisServiceInfo { head, row })),
        _ if is_no_data_result(v) => Ok(None),
        _ => Err(HDMealError::not_found(format!(
            "missing NEIS head/row for {key}"
        ))),
    }
}

fn is_no_data_result(v: &serde_json::Value) -> bool {
    v.get("RESULT")
        .and_then(|r| neis_result_field(r, "code", "CODE"))
        == Some("INFO-200")
}

fn neis_result_field<'a>(
    result: &'a serde_json::Value,
    lower: &str,
    upper: &str,
) -> Option<&'a str> {
    result
        .get(lower)
        .or_else(|| result.get(upper))
        .and_then(|v| v.as_str())
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
    #[serde(rename = "GRADE", deserialize_with = "deserialize_loose_u32")]
    GRADE: u32,
    #[serde(rename = "CLASS_NM", deserialize_with = "deserialize_loose_u32")]
    CLASS_NM: u32,
    #[serde(rename = "PERIO", deserialize_with = "deserialize_loose_usize")]
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
        let Some(next) = d.succ_opt() else {
            break;
        };
        d = next;
    }
    out
}

fn empty_timetable_document(date_str: &str) -> TimetableDocument {
    TimetableDocument {
        id: date_str.to_string(),
        date: date_str.to_string(),
        lessons: BTreeMap::new(),
        created_at: chrono::Utc::now(),
    }
}

fn deserialize_loose_u32<'de, D>(d: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let v = serde_json::Value::deserialize(d)?;
    match v {
        serde_json::Value::Number(n) => n
            .as_u64()
            .and_then(|x| u32::try_from(x).ok())
            .ok_or_else(|| D::Error::custom("invalid u32")),
        serde_json::Value::String(s) => {
            let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
            digits
                .parse::<u32>()
                .map_err(|_| D::Error::custom(format!("invalid u32: {s}")))
        }
        _ => Err(D::Error::custom("expected number or string")),
    }
}

fn deserialize_loose_usize<'de, D>(d: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let v = serde_json::Value::deserialize(d)?;
    match v {
        serde_json::Value::Number(n) => n
            .as_u64()
            .and_then(|x| usize::try_from(x).ok())
            .ok_or_else(|| D::Error::custom("invalid usize")),
        serde_json::Value::String(s) => {
            let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
            digits
                .parse::<usize>()
                .map_err(|_| D::Error::custom(format!("invalid usize: {s}")))
        }
        _ => Err(D::Error::custom("expected number or string")),
    }
}

fn parse_neis_ymd(s: &str) -> Option<String> {
    NaiveDate::parse_from_str(s, "%Y%m%d")
        .ok()
        .map(|d| d.format("%Y-%m-%d").to_string())
}

/// `DDISH_NM` (e.g. `<br/>` 포함 HTML 문자열) 을 파싱해 (메뉴, 알레르기) 쌍과
/// 평문 메뉴명 배열을 반환.
pub fn parse_ddish_nm(raw: &str) -> (Vec<MealMenuItem>, Vec<String>) {
    let mut menus = Vec::new();
    let mut plain = Vec::new();
    let normalized = raw.replace("<br/>", "\n");
    for inner in normalized.split(['\n', '\r']) {
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

        let mut name = ALLERGY_RE.replace_all(trimmed, "").into_owned();
        name = TRAILING_RE.replace(&name, "").into_owned();
        name = name.replace("()", "");
        let name = name.trim().to_owned();
        if name.is_empty() {
            continue;
        }

        let contains_delicious = DELICIOUS_VEC.iter().any(|kw| name.contains(*kw));
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
    (menus, plain)
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

    #[test]
    fn timetable_row_accepts_string_numbers() {
        let raw = serde_json::json!({
            "GRADE": "1",
            "CLASS_NM": "2",
            "PERIO": "3",
            "ITRT_CNTNT": "국어"
        });
        let row: TimetableRow = serde_json::from_value(raw).unwrap();
        assert_eq!(row.GRADE, 1);
        assert_eq!(row.CLASS_NM, 2);
        assert_eq!(row.PERIO, 3);
        assert_eq!(row.ITRT_CNTNT, "국어");
    }

    #[test]
    fn extract_service_treats_root_info_200_as_no_data() {
        let raw = serde_json::json!({
            "RESULT": {
                "CODE": "INFO-200",
                "MESSAGE": "해당하는 데이터가 없습니다."
            }
        });

        assert!(extract_service(&raw, "mealServiceDietInfo")
            .unwrap()
            .is_none());
    }

    #[test]
    fn extract_service_treats_service_info_200_as_no_data() {
        let raw = serde_json::json!({
            "hisTimetable": [{
                "head": [{
                    "RESULT": {
                        "CODE": "INFO-200",
                        "MESSAGE": "해당하는 데이터가 없습니다."
                    }
                }]
            }]
        });

        assert!(extract_service(&raw, "hisTimetable").unwrap().is_none());
    }

    #[test]
    fn extract_service_returns_both_head_and_row() {
        let raw = serde_json::json!({
            "hisTimetable": [
                {
                    "head": [
                        {"list_total_count": 2},
                        {
                            "RESULT": {
                                "CODE": "INFO-000",
                                "MESSAGE": "정상 처리되었습니다."
                            }
                        }
                    ]
                },
                {
                    "row": [
                        {
                            "GRADE": "1",
                            "CLASS_NM": "1",
                            "PERIO": "1",
                            "ITRT_CNTNT": "국어"
                        }
                    ]
                }
            ]
        });

        let info = extract_service(&raw, "hisTimetable").unwrap().unwrap();
        assert_eq!(extract_list_total(info.head), 2);
        assert_eq!(
            info.row
                .get("row")
                .and_then(|r| r.as_array())
                .map(|r| r.len()),
            Some(1)
        );
    }

    #[test]
    fn extract_service_rejects_non_success_result() {
        let raw = serde_json::json!({
            "SchoolSchedule": [{
                "head": [
                    {"list_total_count": 1},
                    {
                        "RESULT": {
                            "CODE": "ERROR-300",
                            "MESSAGE": "인증 실패"
                        }
                    }
                ]
            }]
        });

        let err = extract_service(&raw, "SchoolSchedule").unwrap_err();
        assert_eq!(err.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    }
}
