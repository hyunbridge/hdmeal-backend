//! KST (Asia/Seoul) 시간 헬퍼.
//!
//! - [`KST`]: `+09:00` 고정 오프셋. DST 가 없으므로 `chrono_tz` 없이도 안전.
//! - [`today_kst`], [`now_kst`]: 현재 KST 시각.
//! - [`format_weekday_ko`]: "월" ~ "일" 한글 요일.
//! - [`format_hour`]: "오전 12시" / "오후 3시" 등.
//! - [`to_kst_iso`]: Mongo `created_at` 등을 응답 직렬화용 KST ISO8601 로.

use std::borrow::Cow;

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc};

/// KST = UTC+9. 일광절약시간 없음.
pub const KST_OFFSET_SECS: i32 = 9 * 3600;
pub const KST: FixedOffset = FixedOffset::east_opt(KST_OFFSET_SECS).unwrap();

/// 현재 KST 시각.
pub fn now_kst() -> DateTime<FixedOffset> {
    let now = Utc::now();
    now.with_timezone(&KST)
}

/// KST 기준 오늘 (00:00:00 KST 의 NaiveDate).
pub fn today_kst_date() -> NaiveDate {
    now_kst().date_naive()
}

/// 주어진 KST 날짜 + 00:00:00 으로 고정.
pub fn kst_date_at_start(date: NaiveDate) -> DateTime<FixedOffset> {
    KST.from_local_datetime(&date.and_hms_opt(0, 0, 0).unwrap())
        .unwrap()
}

/// "월" ~ "일" 한글 요일, 월요일=0 순서.
pub fn format_weekday_ko(date: NaiveDate) -> &'static str {
    const KO: [&str; 7] = ["월", "화", "수", "목", "금", "토", "일"];
    let idx = date.weekday().num_days_from_monday() as usize;
    KO[idx]
}

/// "YYYY-MM-DD(요일)" 형식.
pub fn format_date_label(date: NaiveDate) -> String {
    format!("{}({})", date.format("%Y-%m-%d"), format_weekday_ko(date))
}

/// "YYYYMMDD" 형식 (NEIS API 파라미터용).
pub fn format_neis_date(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

/// `0..=24` → 한국어 시각 라벨. 정적 lookup table.
const HOUR_LABELS: [&str; 25] = [
    // 0
    "오전 12시",
    // 1..=11
    "오전 1시",
    "오전 2시",
    "오전 3시",
    "오전 4시",
    "오전 5시",
    "오전 6시",
    "오전 7시",
    "오전 8시",
    "오전 9시",
    "오전 10시",
    "오전 11시",
    // 12
    "오후 12시",
    // 13..=23
    "오후 1시",
    "오후 2시",
    "오후 3시",
    "오후 4시",
    "오후 5시",
    "오후 6시",
    "오후 7시",
    "오후 8시",
    "오후 9시",
    "오후 10시",
    "오후 11시",
    // 24
    "오전 12시",
];

/// "오전 12시" / "오전 3시" / "오후 12시" / "오후 3시" 등.
pub fn format_hour(h: u32) -> Cow<'static, str> {
    match HOUR_LABELS.get(h as usize) {
        Some(&s) => Cow::Borrowed(s),
        None => Cow::Owned(format!("{h}시")),
    }
}

/// UTC 시각을 KST ISO8601 문자열로 변환. 응답의 `updatedAt` 에 사용.
pub fn to_kst_iso<Tz: TimeZone>(dt: &DateTime<Tz>) -> String {
    let kst = dt.with_timezone(&KST);
    kst.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// UTC 시각을 KST 의 NaiveDate 로 변환. (Mongo `date` 가 UTC 자정으로 저장되어
/// 있는 경우) KST 로 보면 다음날이 될 수 있다는 점에 주의.
pub fn utc_midnight_to_kst_date(utc: DateTime<Utc>) -> NaiveDate {
    utc.with_timezone(&KST).date_naive()
}

/// KST 자정 (00:00:00 KST) 을 UTC 로 변환. Mongo 에 저장되는 UTC 자정과 동일.
pub fn kst_midnight_to_utc(date: NaiveDate) -> DateTime<Utc> {
    kst_date_at_start(date).with_timezone(&Utc)
}

/// KST 자정 + 시간 → UTC. KST 의 "M시" 를 UTC 로.
pub fn kst_date_at_hour_to_utc(date: NaiveDate, hour: u32) -> DateTime<Utc> {
    KST.from_local_datetime(&date.and_hms_opt(hour, 0, 0).unwrap())
        .unwrap()
        .with_timezone(&Utc)
}

/// 임의의 NaiveDateTime 을 UTC 로 가정하고 KST NaiveDate 로 변환.
pub fn naive_utc_to_kst_date(dt: NaiveDateTime) -> NaiveDate {
    Utc.from_utc_datetime(&dt).with_timezone(&KST).date_naive()
}

/// 시간 주입 시임. 프로덕션은 `RealClock`, 테스트는 `FixedClock` 사용.
pub trait Clock: Send + Sync + 'static {
    fn now_utc(&self) -> DateTime<Utc>;
    fn now_kst(&self) -> DateTime<FixedOffset> {
        self.now_utc().with_timezone(&KST)
    }
    fn today_kst_date(&self) -> NaiveDate {
        self.now_kst().date_naive()
    }
}

#[derive(Debug, Clone)]
pub struct RealClock;

impl Clock for RealClock {
    fn now_utc(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Clone)]
pub struct FixedClock {
    pub utc: DateTime<Utc>,
}

impl Clock for FixedClock {
    fn now_utc(&self) -> DateTime<Utc> {
        self.utc
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn date_label_format() {
        let s = format_date_label(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(s, "2024-01-01(월)");
    }

    #[test]
    fn hour_formatting() {
        assert_eq!(format_hour(0), "오전 12시");
        assert_eq!(format_hour(1), "오전 1시");
        assert_eq!(format_hour(11), "오전 11시");
        assert_eq!(format_hour(12), "오후 12시");
        assert_eq!(format_hour(15), "오후 3시");
        assert_eq!(format_hour(23), "오후 11시");
        assert_eq!(format_hour(24), "오전 12시");
    }

    #[test]
    fn kst_offset_is_correct() {
        let utc = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let kst = utc.with_timezone(&KST);
        assert_eq!(kst.to_rfc3339(), "2024-01-01T09:00:00+09:00");
    }

    #[test]
    fn kst_midnight_to_utc_midnight() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let utc = kst_midnight_to_utc(d);
        // 2024-01-01 00:00 KST = 2023-12-31 15:00 UTC
        assert_eq!(utc, Utc.with_ymd_and_hms(2023, 12, 31, 15, 0, 0).unwrap());
    }

    #[test]
    fn fixed_clock_returns_fixed_time() {
        let fixed = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
        let clock = FixedClock { utc: fixed };
        assert_eq!(clock.now_utc(), fixed);
    }

    #[test]
    fn fixed_clock_independent_of_system_time() {
        let fixed = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let clock = FixedClock { utc: fixed };
        assert_eq!(clock.now_utc(), fixed);
        assert_ne!(clock.now_utc(), Utc::now());
    }
}
