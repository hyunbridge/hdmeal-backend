//! IngestionService: NEIS 데이터 동기화.
//!
//! 두 가지 dedup 메커니즘:
//!
//! 1. **Cooldown** — 동일 range key 가 [`RANGE_COOLDOWN`] 이내에 성공한 경우
//!    즉시 `Fresh` 반환. 10분 TTL.
//! 2. **Inflight** — 동일 range key 에 대해 fetch 가 진행 중인 경우
//!    `Inflight` 반환. background 또는 short sync 가 끝나면 cooldown 에 진입.
//!
//! 호출자 시맨틱:
//!
//! - [`IngestionService::ensure_range`] (non-blocking) — HTTP 핸들러용.
//!   결과를 기다리지 않고, 필요시 background sync 를 spawn. 호출자는 즉시
//!   DB read 해도 된다. 빈 결과라면 짧은 안내 헤더를 붙이는 정도가 적절.
//! - [`IngestionService::try_sync_range_short`] (blocking, 짧음) — 챗봇용.
//!   background sync 를 spawn 한 뒤 cooldown 진입을 짧게 polling. `true` 면
//!   재조회 가능, `false` 면 background 진행 중.
//! - [`IngestionService::sync_range`] (blocking) — startup warmup / periodic
//!   sync 용. 직접 fetch+persist+cooldown 마킹. inflight 중이면 짧게 대기.
//!

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, NaiveDate, Utc};

use crate::infrastructure::neis::neis::NeisClient;
use crate::repository::DataService;

const RANGE_COOLDOWN: Duration = Duration::from_secs(10 * 60);
const RECENT_SYNC_TTL: Duration = Duration::from_secs(60 * 60);
const WEATHER_BACKGROUND_TIMEOUT: Duration = Duration::from_secs(5 * 60);

const SYNC_TIMEOUT_SHORT: Duration = Duration::from_millis(1500);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStatus {
    Cooldown,
    Synced,
    Skipped,
    Background,
}

/// [`IngestionService::ensure_range`] 가 반환하는 range 의 신선도 상태.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeStatus {
    /// cooldown 안 — 이미 신선함. 별도 fetch 불필요.
    Fresh,
    /// 다른 요청 (또는 background) 가 이 range 에 대해 sync 중. 곧 신선해짐.
    Inflight,
    /// 방금 background sync 를 시작함. 호출자는 즉시 DB read 해도 되지만
    /// 첫 호출은 비어있을 수 있다.
    Spawned,
}

impl RangeStatus {
    /// `X-HDMeal-Sync` 응답 헤더 값.
    pub fn sync_header_label(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Inflight | Self::Spawned => "pending",
        }
    }
}

/// range key 단위 cooldown + inflight dedup 상태.
///
/// `std::sync::Mutex` 가 안전한 이유: 락은 HashMap 조작에만 잠시 유지되고
/// `.await` 포인트에서는 절대 잡고 있지 않다.
#[derive(Clone)]
struct RangeDedup {
    cooldown: Arc<Mutex<HashMap<String, Instant>>>,
    inflight: Arc<Mutex<HashMap<String, Instant>>>,
}

impl RangeDedup {
    fn new() -> Self {
        Self {
            cooldown: Arc::new(Mutex::new(HashMap::new())),
            inflight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn check_cooldown(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut map = self.cooldown.lock().unwrap();
        if let Some(last) = map.get(key) {
            if now.saturating_duration_since(*last) < RANGE_COOLDOWN {
                return true;
            }
        }
        prune_locked(now, &mut map);
        false
    }

    fn mark_cooldown(&self, key: &str) {
        let now = Instant::now();
        let mut map = self.cooldown.lock().unwrap();
        map.insert(key.to_string(), now);
    }

    fn check_inflight(&self, key: &str) -> bool {
        let map = self.inflight.lock().unwrap();
        map.contains_key(key)
    }

    /// inflight 등록. 이미 등록되어 있으면 `false` (다른 요청이 진행 중).
    fn mark_inflight(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut map = self.inflight.lock().unwrap();
        if map.contains_key(key) {
            return false;
        }
        map.insert(key.to_string(), now);
        true
    }

    fn clear_inflight(&self, key: &str) {
        let mut map = self.inflight.lock().unwrap();
        map.remove(key);
    }
}

pub struct IngestionService {
    data: Arc<DataService>,
    neis: Arc<NeisClient>,
    dedup: RangeDedup,
}

impl IngestionService {
    pub fn new(data: Arc<DataService>, neis: Arc<NeisClient>) -> Self {
        Self {
            data,
            neis,
            dedup: RangeDedup::new(),
        }
    }

    fn key_range(start: NaiveDate, end: NaiveDate) -> String {
        format!(
            "range:{}:{}",
            start.format("%Y-%m-%d"),
            end.format("%Y-%m-%d")
        )
    }

    async fn persist_fetched(
        data: &DataService,
        fetched: &crate::infrastructure::neis::neis::NeisFetchAll,
    ) -> Result<(), String> {
        // 3 컬렉션을 병렬 upsert. Mongo connection pool 이 충분히 크므로
        // round-trip latency 가 직렬 → 3-way overlap 으로 줄어듦.
        let (meals_res, schedules_res, timetables_res) = tokio::join!(
            data.upsert_meals_batch(&fetched.meals),
            data.upsert_schedules_batch(&fetched.schedules),
            data.upsert_timetables_batch(&fetched.timetables),
        );
        let mut errors = Vec::new();
        if let Err(e) = meals_res {
            tracing::warn!(error = %e, count = fetched.meals.len(), "upsert_meals_batch failed");
            errors.push(("meals", e.to_string()));
        }
        if let Err(e) = schedules_res {
            tracing::warn!(
                error = %e,
                count = fetched.schedules.len(),
                "upsert_schedules_batch failed"
            );
            errors.push(("schedules", e.to_string()));
        }
        if let Err(e) = timetables_res {
            tracing::warn!(
                error = %e,
                count = fetched.timetables.len(),
                "upsert_timetables_batch failed"
            );
            errors.push(("timetables", e.to_string()));
        }
        persist_error_message(&errors).map_or(Ok(()), Err)
    }

    /// blocking: cooldown / inflight 안이면 즉시 단축 반환, 아니면 직접 fetch.
    ///
    /// startup warmup / periodic sync 전용. 60초 timeout 은 호출자가 `tokio::time::timeout` 으로
    /// 별도 감싸서 적용 ([`crate::app::run`] 참고).
    pub async fn sync_range(&self, start: NaiveDate, end: NaiveDate) -> Result<SyncStatus, String> {
        let key = Self::key_range(start, end);
        if self.dedup.check_cooldown(&key) {
            return Ok(SyncStatus::Cooldown);
        }
        if self.dedup.check_inflight(&key) {
            // 다른 요청이 이 range 에 대해 sync 중. 짧게 cooldown 진입을 기다림.
            if self.wait_for_cooldown(&key, SYNC_TIMEOUT_SHORT).await {
                return Ok(SyncStatus::Cooldown);
            }
            return Err("sync_range: timed out waiting for inflight sync".to_string());
        }
        self.do_range_sync(start, end)
            .await
            .map(|_| SyncStatus::Synced)
    }

    /// inflight 등록 후 fetch+persist+cooldown. 등록 실패 시 즉시 단축.
    async fn do_range_sync(&self, start: NaiveDate, end: NaiveDate) -> Result<(), String> {
        let key = Self::key_range(start, end);
        if !self.dedup.mark_inflight(&key) {
            return Err("inflight already registered".to_string());
        }
        let data = self.data.clone();
        let neis = self.neis.clone();
        let res = async {
            let fetched = neis
                .fetch_all(start, end)
                .await
                .map_err(|e| e.to_string())?;
            Self::persist_fetched(&data, &fetched).await
        }
        .await;
        self.dedup.clear_inflight(&key);
        if res.is_ok() {
            self.dedup.mark_cooldown(&key);
        }
        res
    }

    /// non-blocking. cooldown / inflight 안이면 즉시 `Fresh` / `Inflight` 반환.
    /// 그 외에는 background 에서 sync 를 시작하고 `Spawned` 반환.
    ///
    /// 호출자는 즉시 DB read 해도 된다. cold start 라 DB 가 비어있어도
    /// background 가 데이터를 채우면 다음 호출에서 cooldown 안으로 들어온다.
    pub fn ensure_range(&self, start: NaiveDate, end: NaiveDate) -> RangeStatus {
        let key = Self::key_range(start, end);
        if self.dedup.check_cooldown(&key) {
            return RangeStatus::Fresh;
        }
        if self.dedup.check_inflight(&key) {
            return RangeStatus::Inflight;
        }
        self.spawn_background_range_sync(start, end);
        RangeStatus::Spawned
    }

    /// 짧은 timeout (1.5s) 동안 sync 시도. 결과에 무관하게 background fallback 보장.
    ///
    /// 챗봇에서 "동기화 후 재조회" 가 필요한 호출자용.
    /// - `true` = cooldown 진입 (재조회 가능)
    /// - `false` = background 진행 중 (DB read 해도 비어있을 수 있음)
    pub async fn try_sync_range_short(&self, start: NaiveDate, end: NaiveDate) -> bool {
        let key = Self::key_range(start, end);
        if self.dedup.check_cooldown(&key) {
            return true;
        }
        if !self.dedup.check_inflight(&key) {
            // dedup 된 background sync 시작. 이미 누가 진행 중이면 그대로 두기.
            self.spawn_background_range_sync(start, end);
        }
        self.wait_for_cooldown(&key, SYNC_TIMEOUT_SHORT).await
    }

    async fn wait_for_cooldown(&self, key: &str, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if self.dedup.check_cooldown(key) {
                return true;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        self.dedup.check_cooldown(key)
    }

    /// background 에서 range sync 를 시작한다. cooldown / inflight dedup 적용.
    /// - cooldown 안: 빈 JoinHandle 즉시 반환 (별도 fetch 안 함)
    /// - inflight 중: 빈 JoinHandle 반환 (이미 누가 진행 중)
    /// - 그 외: tokio task 로 fetch+persist. 성공 시 cooldown 마킹.
    pub fn spawn_background_range_sync(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> tokio::task::JoinHandle<()> {
        let key = Self::key_range(start, end);
        if self.dedup.check_cooldown(&key) {
            return tokio::spawn(async {});
        }
        if !self.dedup.mark_inflight(&key) {
            return tokio::spawn(async {});
        }
        let data = self.data.clone();
        let neis = self.neis.clone();
        let dedup = self.dedup.clone();
        let key_str = key.clone();
        tokio::spawn(async move {
            let work = async {
                let fetched = neis
                    .fetch_all(start, end)
                    .await
                    .map_err(|e| e.to_string())?;
                Self::persist_fetched(&data, &fetched).await
            };
            let result = tokio::time::timeout(WEATHER_BACKGROUND_TIMEOUT, work).await;
            dedup.clear_inflight(&key_str);
            match result {
                Ok(Ok(())) => dedup.mark_cooldown(&key_str),
                Ok(Err(e)) => {
                    tracing::warn!(key = %key_str, error = %e, "background range sync failed")
                }
                Err(_) => {
                    tracing::warn!(key = %key_str, "background range sync timed out")
                }
            }
        })
    }

    /// 기본 윈도우: 어제 ~ +7 일.
    pub async fn sync_window(&self) -> Result<SyncStatus, String> {
        let today = crate::shared::timezone::today_kst_date();
        let start = today.pred_opt().unwrap_or(today);
        let end = today + chrono::Duration::days(7);
        self.sync_range(start, end).await
    }

    /// 오늘 ~ +N 일 구간을 동기화 대상으로 표시.
    pub async fn sync_window_offset(
        &self,
        past_days: i64,
        future_days: i64,
    ) -> Result<SyncStatus, String> {
        let today = crate::shared::timezone::today_kst_date();
        let start = today + chrono::Duration::days(-past_days);
        let end = today + chrono::Duration::days(future_days);
        self.sync_range(start, end).await
    }
}

fn prune_locked(now: Instant, map: &mut HashMap<String, Instant>) {
    map.retain(|_, t| now.saturating_duration_since(*t) < RECENT_SYNC_TTL);
}

fn persist_error_message(errors: &[(&'static str, String)]) -> Option<String> {
    if errors.is_empty() {
        return None;
    }
    Some(
        errors
            .iter()
            .map(|(label, error)| format!("{label}: {error}"))
            .collect::<Vec<_>>()
            .join("; "),
    )
}

pub const SYNC_TIMEOUT_SHORT_DURATION: Duration = SYNC_TIMEOUT_SHORT;
pub const SYNC_TIMEOUT_AUX_DURATION: Duration = Duration::from_secs(2);

/// 챗봇 캐시 freshness 헬퍼: `created_at` 기준 TTL 비교.
pub fn is_fresh(created_at: DateTime<Utc>, ttl: Duration) -> bool {
    let now = chrono::Utc::now();
    let age = now.signed_duration_since(created_at);
    age <= chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::seconds(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cooldown_returns_true_within_window() {
        assert!(is_fresh(
            chrono::Utc::now() - chrono::Duration::seconds(5),
            Duration::from_secs(60),
        ));
        assert!(!is_fresh(
            chrono::Utc::now() - chrono::Duration::seconds(120),
            Duration::from_secs(60),
        ));
    }

    #[test]
    fn persist_error_message_includes_all_failed_collections() {
        let msg = persist_error_message(&[
            ("meals", "duplicate key".to_string()),
            ("timetables", "network".to_string()),
        ])
        .unwrap();

        assert_eq!(msg, "meals: duplicate key; timetables: network");
    }

    #[test]
    fn range_status_sync_header_labels() {
        assert_eq!(RangeStatus::Fresh.sync_header_label(), "fresh");
        assert_eq!(RangeStatus::Inflight.sync_header_label(), "pending");
        assert_eq!(RangeStatus::Spawned.sync_header_label(), "pending");
    }

    #[test]
    fn range_dedup_isolates_keys_and_tracks_inflight() {
        let dedup = RangeDedup::new();
        let k1 = "range:2025-01-01:2025-01-07";
        let k2 = "range:2025-02-01:2025-02-07";

        assert!(dedup.mark_inflight(k1));
        assert!(!dedup.mark_inflight(k1), "이미 inflight 인 key 는 false");
        assert!(dedup.mark_inflight(k2), "다른 key 는 별도 dedup");

        dedup.clear_inflight(k1);
        assert!(dedup.mark_inflight(k1));

        dedup.mark_cooldown(k1);
        assert!(dedup.check_cooldown(k1));
        assert!(!dedup.check_cooldown(k2));
    }
}
