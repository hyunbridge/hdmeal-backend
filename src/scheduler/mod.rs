//! 주기 작업 러너.
//!
//! [`PeriodicTask::start`] 가 tokio task 에서 `interval` 마다 `tick_fn` 을 호출.
//! `tick_fn` 이 panic 해도 다음 tick 에서 재시도. 종료 시 `stop` 으로 취소.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use std::sync::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub struct PeriodicTask {
    interval: Duration,
    state: Arc<Mutex<State>>,
    notify_stop: Arc<Notify>,
    stop_requested: Arc<AtomicBool>,
}

struct State {
    handle: Option<JoinHandle<()>>,
}

impl PeriodicTask {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            state: Arc::new(Mutex::new(State { handle: None })),
            notify_stop: Arc::new(Notify::new()),
            stop_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 작업 시작. 이미 시작되어 있으면 기존 인스턴스를 그대로 둡니다.
    pub fn start<F, Fut>(&self, mut tick_fn: F)
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut state = self.state.lock().unwrap();
        if state.handle.is_some() {
            return;
        }
        self.stop_requested.store(false, Ordering::SeqCst);
        let interval = self.interval;
        let notify = self.notify_stop.clone();
        let stop_requested = self.stop_requested.clone();
        let handle = tokio::spawn(async move {
            if stop_requested.load(Ordering::SeqCst) {
                return;
            }
            // 시작 시점에 즉시 1회 실행.
            if let Err(e) = AssertUnwindSafe(tick_fn()).catch_unwind().await {
                tracing::error!(error = ?e, "periodic task initial tick panicked");
            }
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if stop_requested.load(Ordering::SeqCst) {
                    break;
                }
                tokio::select! {
                    _ = notify.notified() => break,
                    _ = ticker.tick() => {
                        if stop_requested.load(Ordering::SeqCst) {
                            break;
                        }
                        if let Err(e) = AssertUnwindSafe(tick_fn()).catch_unwind().await {
                            tracing::error!(error = ?e, "periodic task tick panicked");
                        }
                    }
                }
            }
        });
        state.handle = Some(handle);
    }

    /// 작업 취소. future 가 즉시 반환되지만, 현재 진행 중인 tick 은 완료될 때까지
    /// 대기합니다.
    pub async fn stop(&self) {
        self.stop_requested.store(true, Ordering::SeqCst);
        let handle = {
            let mut state = self.state.lock().unwrap();
            self.notify_stop.notify_waiters();
            state.handle.take()
        };
        if let Some(h) = handle {
            let _ = h.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn periodic_task_fires_periodically() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter2 = counter.clone();
        let task = PeriodicTask::new(Duration::from_millis(5));
        task.start(move || {
            let c = counter2.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });
        tokio::time::sleep(Duration::from_millis(35)).await;
        task.stop().await;
        let n = counter.load(Ordering::SeqCst);
        assert!(n >= 1, "expected at least 1 tick, got {n}");
    }
}
