//! 컴포지션 루트: 모든 의존성을 와이어업하고 tokio 런타임에서 HTTP 서버를 띄움.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::signal;

use crate::application::AppContext;
use crate::config::AppConfig;
use crate::infrastructure::neis::auxiliary::{KmaClient, SeoulWaterClient};
use crate::infrastructure::neis::http_client::HttpClient;
use crate::infrastructure::neis::neis::NeisClient;
use crate::repository::DataService;
use crate::scheduler::PeriodicTask;
use crate::shared::metrics::Metrics;
use crate::shared::observability;
use crate::transport::http;

/// 모든 초기화 + 서버 시작.
pub async fn run() -> anyhow::Result<()> {
    // rustls crypto provider 설치 (reqwest `rustls-no-provider` feature 대응).
    // mongodb 는 provider 를 명시적으로 builder 에 넘기므로 의존하지 않지만,
    // reqwest / opentelemetry-otlp (gRPC) 등이 process-wide default provider 를 사용한다.
    // OTLP exporter 가 https endpoint 로 export 할 수 있으므로 observability::init 보다 먼저 호출.
    // idempotent: 이미 설치된 경우 Err 를 무시. ring 은 mongodb 의 `rustls-tls` 가 활성화하므로
    // 정확히 하나의 provider 만 활성화 → rustls 의 `get_default_or_install_from_crate_features` 와도 호환.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let config = AppConfig::from_env()?;
    observability::init(&config.app_name, config.otel_endpoint.as_deref())?;

    // Repository (MongoDB) — 전역 가변 static 대신 Arc 로 명시적 수명 관리
    let mongo_client = crate::repository::init_client(&config).await?;
    let data = Arc::new(DataService::new(mongo_client.clone(), &config).await?);
    tracing::info!(db = %config.mongodb_database, "MongoDB connected");

    // Infrastructure
    let http = HttpClient::new()?;
    let neis = Arc::new(NeisClient::new(config.clone(), http.clone()));
    let kma = Arc::new(KmaClient::new(config.clone(), http.clone()));
    let seoul_water = Arc::new(SeoulWaterClient::new(
        http.clone(),
        config.seoul_data_token.clone(),
    ));

    // AppContext
    let config = Arc::new(config);
    let ctx = Arc::new(AppContext::new(
        data.clone(),
        neis.clone(),
        kma,
        seoul_water,
        config.clone(),
    ));

    // 시작 시: today-1 ~ today+10 미리 sync.
    let warmup = tokio::time::timeout(
        Duration::from_secs(60),
        ctx.ingestion.sync_window_offset(1, 10),
    )
    .await;
    match warmup {
        Ok(Ok(status)) => tracing::debug!(?status, "initial warmup finished"),
        Ok(Err(e)) => tracing::warn!(error = %e, "initial warmup failed"),
        Err(_) => tracing::warn!("initial warmup timed out"),
    }

    // 3 시간 주기 sync_window
    let periodic = PeriodicTask::new(Duration::from_secs(3 * 60 * 60));
    let ingestion = ctx.ingestion.clone();
    periodic.start(move || {
        let i = ingestion.clone();
        async move {
            if let Err(e) = i.sync_window().await {
                tracing::warn!(error = %e, "periodic window sync failed");
            }
        }
    });

    // HTTP 서버
    let metrics = Arc::new(Metrics::new());
    let router = http::build_router(
        config.clone(),
        ctx.clone(),
        metrics.clone(),
        mongo_client.clone(),
    );
    let addr: SocketAddr = ([0, 0, 0, 0], config.port).into();
    tracing::info!(%addr, "HTTP server listening");

    let server_fut = warp::serve(router).run(addr);
    let shutdown = async {
        if let Err(e) = signal::ctrl_c().await {
            tracing::error!(error = %e, "ctrl_c failed");
        }
        tracing::info!("shutdown signal received");
    };
    tokio::select! {
        _ = server_fut => {},
        _ = shutdown => {
            periodic.stop().await;
            // MongoDB client 의 background cleanup 은 Arc 들이 drop 될 때
            // `Client::Drop` 에서 처리됨 (mongodb 공식 권장). `Client::shutdown`
            // 은 `self` 를 요구하므로 Arc<Client> 에서는 호출할 수 없음.
            // ctx 와 data 가 scope 끝에서 drop 되며 driver 가 background
            // task 를 정리함.
            drop(mongo_client);
            observability::shutdown();
        }
    }

    Ok(())
}
