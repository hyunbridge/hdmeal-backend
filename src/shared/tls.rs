//! rustls provider 초기화 헬퍼.
//!
//! reqwest 의 `rustls-no-provider` feature 와 OTLP gRPC client 가 process-wide
//! default provider 를 기대하므로, 앱 시작 초기에 한 번 설치해 둡니다.

/// ring 기반 rustls provider 를 process-wide default 로 설치합니다.
///
/// 이미 설치된 경우는 무시합니다. 여러 진입점에서 호출해도 안전합니다.
pub fn install_rustls_ring_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
