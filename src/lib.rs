//! HDMeal 백엔드 (Rust + Warp) 라이브러리 진입점.
//!
//! [`app`] 모듈이 컴포지션 루트이며, [`app::run`] 가 HTTP 서버를 띄우는 단일 함수입니다.

pub mod app;
pub mod application;
pub mod config;
pub mod domain;
pub mod error;
pub mod infrastructure;
pub mod repository;
pub mod scheduler;
pub mod shared;
pub mod transport;

pub use config::AppConfig;
pub use error::{HDMealError, HDMealResult};
