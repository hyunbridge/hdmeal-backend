use std::process::ExitCode;

use hdmeal_backend::app;

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(e) = app::run().await {
        eprintln!("fatal: {e:?}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}
