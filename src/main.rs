use std::process::ExitCode;
use std::{
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use hdmeal_backend::app;

const HEALTHCHECK_FLAG: &str = "--healthcheck";
const HEALTHCHECK_PATH: &str = "/healthz";
const HEALTHCHECK_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_PORT: u16 = 8000;

#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args_os().any(|arg| arg == HEALTHCHECK_FLAG) {
        return match run_healthcheck() {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("healthcheck failed: {e:#}");
                ExitCode::from(1)
            }
        };
    }

    if let Err(e) = app::run().await {
        eprintln!("fatal: {e:?}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

fn run_healthcheck() -> anyhow::Result<()> {
    let port = match std::env::var("PORT") {
        Ok(value) => value
            .parse::<u16>()
            .map_err(|e| anyhow::anyhow!("invalid PORT value {value:?}: {e}"))?,
        Err(std::env::VarError::NotPresent) => DEFAULT_PORT,
        Err(e) => return Err(e.into()),
    };

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = TcpStream::connect_timeout(&addr, HEALTHCHECK_TIMEOUT)?;
    stream.set_read_timeout(Some(HEALTHCHECK_TIMEOUT))?;
    stream.set_write_timeout(Some(HEALTHCHECK_TIMEOUT))?;
    stream.write_all(
        format!("GET {HEALTHCHECK_PATH} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",)
            .as_bytes(),
    )?;

    let mut reader = BufReader::new(stream);
    let mut status_line = String::new();
    reader.read_line(&mut status_line)?;
    ensure_200_status(&status_line)?;

    Ok(())
}

fn ensure_200_status(status_line: &str) -> anyhow::Result<()> {
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("invalid healthcheck response: {status_line:?}"))?;

    if status != "200" {
        anyhow::bail!("unexpected healthcheck status: {status_line:?}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ensure_200_status;

    #[test]
    fn accepts_http_200_status_line() {
        ensure_200_status("HTTP/1.1 200 OK\r\n").unwrap();
    }

    #[test]
    fn rejects_non_200_status_line() {
        let err = ensure_200_status("HTTP/1.1 503 Service Unavailable\r\n").unwrap_err();
        assert!(err.to_string().contains("unexpected healthcheck status"));
    }

    #[test]
    fn rejects_malformed_status_line() {
        let err = ensure_200_status("OK\r\n").unwrap_err();
        assert!(err.to_string().contains("invalid healthcheck response"));
    }
}
