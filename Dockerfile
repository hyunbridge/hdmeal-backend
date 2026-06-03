# ---- builder ----
FROM rust:1.83-slim-bookworm AS builder

WORKDIR /app

# 시스템 의존성 (reqwest/rustls 등 컴파일용)
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 종속성만 먼저 빌드 (캐시 활용)
COPY Cargo.toml Cargo.lock* ./
RUN mkdir -p src && echo "fn main(){}" > src/main.rs && echo "" > src/lib.rs \
    && cargo build --release --bin hdmeal-backend \
    && rm -rf src target/release/deps/hdmeal_backend* target/release/hdmeal-backend*

# 실제 소스 빌드
COPY src ./src
COPY data ./data
RUN cargo build --release --bin hdmeal-backend

# ---- runtime ----
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -u 10001 -M -s /usr/sbin/nologin hdmeal

WORKDIR /app
COPY --from=builder /app/target/release/hdmeal-backend /usr/local/bin/hdmeal-backend
COPY --from=builder /app/data /app/data

ENV DATA_DIR=/app/data \
    PORT=8000

USER 10001
EXPOSE 8000

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/hdmeal-backend"]
