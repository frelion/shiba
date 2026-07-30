FROM postgres:17-bookworm AS builder

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        curl \
        libssl-dev \
        pkg-config \
        postgresql-server-dev-17 \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 --silent --show-error --fail \
      https://sh.rustup.rs \
      | sh -s -- --profile minimal --default-toolchain stable --no-modify-path

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /src/shiba
COPY Cargo.toml Cargo.lock build.rs shiba.control ./
COPY sql ./sql
COPY src ./src

RUN cargo install cargo-pgrx --version 0.19.1 --locked \
    && cargo pgrx init --pg17 /usr/lib/postgresql/17/bin/pg_config \
    && cargo pgrx install --pg-config /usr/lib/postgresql/17/bin/pg_config

FROM postgres:17-bookworm

COPY --from=builder /usr/lib/postgresql/17/lib/shiba.so /usr/lib/postgresql/17/lib/shiba.so
COPY --from=builder /usr/share/postgresql/17/extension/shiba* /usr/share/postgresql/17/extension/
