ARG PG_MAJOR=17

FROM postgres:${PG_MAJOR}-bookworm AS builder

ARG PG_MAJOR

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        curl \
        libssl-dev \
        pkg-config \
        postgresql-server-dev-${PG_MAJOR} \
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
    && cargo pgrx init --pg${PG_MAJOR} /usr/lib/postgresql/${PG_MAJOR}/bin/pg_config \
    && cargo pgrx install \
         --pg-config /usr/lib/postgresql/${PG_MAJOR}/bin/pg_config \
         --no-default-features \
         --features pg${PG_MAJOR}

FROM postgres:${PG_MAJOR}-bookworm

ARG PG_MAJOR

COPY --from=builder /usr/lib/postgresql/${PG_MAJOR}/lib/shiba.so /usr/lib/postgresql/${PG_MAJOR}/lib/shiba.so
COPY --from=builder /usr/share/postgresql/${PG_MAJOR}/extension/shiba* /usr/share/postgresql/${PG_MAJOR}/extension/
