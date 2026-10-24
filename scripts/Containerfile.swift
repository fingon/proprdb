FROM docker.io/library/golang:1.27.1-bookworm AS go
FROM docker.io/library/swift:6.2

RUN apt-get update \
    && apt-get install -y --no-install-recommends libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=go /usr/local/go /usr/local/go
ENV PATH="/usr/local/go/bin:${PATH}"
