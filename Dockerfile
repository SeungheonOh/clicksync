FROM golang@sha256:8e02eb337d9e0ea459e041f1ee5eece41cbb61f1d83e7d883a3e2fb4862063fa AS build

ARG CLICKSYNC_BUILD_ID=development
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags="-s -w -X main.buildID=${CLICKSYNC_BUILD_ID}" \
    -o /out/clicksync \
    ./cmd/clicksync
RUN mkdir -p /out/state

FROM scratch

COPY --from=build /out/clicksync /clicksync
COPY --from=build --chown=65532:65532 /out/state /var/lib/clicksync

USER 65532:65532
ENV CLICKSYNC_LOCK_PATH=/var/lib/clicksync/writer.lock
ENTRYPOINT ["/clicksync"]
