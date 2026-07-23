# syntax=docker/dockerfile:1.7

FROM golang:1.25.8-alpine3.23@sha256:8e02eb337d9e0ea459e041f1ee5eece41cbb61f1d83e7d883a3e2fb4862063fa AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false \
    -ldflags='-s -w' -o /out/clicksync-p2p ./cmd/clicksync-p2p

FROM scratch
COPY --from=build /out/clicksync-p2p /clicksync-p2p
USER 65532:65532
ENTRYPOINT ["/clicksync-p2p"]
