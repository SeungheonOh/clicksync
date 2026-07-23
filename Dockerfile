FROM golang@sha256:8e02eb337d9e0ea459e041f1ee5eece41cbb61f1d83e7d883a3e2fb4862063fa AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
RUN --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w -buildid=" \
      -o /out/clicksync \
      ./cmd/clicksync
RUN install -d -o 65532 -g 65532 -m 0700 /out/clicksync-state \
    && touch /out/clicksync-state/.volume-owner \
    && chown 65532:65532 /out/clicksync-state/.volume-owner

FROM scratch
COPY --from=build /out/clicksync /clicksync
COPY --from=build --chown=65532:65532 /out/clicksync-state /var/lib/clicksync-state
USER 65532:65532
ENTRYPOINT ["/clicksync"]
CMD ["sync"]
