# Clicksync external watchdog

This is a separate host process, not part of the Clicksync ingestion binary.
It polls the local Docker Engine every 30 seconds and:

- appends container exit, missing, ambiguous-identity, and Docker-reported
  unhealthy transitions to a host-side JSONL log, calling `fsync` after every
  event;
- sums only `UsageData.Size` for Docker volumes and `SizeRw` for containers
  whose `io.clicksync.scope` label is exactly `clicksync`; and
- at `>= 100000000000` bytes (decimal 100 GB), stops only the one revalidated
  container labeled both `io.clicksync.scope=clicksync` and
  `io.clicksync.kind=ingestion`.

Images, shared image layers, build cache, bind mounts, and Docker objects with
any other or missing scope label are excluded. Container root filesystems are
not double-counted: only their writable-layer `SizeRw` is included. The
watchdog never sends a stop request to the ClickHouse container. Missing,
negative, or ambiguous Engine accounting causes a durable alert and no stop.

The current Compose ingestion service does not define a Docker healthcheck, so
the live deployment can produce exit/missing alerts but cannot enter Docker's
`unhealthy` state. The watcher observes and tests that state when a healthcheck
is configured; it does not claim an application-level health probe.

## Install as a persistent user service

Build with the repository's pinned, offline Go image, then install the binary
and unit outside the repository:

```sh
mkdir -p "$HOME/.local/libexec" "$HOME/.config/systemd/user"
docker run --rm --network none \
  --user "$(id -u):$(id -g)" -e GOCACHE=/tmp/gocache \
  -v "$PWD:/src:ro" -v "$HOME/.local/libexec:/out" \
  -w /src/ops/clicksync-watchdog \
  golang@sha256:8e02eb337d9e0ea459e041f1ee5eece41cbb61f1d83e7d883a3e2fb4862063fa \
  sh -c 'CGO_ENABLED=0 go build -mod=readonly -trimpath -ldflags="-s -w -buildid=" -o /out/clicksync-watchdog .'
install -m 0600 ops/clicksync-watchdog/clicksync-watchdog.service \
  "$HOME/.config/systemd/user/clicksync-watchdog.service"
systemctl --user daemon-reload
systemctl --user enable --now clicksync-watchdog.service
```

The durable files are private and non-versioned:

- events: `~/.local/state/clicksync-watchdog/events.jsonl`
- transition/capacity latch: `~/.local/state/clicksync-watchdog/state.json`
- singleton process lock: `~/.local/state/clicksync-watchdog/watchdog.lock`

Inspect them with:

```sh
systemctl --user status clicksync-watchdog.service
journalctl --user -u clicksync-watchdog.service
tail -n 20 "$HOME/.local/state/clicksync-watchdog/events.jsonl"
```

## Optional webhook

Create `~/.config/clicksync-watchdog/environment` with mode `0600`:

```text
CLICKSYNC_WATCHDOG_WEBHOOK_URL=https://alerts.example.invalid/clicksync
```

Alert events are written and synced locally before best-effort HTTP delivery.
A webhook is required for real-time notification outside this host. Without
one, the JSONL log is the only notification surface. In particular, this
watcher cannot wake, resume, or notify the current Codex turn by itself.

`CLICKSYNC_WATCHDOG_POLL_INTERVAL` may change the interval (minimum `5s`).
`CLICKSYNC_WATCHDOG_STATE_DIR` may relocate durable state. The default local
Engine socket is `/var/run/docker.sock`; `CLICKSYNC_WATCHDOG_DOCKER_SOCKET`
may select another Unix socket. `CLICKSYNC_WATCHDOG_DOCKER_ENDPOINT` exists
for isolated HTTP fake tests and should not be set by the live service.

## Isolated tests

Tests use injected fakes and `httptest`; they neither access the real Docker
socket nor stop a real container:

```sh
docker run --rm --network none -v "$PWD:/src:ro" \
  -w /src/ops/clicksync-watchdog \
  golang@sha256:8e02eb337d9e0ea459e041f1ee5eece41cbb61f1d83e7d883a3e2fb4862063fa \
  go test -mod=readonly ./...
```
