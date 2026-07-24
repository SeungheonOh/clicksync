package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

type fakeDocker struct {
	usage       diskUsage
	usageErr    error
	summaries   []containerSummary
	listErr     error
	containers  map[string]container
	inspectErr  error
	stopIDs     []string
	stopTimeout []time.Duration
	stopErr     error
}

type memoryEvents struct {
	values []event
	err    error
}

type checkingNotifier struct {
	events *memoryEvents
	err    error
	calls  int
}

func (fake *fakeDocker) DiskUsage(context.Context) (diskUsage, error) {
	return fake.usage, fake.usageErr
}

func (fake *fakeDocker) IngestionContainers(
	context.Context,
) ([]containerSummary, error) {
	return fake.summaries, fake.listErr
}

func (fake *fakeDocker) Inspect(
	_ context.Context,
	id string,
) (container, error) {
	if fake.inspectErr != nil {
		return container{}, fake.inspectErr
	}
	value, found := fake.containers[id]
	if !found {
		return container{}, errContainerNotFound
	}
	return value, nil
}

func (fake *fakeDocker) Stop(
	_ context.Context,
	id string,
	timeout time.Duration,
) (bool, error) {
	fake.stopIDs = append(fake.stopIDs, id)
	fake.stopTimeout = append(fake.stopTimeout, timeout)
	if fake.stopErr != nil {
		return false, fake.stopErr
	}
	value := fake.containers[id]
	value.State.Running = false
	value.State.Status = "exited"
	fake.containers[id] = value
	return false, nil
}

func (memory *memoryEvents) Append(value event) error {
	if memory.err != nil {
		return memory.err
	}
	memory.values = append(memory.values, value)
	return nil
}

func (notifier *checkingNotifier) Notify(
	context.Context,
	event,
) error {
	notifier.calls++
	if len(notifier.events.values) == 0 {
		return errors.New("notification preceded durable append")
	}
	return notifier.err
}

func TestOwnedFootprintUsesOnlyExactLabelsAndDockerBytes(t *testing.T) {
	usage := diskUsage{
		Volumes: []volumeUsage{
			testVolume("data", ownershipLabelValue, 90),
			testVolume("other", "another-project", 9_000),
		},
		Containers: []containerUsage{
			{
				ID:     "ingestion",
				Names:  []string{"/clicksync"},
				Labels: map[string]string{ownershipLabelKey: ownershipLabelValue},
				SizeRW: 7,
			},
			{
				ID:     "unrelated",
				Names:  []string{"/unrelated"},
				Labels: map[string]string{ownershipLabelKey: "unrelated"},
				SizeRW: 8_000,
			},
		},
	}
	got, err := calculateOwnedFootprint(usage)
	if err != nil {
		t.Fatal(err)
	}
	if got.Bytes != 97 || len(got.Objects) != 2 {
		t.Fatalf("owned footprint = %+v, want 97 bytes across two objects", got)
	}
	if got.Objects[0].Type != "container_writable_layer" ||
		got.Objects[1].Type != "volume" {
		t.Fatalf("owned objects are not deterministically ordered: %+v", got.Objects)
	}
}

func TestOwnedFootprintRejectsMissingNegativeAndOverflowSizes(t *testing.T) {
	missing := testVolume("missing", ownershipLabelValue, 1)
	missing.UsageData = nil
	for name, usage := range map[string]diskUsage{
		"missing": {Volumes: []volumeUsage{missing}},
		"negative volume": {
			Volumes: []volumeUsage{
				testVolume("negative", ownershipLabelValue, -1),
			},
		},
		"negative container": {
			Containers: []containerUsage{{
				ID:     "negative",
				Labels: map[string]string{ownershipLabelKey: ownershipLabelValue},
				SizeRW: -1,
			}},
		},
		"overflow": {
			Volumes: []volumeUsage{
				testVolume("large", ownershipLabelValue, int64(^uint64(0)>>1)),
				testVolume("one", ownershipLabelValue, 1),
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := calculateOwnedFootprint(usage); err == nil {
				t.Fatal("invalid Docker size was accepted")
			}
		})
	}
}

func TestExactCapacityStopsOnlyRevalidatedIngestionOnce(t *testing.T) {
	fake := healthyFakeDocker(capacityLimitBytes)
	events := &memoryEvents{}
	var saved watchdogState
	watch := &watchdog{
		docker:              fake,
		events:              events,
		measurementInterval: time.Hour,
		now:                 fixedClock(),
		saveState: func(state watchdogState) error {
			saved = state
			return nil
		},
	}
	if err := watch.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fake.stopIDs, []string{"ingestion-id"}) ||
		!slices.Equal(fake.stopTimeout, []time.Duration{stopTimeout}) {
		t.Fatalf("stop calls ids=%v timeouts=%v", fake.stopIDs, fake.stopTimeout)
	}
	if slices.Contains(fake.stopIDs, "database-id") {
		t.Fatal("watchdog attempted to stop ClickHouse")
	}
	if !saved.CapacityReached || saved.LastCapacityStopID != "ingestion-id" {
		t.Fatalf("capacity latch was not persisted: %+v", saved)
	}
	if !hasEvent(events.values, "capacity_limit_reached") ||
		!hasEvent(events.values, "capacity_ingestion_stopped") {
		t.Fatalf("capacity events missing: %+v", events.values)
	}
	if err := watch.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fake.stopIDs) != 1 {
		t.Fatalf("stopped ingestion %d times", len(fake.stopIDs))
	}

	restartedEvents := &memoryEvents{}
	restarted := &watchdog{
		docker:              fake,
		events:              restartedEvents,
		measurementInterval: time.Hour,
		now:                 fixedClock(),
	}
	restarted.restore(saved)
	if err := restarted.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fake.stopIDs) != 1 || hasEvent(restartedEvents.values, "capacity_limit_reached") {
		t.Fatalf(
			"restart repeated capacity action: stops=%v events=%+v",
			fake.stopIDs,
			restartedEvents.values,
		)
	}
}

func TestCapacityNeverStopsAmbiguousWrongOrChangedIdentity(t *testing.T) {
	for name, mutate := range map[string]func(*fakeDocker){
		"ambiguous": func(fake *fakeDocker) {
			fake.summaries = append(fake.summaries, containerSummary{ID: "second"})
		},
		"wrong labels": func(fake *fakeDocker) {
			value := fake.containers["ingestion-id"]
			value.Labels[kindLabelKey] = "database"
			fake.containers["ingestion-id"] = value
		},
		"identity changed on reinspection": func(fake *fakeDocker) {
			value := fake.containers["ingestion-id"]
			value.ID = "replacement-id"
			fake.containers["ingestion-id"] = value
		},
	} {
		t.Run(name, func(t *testing.T) {
			fake := healthyFakeDocker(capacityLimitBytes)
			mutate(fake)
			events := &memoryEvents{}
			watch := &watchdog{
				docker:              fake,
				events:              events,
				measurementInterval: time.Hour,
				now:                 fixedClock(),
			}
			if err := watch.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(fake.stopIDs) != 0 {
				t.Fatalf("unsafe stop calls: %v", fake.stopIDs)
			}
		})
	}
}

func TestExitAndDockerUnhealthyAlertsAreTransitionDeduplicated(t *testing.T) {
	for name, mutate := range map[string]func(*container){
		"exited": func(value *container) {
			value.State.Running = false
			value.State.Status = "exited"
			value.State.ExitCode = 1
		},
		"unhealthy": func(value *container) {
			value.State.Health = &containerHealth{
				Status:        "unhealthy",
				FailingStreak: 3,
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			fake := healthyFakeDocker(1)
			value := fake.containers["ingestion-id"]
			mutate(&value)
			fake.containers["ingestion-id"] = value
			events := &memoryEvents{}
			var saved watchdogState
			watch := &watchdog{
				docker:              fake,
				events:              events,
				measurementInterval: time.Hour,
				now:                 fixedClock(),
				saveState: func(state watchdogState) error {
					saved = state
					return nil
				},
			}
			if err := watch.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			firstCount := len(events.values)
			if err := watch.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(events.values) != firstCount {
				t.Fatalf("unchanged state emitted duplicate events: %+v", events.values)
			}
			restarted := &watchdog{
				docker:              fake,
				events:              &memoryEvents{},
				measurementInterval: time.Hour,
				now:                 fixedClock(),
			}
			restarted.restore(saved)
			if err := restarted.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(restarted.events.(*memoryEvents).values) != 0 {
				t.Fatal("persisted transition was re-alerted after restart")
			}
		})
	}
}

func TestDiskAPIFailureAlertsWithoutStopping(t *testing.T) {
	fake := healthyFakeDocker(1)
	fake.usageErr = errors.New("df unavailable")
	events := &memoryEvents{}
	watch := &watchdog{
		docker:              fake,
		events:              events,
		measurementInterval: time.Hour,
		now:                 fixedClock(),
	}
	if err := watch.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fake.stopIDs) != 0 || !hasEvent(events.values, "footprint_measurement_failed") {
		t.Fatalf("disk API failure result stops=%v events=%+v", fake.stopIDs, events.values)
	}
	if err := watch.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if countEvents(events.values, "footprint_measurement_failed") != 1 {
		t.Fatal("unchanged disk API failure was not deduplicated")
	}
}

func TestWebhookRunsAfterDurableAlertAndFailureIsLogged(t *testing.T) {
	events := &memoryEvents{}
	notifier := &checkingNotifier{
		events: events,
		err:    errors.New("webhook unavailable"),
	}
	watch := &watchdog{
		events:   events,
		notifier: notifier,
		now:      fixedClock(),
	}
	if err := watch.emit(context.Background(), event{
		Severity: "alert",
		Kind:     "proof_alert",
		Message:  "proof",
	}, true); err != nil {
		t.Fatal(err)
	}
	if notifier.calls != 1 ||
		!hasEvent(events.values, "proof_alert") ||
		!hasEvent(events.values, "webhook_delivery_failed") {
		t.Fatalf(
			"webhook durability proof calls=%d events=%+v",
			notifier.calls,
			events.values,
		)
	}
}

func TestDockerHTTPClientUsesConfiguredEndpoint(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		paths = append(paths, request.Method+" "+request.URL.RequestURI())
		switch {
		case request.URL.Path == "/containers/json":
			json.NewEncoder(writer).Encode([]containerSummary{{ID: "ingestion-id"}})
		case request.URL.Path == "/system/df":
			json.NewEncoder(writer).Encode(diskUsage{})
		case request.URL.Path == "/containers/ingestion-id/json":
			json.NewEncoder(writer).Encode(testIngestionContainer())
		case request.URL.Path == "/containers/ingestion-id/stop":
			writer.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client := newDockerHTTPClient(server.URL)
	ctx := context.Background()
	if _, err := client.IngestionContainers(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DiskUsage(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Inspect(ctx, "ingestion-id"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Stop(ctx, "ingestion-id", stopTimeout); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(paths, "\n")
	for _, expected := range []string{
		"GET /containers/json?",
		"GET /system/df?type=volume&type=container",
		"GET /containers/ingestion-id/json",
		"POST /containers/ingestion-id/stop?t=60",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("configured HTTP endpoint did not receive %q:\n%s", expected, joined)
		}
	}
}

func TestDurableLogAndStateUsePrivateFiles(t *testing.T) {
	dir := t.TempDir()
	log, err := openDurableEventLog(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Append(event{Kind: "proof", Severity: "info"}); err != nil {
		t.Fatal(err)
	}
	if err := log.file.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("event log mode = %o", info.Mode().Perm())
	}
	state := watchdogState{Version: 1, CapacityReached: true}
	if err := saveWatchdogState(dir, state); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadWatchdogState(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if loaded != state {
		t.Fatalf("loaded state = %+v, want %+v", loaded, state)
	}
}

func healthyFakeDocker(bytes int64) *fakeDocker {
	value := testIngestionContainer()
	return &fakeDocker{
		usage: diskUsage{
			Volumes: []volumeUsage{
				testVolume("clicksync-data", ownershipLabelValue, bytes),
				testVolume("unrelated", "unrelated", capacityLimitBytes),
			},
			Containers: []containerUsage{
				{
					ID: "database-id",
					Names: []string{
						"/clicksync-clickhouse",
					},
					Labels: map[string]string{
						ownershipLabelKey: ownershipLabelValue,
						kindLabelKey:      "database",
					},
				},
			},
		},
		summaries: []containerSummary{{ID: value.ID}},
		containers: map[string]container{
			value.ID: value,
		},
	}
}

func testIngestionContainer() container {
	return container{
		ID:   "ingestion-id",
		Name: "/" + targetContainerName,
		Labels: map[string]string{
			ownershipLabelKey: ownershipLabelValue,
			kindLabelKey:      ingestionKind,
		},
		State: containerState{
			Status:  "running",
			Running: true,
		},
	}
}

func testVolume(name string, scope string, bytes int64) volumeUsage {
	value := volumeUsage{
		Name: name,
		Labels: map[string]string{
			ownershipLabelKey: scope,
		},
	}
	value.UsageData = &struct {
		Size int64 `json:"Size"`
	}{Size: bytes}
	return value
}

func fixedClock() func() time.Time {
	return func() time.Time {
		return time.Date(2026, 7, 24, 8, 0, 0, 0, time.UTC)
	}
}

func hasEvent(values []event, kind string) bool {
	return countEvents(values, kind) != 0
}

func countEvents(values []event, kind string) int {
	count := 0
	for _, value := range values {
		if value.Kind == kind {
			count++
		}
	}
	return count
}
