package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	capacityLimitBytes  int64 = 100_000_000_000
	targetContainerName       = "clicksync"
	ownershipLabelKey         = "io.clicksync.scope"
	ownershipLabelValue       = "clicksync"
	kindLabelKey              = "io.clicksync.kind"
	ingestionKind             = "ingestion"
	stopTimeout               = 60 * time.Second
)

type eventSink interface {
	Append(event) error
}

type eventNotifier interface {
	Notify(context.Context, event) error
}

type event struct {
	Timestamp       string        `json:"timestamp"`
	Severity        string        `json:"severity"`
	Kind            string        `json:"kind"`
	Message         string        `json:"message"`
	OwnedBytes      *int64        `json:"owned_bytes,omitempty"`
	LimitBytes      int64         `json:"limit_bytes,omitempty"`
	ContainerID     string        `json:"container_id,omitempty"`
	ContainerState  string        `json:"container_state,omitempty"`
	ContainerHealth string        `json:"container_health,omitempty"`
	ExitCode        *int          `json:"exit_code,omitempty"`
	Objects         []ownedObject `json:"objects,omitempty"`
}

type ownedObject struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	Bytes int64  `json:"bytes"`
}

type footprint struct {
	Bytes   int64
	Objects []ownedObject
}

type watchdog struct {
	docker              dockerAPI
	events              eventSink
	notifier            eventNotifier
	now                 func() time.Time
	measurementInterval time.Duration
	saveState           func(watchdogState) error

	lastCondition      string
	lastMeasureError   bool
	capacityReached    bool
	lastCapacityStopID string
	lastMeasurement    time.Time
}

type watchdogState struct {
	Version            int       `json:"version"`
	LastCondition      string    `json:"last_condition"`
	LastMeasureError   bool      `json:"last_measure_error"`
	CapacityReached    bool      `json:"capacity_reached"`
	LastCapacityStopID string    `json:"last_capacity_stop_id"`
	LastMeasurement    time.Time `json:"last_measurement"`
}

func calculateOwnedFootprint(usage diskUsage) (footprint, error) {
	var result footprint
	add := func(object ownedObject) error {
		if object.Name == "" {
			return errors.New("owned Docker object has no name")
		}
		if object.Bytes < 0 {
			return fmt.Errorf(
				"owned Docker %s %q has unknown size %d",
				object.Type,
				object.Name,
				object.Bytes,
			)
		}
		if result.Bytes > math.MaxInt64-object.Bytes {
			return errors.New("owned Docker footprint overflows int64")
		}
		result.Bytes += object.Bytes
		result.Objects = append(result.Objects, object)
		return nil
	}
	for _, volume := range usage.Volumes {
		if volume.Labels[ownershipLabelKey] != ownershipLabelValue {
			continue
		}
		if volume.UsageData == nil {
			return footprint{}, fmt.Errorf(
				"owned Docker volume %q has no usage data",
				volume.Name,
			)
		}
		if err := add(ownedObject{
			Type:  "volume",
			Name:  volume.Name,
			Bytes: volume.UsageData.Size,
		}); err != nil {
			return footprint{}, err
		}
	}
	for _, value := range usage.Containers {
		if value.Labels[ownershipLabelKey] != ownershipLabelValue {
			continue
		}
		name := value.ID
		if len(value.Names) != 0 {
			name = strings.TrimPrefix(value.Names[0], "/")
		}
		if err := add(ownedObject{
			Type:  "container_writable_layer",
			Name:  name,
			Bytes: value.SizeRW,
		}); err != nil {
			return footprint{}, err
		}
	}
	sort.Slice(result.Objects, func(i, j int) bool {
		if result.Objects[i].Type != result.Objects[j].Type {
			return result.Objects[i].Type < result.Objects[j].Type
		}
		return result.Objects[i].Name < result.Objects[j].Name
	})
	return result, nil
}

func isIngestionTarget(value container) bool {
	return value.Name == "/"+targetContainerName &&
		value.Labels[ownershipLabelKey] == ownershipLabelValue &&
		value.Labels[kindLabelKey] == ingestionKind
}

func (watch *watchdog) Run(ctx context.Context, interval time.Duration) error {
	if err := watch.Step(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := watch.Step(ctx); err != nil {
				return err
			}
		}
	}
}

func (watch *watchdog) Step(ctx context.Context) error {
	summaries, listErr := watch.docker.IngestionContainers(ctx)
	var (
		target     container
		inspectErr error
	)
	switch {
	case listErr != nil:
		inspectErr = listErr
		if err := watch.transition(ctx, "list_error", event{
			Severity: "alert",
			Kind:     "container_list_failed",
			Message:  listErr.Error(),
		}, true); err != nil {
			return err
		}
	case len(summaries) == 0:
		inspectErr = errContainerNotFound
		if err := watch.observeContainer(ctx, container{}, inspectErr); err != nil {
			return err
		}
	case len(summaries) > 1:
		inspectErr = fmt.Errorf(
			"found %d containers with Clicksync ingestion ownership labels",
			len(summaries),
		)
		if err := watch.transition(ctx, "ambiguous_targets", event{
			Severity: "alert",
			Kind:     "container_target_ambiguous",
			Message:  inspectErr.Error() + "; refusing any stop action",
		}, true); err != nil {
			return err
		}
	default:
		target, inspectErr = watch.docker.Inspect(ctx, summaries[0].ID)
		if inspectErr != nil {
			if err := watch.transition(ctx, "inspect_error", event{
				Severity: "alert",
				Kind:     "container_inspect_failed",
				Message:  inspectErr.Error(),
			}, true); err != nil {
				return err
			}
			break
		}
		if err := watch.observeContainer(ctx, target, inspectErr); err != nil {
			return err
		}
	}

	usage, err := watch.docker.DiskUsage(ctx)
	if err != nil {
		if !watch.lastMeasureError {
			if emitErr := watch.emit(ctx, event{
				Severity: "alert",
				Kind:     "footprint_measurement_failed",
				Message:  err.Error(),
			}, true); emitErr != nil {
				return emitErr
			}
		}
		watch.lastMeasureError = true
		if err := watch.persist(); err != nil {
			return err
		}
		return nil
	}
	measured, err := calculateOwnedFootprint(usage)
	if err != nil {
		if !watch.lastMeasureError {
			if emitErr := watch.emit(ctx, event{
				Severity: "alert",
				Kind:     "footprint_measurement_failed",
				Message:  err.Error(),
			}, true); emitErr != nil {
				return emitErr
			}
		}
		watch.lastMeasureError = true
		if err := watch.persist(); err != nil {
			return err
		}
		return nil
	}
	if watch.lastMeasureError {
		if err := watch.emit(ctx, measurementEvent(
			"info",
			"footprint_measurement_recovered",
			"Docker footprint measurement recovered",
			measured,
		), false); err != nil {
			return err
		}
	}
	watch.lastMeasureError = false
	now := watch.clock()
	if watch.lastMeasurement.IsZero() ||
		now.Sub(watch.lastMeasurement) >= watch.measurementInterval {
		if err := watch.emit(ctx, measurementEvent(
			"info",
			"footprint_measured",
			"measured Clicksync-owned Docker storage",
			measured,
		), false); err != nil {
			return err
		}
		watch.lastMeasurement = now
		if err := watch.persist(); err != nil {
			return err
		}
	}

	if measured.Bytes < capacityLimitBytes {
		if watch.capacityReached {
			if err := watch.emit(ctx, measurementEvent(
				"info",
				"capacity_below_limit",
				"Clicksync-owned Docker storage is below the stop threshold",
				measured,
			), false); err != nil {
				return err
			}
		}
		watch.capacityReached = false
		watch.lastCapacityStopID = ""
		return watch.persist()
	}

	if !watch.capacityReached {
		if err := watch.emit(ctx, measurementEvent(
			"alert",
			"capacity_limit_reached",
			"Clicksync-owned Docker storage reached the 100 GB decimal stop threshold",
			measured,
		), true); err != nil {
			return err
		}
	}
	watch.capacityReached = true
	if err := watch.persist(); err != nil {
		return err
	}
	if inspectErr != nil || !isIngestionTarget(target) || !target.State.Running {
		return nil
	}

	fresh, err := watch.docker.Inspect(ctx, target.ID)
	if err != nil {
		return watch.emit(ctx, event{
			Severity:    "alert",
			Kind:        "capacity_stop_reinspect_failed",
			Message:     err.Error(),
			OwnedBytes:  int64Pointer(measured.Bytes),
			LimitBytes:  capacityLimitBytes,
			ContainerID: target.ID,
		}, true)
	}
	if fresh.ID != target.ID || !isIngestionTarget(fresh) {
		return watch.emit(ctx, event{
			Severity:    "alert",
			Kind:        "capacity_stop_identity_changed",
			Message:     "refusing to stop a container whose exact ingestion identity changed",
			OwnedBytes:  int64Pointer(measured.Bytes),
			LimitBytes:  capacityLimitBytes,
			ContainerID: target.ID,
		}, true)
	}
	if !fresh.State.Running {
		return nil
	}
	alreadyStopped, err := watch.docker.Stop(ctx, fresh.ID, stopTimeout)
	if err != nil {
		return watch.emit(ctx, event{
			Severity:       "alert",
			Kind:           "capacity_stop_failed",
			Message:        err.Error(),
			OwnedBytes:     int64Pointer(measured.Bytes),
			LimitBytes:     capacityLimitBytes,
			ContainerID:    fresh.ID,
			ContainerState: fresh.State.Status,
		}, true)
	}
	watch.lastCapacityStopID = fresh.ID
	if err := watch.persist(); err != nil {
		return err
	}
	message := "stopped only the Clicksync ingestion container"
	if alreadyStopped {
		message = "Clicksync ingestion container was already stopped"
	}
	return watch.emit(ctx, event{
		Severity:       "alert",
		Kind:           "capacity_ingestion_stopped",
		Message:        message,
		OwnedBytes:     int64Pointer(measured.Bytes),
		LimitBytes:     capacityLimitBytes,
		ContainerID:    fresh.ID,
		ContainerState: fresh.State.Status,
	}, true)
}

func (watch *watchdog) observeContainer(
	ctx context.Context,
	value container,
	inspectErr error,
) error {
	if errors.Is(inspectErr, errContainerNotFound) {
		return watch.transition(ctx, "missing", event{
			Severity: "alert",
			Kind:     "container_missing",
			Message:  "Clicksync ingestion container is absent",
		}, true)
	}
	if !isIngestionTarget(value) {
		return watch.transition(ctx, "identity_mismatch:"+value.ID, event{
			Severity:    "alert",
			Kind:        "container_identity_mismatch",
			Message:     "exact-name container lacks the required Clicksync ingestion labels",
			ContainerID: value.ID,
		}, true)
	}
	health := "not_configured"
	if value.State.Health != nil {
		health = value.State.Health.Status
	}
	base := event{
		ContainerID:     value.ID,
		ContainerState:  value.State.Status,
		ContainerHealth: health,
	}
	if !value.State.Running {
		base.Severity = "alert"
		base.Kind = "container_exited"
		base.Message = "Clicksync ingestion container is not running"
		base.ExitCode = intPointer(value.State.ExitCode)
		condition := "not_running:" + value.ID + ":" +
			value.State.Status + ":" + fmt.Sprint(value.State.ExitCode)
		if watch.lastCapacityStopID == value.ID {
			condition = "capacity_stopped:" + value.ID
			base.Kind = "container_stopped_by_capacity"
			base.Message = "Clicksync ingestion remains stopped after the capacity action"
		}
		return watch.transition(ctx, condition, base, true)
	}
	if value.State.Paused || value.State.Restarting || value.State.Dead {
		base.Severity = "alert"
		base.Kind = "container_not_ready"
		base.Message = "Clicksync ingestion container is paused, restarting, or dead"
		return watch.transition(
			ctx,
			"not_ready:"+value.ID+":"+value.State.Status,
			base,
			true,
		)
	}
	if health == "unhealthy" {
		base.Severity = "alert"
		base.Kind = "container_unhealthy"
		base.Message = "Clicksync ingestion container reports unhealthy"
		return watch.transition(ctx, "unhealthy:"+value.ID, base, true)
	}
	base.Severity = "info"
	base.Kind = "container_running"
	base.Message = "Clicksync ingestion container is running"
	if watch.lastCondition != "" &&
		!strings.HasPrefix(watch.lastCondition, "running:") {
		base.Kind = "container_recovered"
		base.Message = "Clicksync ingestion container recovered"
	}
	return watch.transition(
		ctx,
		"running:"+value.ID+":"+health,
		base,
		false,
	)
}

func (watch *watchdog) transition(
	ctx context.Context,
	condition string,
	value event,
	notify bool,
) error {
	if watch.lastCondition == condition {
		return nil
	}
	if err := watch.emit(ctx, value, notify); err != nil {
		return err
	}
	watch.lastCondition = condition
	return watch.persist()
}

func (watch *watchdog) emit(
	ctx context.Context,
	value event,
	notify bool,
) error {
	if value.Timestamp == "" {
		value.Timestamp = watch.clock().UTC().Format(time.RFC3339Nano)
	}
	if err := watch.events.Append(value); err != nil {
		return fmt.Errorf("append durable watchdog event: %w", err)
	}
	if notify && watch.notifier != nil {
		if err := watch.notifier.Notify(ctx, value); err != nil {
			failure := event{
				Timestamp: value.Timestamp,
				Severity:  "alert",
				Kind:      "webhook_delivery_failed",
				Message:   err.Error(),
			}
			if appendErr := watch.events.Append(failure); appendErr != nil {
				return errors.Join(err, appendErr)
			}
		}
	}
	return nil
}

func (watch *watchdog) clock() time.Time {
	if watch.now != nil {
		return watch.now()
	}
	return time.Now()
}

func (watch *watchdog) restore(state watchdogState) {
	if state.Version != 1 {
		return
	}
	watch.lastCondition = state.LastCondition
	watch.lastMeasureError = state.LastMeasureError
	watch.capacityReached = state.CapacityReached
	watch.lastCapacityStopID = state.LastCapacityStopID
	watch.lastMeasurement = state.LastMeasurement
}

func (watch *watchdog) persist() error {
	if watch.saveState == nil {
		return nil
	}
	return watch.saveState(watchdogState{
		Version:            1,
		LastCondition:      watch.lastCondition,
		LastMeasureError:   watch.lastMeasureError,
		CapacityReached:    watch.capacityReached,
		LastCapacityStopID: watch.lastCapacityStopID,
		LastMeasurement:    watch.lastMeasurement,
	})
}

func measurementEvent(
	severity string,
	kind string,
	message string,
	measured footprint,
) event {
	return event{
		Severity:   severity,
		Kind:       kind,
		Message:    message,
		OwnedBytes: int64Pointer(measured.Bytes),
		LimitBytes: capacityLimitBytes,
		Objects:    measured.Objects,
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}

func intPointer(value int) *int {
	return &value
}
