package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

const defaultPollInterval = 30 * time.Second

type durableEventLog struct {
	file *os.File
}

type webhookNotifier struct {
	url    string
	client *http.Client
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	once := flag.Bool("once", false, "perform one observation and exit")
	flag.Parse()

	stateDir, err := watchdogStateDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return fmt.Errorf("create watchdog state directory: %w", err)
	}
	if err := os.Chmod(stateDir, 0o700); err != nil {
		return fmt.Errorf("secure watchdog state directory: %w", err)
	}
	lock, err := acquireProcessLock(filepath.Join(stateDir, "watchdog.lock"))
	if err != nil {
		return err
	}
	defer lock.Close()
	events, err := openDurableEventLog(filepath.Join(stateDir, "events.jsonl"))
	if err != nil {
		return err
	}
	defer events.file.Close()

	interval, err := pollInterval()
	if err != nil {
		return err
	}
	socket := os.Getenv("CLICKSYNC_WATCHDOG_DOCKER_SOCKET")
	if socket == "" {
		socket = "/var/run/docker.sock"
	}
	var docker dockerAPI
	if endpoint := os.Getenv("CLICKSYNC_WATCHDOG_DOCKER_ENDPOINT"); endpoint != "" {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Scheme != "http" || parsed.Host == "" ||
			parsed.User != nil {
			return errors.New("CLICKSYNC_WATCHDOG_DOCKER_ENDPOINT must be an HTTP URL without userinfo")
		}
		docker = newDockerHTTPClient(endpoint)
	} else {
		docker = newDockerClient(socket)
	}
	var notifier eventNotifier
	if endpoint := os.Getenv("CLICKSYNC_WATCHDOG_WEBHOOK_URL"); endpoint != "" {
		parsed, err := url.Parse(endpoint)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
			parsed.Host == "" || parsed.User != nil {
			return errors.New("CLICKSYNC_WATCHDOG_WEBHOOK_URL must be an HTTP(S) URL without userinfo")
		}
		notifier = &webhookNotifier{
			url: endpoint,
			client: &http.Client{
				Timeout: 10 * time.Second,
			},
		}
	}
	watch := &watchdog{
		docker:              docker,
		events:              events,
		notifier:            notifier,
		measurementInterval: time.Hour,
		saveState: func(value watchdogState) error {
			return saveWatchdogState(stateDir, value)
		},
	}
	state, err := loadWatchdogState(filepath.Join(stateDir, "state.json"))
	if err != nil {
		return err
	}
	watch.restore(state)
	start := event{
		Severity:   "info",
		Kind:       "watchdog_started",
		Message:    "external Clicksync Docker watchdog started",
		LimitBytes: capacityLimitBytes,
	}
	if notifier == nil {
		start.Message += "; no webhook is configured, so alerts are durable locally only"
	}
	if err := watch.emit(context.Background(), start, false); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	if *once {
		return watch.Step(ctx)
	}
	err = watch.Run(ctx, interval)
	if errors.Is(err, context.Canceled) {
		_ = watch.emit(context.Background(), event{
			Severity: "info",
			Kind:     "watchdog_stopped",
			Message:  "external Clicksync Docker watchdog stopped",
		}, false)
		return nil
	}
	return err
}

func loadWatchdogState(path string) (watchdogState, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return watchdogState{}, nil
	}
	if err != nil {
		return watchdogState{}, fmt.Errorf("read watchdog state: %w", err)
	}
	var state watchdogState
	if err := json.Unmarshal(data, &state); err != nil {
		return watchdogState{}, fmt.Errorf("decode watchdog state: %w", err)
	}
	if state.Version != 1 {
		return watchdogState{}, fmt.Errorf(
			"unsupported watchdog state version %d",
			state.Version,
		)
	}
	return state, nil
}

func saveWatchdogState(dir string, state watchdogState) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	temporary := filepath.Join(
		dir,
		".state.json."+strconv.Itoa(os.Getpid()),
	)
	file, err := os.OpenFile(
		temporary,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0o600,
	)
	if err != nil {
		return fmt.Errorf("open temporary watchdog state: %w", err)
	}
	cleanup := func() {
		file.Close()
		os.Remove(temporary)
	}
	if _, err := file.Write(encoded); err != nil {
		cleanup()
		return err
	}
	if err := file.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := file.Close(); err != nil {
		os.Remove(temporary)
		return err
	}
	target := filepath.Join(dir, "state.json")
	if err := os.Rename(temporary, target); err != nil {
		os.Remove(temporary)
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func watchdogStateDir() (string, error) {
	if configured := os.Getenv("CLICKSYNC_WATCHDOG_STATE_DIR"); configured != "" {
		return configured, nil
	}
	if stateHome := os.Getenv("XDG_STATE_HOME"); stateHome != "" {
		return filepath.Join(stateHome, "clicksync-watchdog"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".local", "state", "clicksync-watchdog"), nil
}

func pollInterval() (time.Duration, error) {
	raw := os.Getenv("CLICKSYNC_WATCHDOG_POLL_INTERVAL")
	if raw == "" {
		return defaultPollInterval, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse CLICKSYNC_WATCHDOG_POLL_INTERVAL: %w", err)
	}
	if value < 5*time.Second {
		return 0, errors.New("CLICKSYNC_WATCHDOG_POLL_INTERVAL must be at least 5s")
	}
	return value, nil
}

func acquireProcessLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open watchdog lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, errors.New("another clicksync-watchdog process holds the state lock")
	}
	if err := file.Truncate(0); err != nil {
		file.Close()
		return nil, err
	}
	if _, err := file.WriteString(strconv.Itoa(os.Getpid()) + "\n"); err != nil {
		file.Close()
		return nil, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

func openDurableEventLog(path string) (*durableEventLog, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open watchdog event log: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		file.Close()
		return nil, fmt.Errorf("secure watchdog event log: %w", err)
	}
	return &durableEventLog{file: file}, nil
}

func (log *durableEventLog) Append(value event) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	if _, err := log.file.Write(encoded); err != nil {
		return err
	}
	return log.file.Sync()
}

func (notifier *webhookNotifier) Notify(
	ctx context.Context,
	value event,
) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		notifier.url,
		bytes.NewReader(encoded),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := notifier.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf(
			"webhook returned %s: %s",
			response.Status,
			string(message),
		)
	}
	return nil
}
