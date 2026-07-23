package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	MainnetMagic        = uint32(764824073)
	DefaultProjectLimit = uint64(100 * 1024 * 1024 * 1024)
	DefaultActiveLimit  = uint64(70 * 1024 * 1024 * 1024)
	DefaultWarningLimit = uint64(60 * 1024 * 1024 * 1024)
)

var DefaultPeers = []string{
	"backbone.cardano.iog.io:3001",
	"backbone.mainnet.cardanofoundation.org:3001",
}

type Config struct {
	ClickHouseHost     string
	ClickHousePort     uint16
	ClickHouseUser     string
	ClickHousePassword string
	ClickHouseDatabase string
	NetworkName        string
	NetworkMagic       uint32
	Peers              []string
	Corroboration      int
	Start              string
	StartPoint         string
	MaxBlocks          uint64
	StopAtTip          bool
	QueueCapacity      int
	LockPath           string
	WriterCoordination string
	WarningBytes       uint64
	ActiveLimitBytes   uint64
	ProjectLimitBytes  uint64
	DialTimeout        time.Duration
	ProtocolTimeout    time.Duration
}

type PeerConfig struct {
	NetworkMagic  uint32
	Peers         []string
	Corroboration int
}

func PeersFromEnv() (PeerConfig, error) {
	magic, err := envUint32("CARDANO_NETWORK_MAGIC", MainnetMagic)
	if err != nil {
		return PeerConfig{}, err
	}
	corroboration, err := envInt("CARDANO_CORROBORATION", 2)
	if err != nil {
		return PeerConfig{}, err
	}
	var peers []string
	for _, peer := range strings.Split(env("CARDANO_PEERS", strings.Join(DefaultPeers, ",")), ",") {
		if peer = strings.TrimSpace(peer); peer != "" {
			peers = append(peers, peer)
		}
	}
	if magic == 0 {
		return PeerConfig{}, errors.New("CARDANO_NETWORK_MAGIC must be non-zero")
	}
	if corroboration < 2 || corroboration > len(peers) {
		return PeerConfig{}, fmt.Errorf("CARDANO_CORROBORATION must be in 2..%d", len(peers))
	}
	seen := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		key := strings.ToLower(peer)
		if _, ok := seen[key]; ok {
			return PeerConfig{}, fmt.Errorf("duplicate peer %q cannot count as independent corroboration", peer)
		}
		seen[key] = struct{}{}
	}
	return PeerConfig{NetworkMagic: magic, Peers: peers, Corroboration: corroboration}, nil
}

func FromEnv() (Config, error) {
	cfg := Config{
		ClickHouseHost:     env("CLICKHOUSE_HOST", "127.0.0.1"),
		ClickHouseUser:     env("CLICKHOUSE_USER", "clicksync"),
		ClickHousePassword: os.Getenv("CLICKHOUSE_PASSWORD"),
		ClickHouseDatabase: env("CLICKHOUSE_DATABASE", "clicksync"),
		NetworkName:        env("CARDANO_NETWORK_NAME", "mainnet"),
		Start:              env("CLICKSYNC_START", "intersection"),
		StartPoint:         strings.TrimSpace(os.Getenv("CLICKSYNC_START_POINT")),
		LockPath:           env("CLICKSYNC_LOCK_PATH", "./clicksync-state/writer.lock"),
		WriterCoordination: env("CLICKSYNC_WRITER_COORDINATION", ""),
		DialTimeout:        10 * time.Second,
		ProtocolTimeout:    90 * time.Second,
	}
	var err error
	if cfg.ClickHousePort, err = envUint16("CLICKHOUSE_NATIVE_PORT", 9000); err != nil {
		return Config{}, err
	}
	if cfg.MaxBlocks, err = envUint64("CLICKSYNC_MAX_BLOCKS", 0); err != nil {
		return Config{}, err
	}
	if cfg.StopAtTip, err = envBool("CLICKSYNC_STOP_AT_TIP", false); err != nil {
		return Config{}, err
	}
	if cfg.QueueCapacity, err = envInt("CLICKSYNC_QUEUE_CAPACITY", 4); err != nil {
		return Config{}, err
	}
	if cfg.WarningBytes, err = envUint64("CLICKSYNC_WARNING_BYTES", DefaultWarningLimit); err != nil {
		return Config{}, err
	}
	if cfg.ActiveLimitBytes, err = envUint64("CLICKSYNC_ACTIVE_DATA_LIMIT_BYTES", DefaultActiveLimit); err != nil {
		return Config{}, err
	}
	if cfg.ProjectLimitBytes, err = envUint64("CLICKSYNC_PROJECT_LIMIT_BYTES", DefaultProjectLimit); err != nil {
		return Config{}, err
	}
	peerConfig, err := PeersFromEnv()
	if err != nil {
		return Config{}, err
	}
	cfg.NetworkMagic = peerConfig.NetworkMagic
	cfg.Peers = peerConfig.Peers
	cfg.Corroboration = peerConfig.Corroboration
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	switch {
	case c.ClickHouseHost == "":
		return errors.New("CLICKHOUSE_HOST is required")
	case c.ClickHousePort == 0:
		return errors.New("CLICKHOUSE_NATIVE_PORT must be non-zero")
	case c.ClickHouseUser == "":
		return errors.New("CLICKHOUSE_USER is required")
	case c.ClickHousePassword == "":
		return errors.New("CLICKHOUSE_PASSWORD is required")
	case c.ClickHouseDatabase != "clicksync":
		return fmt.Errorf("CLICKHOUSE_DATABASE must be clicksync, got %q", c.ClickHouseDatabase)
	case c.NetworkMagic == 0:
		return errors.New("CARDANO_NETWORK_MAGIC must be non-zero")
	case len(c.Peers) < 2:
		return errors.New("at least two independently operated CARDANO_PEERS are required")
	case c.Corroboration < 2 || c.Corroboration > len(c.Peers):
		return fmt.Errorf("CARDANO_CORROBORATION must be in 2..%d", len(c.Peers))
	case c.Start != "origin" && c.Start != "intersection":
		return errors.New("CLICKSYNC_START must be origin or intersection")
	case c.Start == "origin" && c.StartPoint != "":
		return errors.New("CLICKSYNC_START_POINT cannot be set for Origin sync")
	case c.QueueCapacity < 1 || c.QueueCapacity > 32:
		return errors.New("CLICKSYNC_QUEUE_CAPACITY must be in 1..32")
	case c.WriterCoordination != "" && c.WriterCoordination != "single-host-flock":
		return errors.New("only single-host-flock writer coordination is supported")
	case c.WarningBytes >= c.ActiveLimitBytes || c.ActiveLimitBytes >= c.ProjectLimitBytes:
		return errors.New("storage thresholds must satisfy warning < active limit < project limit")
	}
	seen := make(map[string]struct{}, len(c.Peers))
	for _, peer := range c.Peers {
		key := strings.ToLower(peer)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate peer %q cannot count as independent corroboration", peer)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envUint16(name string, fallback uint16) (uint16, error) {
	value, err := envUint64(name, uint64(fallback))
	if err != nil || value > 65535 {
		return 0, fmt.Errorf("%s must fit UInt16", name)
	}
	return uint16(value), nil
}

func envUint32(name string, fallback uint32) (uint32, error) {
	value, err := envUint64(name, uint64(fallback))
	if err != nil || value > 1<<32-1 {
		return 0, fmt.Errorf("%s must fit UInt32", name)
	}
	return uint32(value), nil
}

func envUint64(name string, fallback uint64) (uint64, error) {
	text := strings.TrimSpace(os.Getenv(name))
	if text == "" {
		return fallback, nil
	}
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return value, nil
}

func envInt(name string, fallback int) (int, error) {
	text := strings.TrimSpace(os.Getenv(name))
	if text == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(text)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return value, nil
}

func envBool(name string, fallback bool) (bool, error) {
	text := strings.TrimSpace(os.Getenv(name))
	if text == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(text)
	if err != nil {
		return false, fmt.Errorf("%s: %w", name, err)
	}
	return value, nil
}
