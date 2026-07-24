package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	MainnetMagic = uint32(764824073)

	defaultRelays = "backbone.cardano.iog.io:3001|iog," +
		"backbone.mainnet.cardanofoundation.org:3001|cardano-foundation"
	defaultStart = "133660799:" +
		"e757d57eb8dc9500a61c60a39fadb63d9be6973ba96ae337fd24453d4d15c343:" +
		"10781330"
)

type Database struct {
	Host     string
	Port     uint16
	User     string
	Password string
	Name     string
	OpenConn int
}

type Relay struct {
	Host     string
	Operator string
}

type Point struct {
	Origin      bool
	Slot        uint64
	Hash        [32]byte
	BlockNumber uint64
	IsByronEBB  bool
}

type Sync struct {
	Database Database

	NetworkName  string
	NetworkMagic uint32
	Relays       []Relay
	Start        Point

	LockPath              string
	DialTimeout           time.Duration
	ProtocolTimeout       time.Duration
	ShutdownTimeout       time.Duration
	ReconnectInitial      time.Duration
	ReconnectMaximum      time.Duration
	BlockFetchRangeBlocks int
	BlockFetchQueueSize   int
	RelayQueueSize        int
	AgreedQueueSize       int
	AgreedQueueBytes      int64
	NormalizeWorkers      int
	ReorderSize           int
	ReorderBytes          int64
	BatchBlocks           int
	BatchBytes            int64
	BatchRows             uint64
	BatchAge              time.Duration
	RollbackDepth         uint32
}

func DatabaseFromEnv() (Database, error) {
	port, err := uint16Env("CLICKHOUSE_NATIVE_PORT", 9000)
	if err != nil {
		return Database{}, err
	}
	openConn, err := intEnv("CLICKHOUSE_OPEN_CONNS", 16)
	if err != nil {
		return Database{}, err
	}
	cfg := Database{
		Host:     textEnv("CLICKHOUSE_HOST", "127.0.0.1"),
		Port:     port,
		User:     textEnv("CLICKHOUSE_USER", "clicksync"),
		Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		Name:     textEnv("CLICKHOUSE_DATABASE", "clicksync"),
		OpenConn: openConn,
	}
	return cfg, cfg.Validate()
}

func (c Database) Validate() error {
	switch {
	case strings.TrimSpace(c.Host) == "":
		return errors.New("CLICKHOUSE_HOST is required")
	case c.Port == 0:
		return errors.New("CLICKHOUSE_NATIVE_PORT must be non-zero")
	case strings.TrimSpace(c.User) == "":
		return errors.New("CLICKHOUSE_USER is required")
	case c.Password == "":
		return errors.New("CLICKHOUSE_PASSWORD is required")
	case c.Name != "clicksync":
		return fmt.Errorf("CLICKHOUSE_DATABASE must be clicksync, got %q", c.Name)
	case c.OpenConn < 2 || c.OpenConn > 64:
		return errors.New("CLICKHOUSE_OPEN_CONNS must be in 2..64")
	default:
		return nil
	}
}

func SyncFromEnv() (Sync, error) {
	database, err := DatabaseFromEnv()
	if err != nil {
		return Sync{}, err
	}
	networkMagic, err := uint32Env("CARDANO_NETWORK_MAGIC", MainnetMagic)
	if err != nil {
		return Sync{}, err
	}
	relays, err := ParseRelays(textEnv("CARDANO_RELAYS", defaultRelays))
	if err != nil {
		return Sync{}, err
	}
	start, err := ParsePoint(textEnv("CLICKSYNC_START", defaultStart))
	if err != nil {
		return Sync{}, fmt.Errorf("CLICKSYNC_START: %w", err)
	}
	rollbackDepth, err := RollbackDepthFromEnv()
	if err != nil {
		return Sync{}, err
	}
	cfg := Sync{
		Database:              database,
		NetworkName:           textEnv("CARDANO_NETWORK_NAME", "mainnet"),
		NetworkMagic:          networkMagic,
		Relays:                relays,
		Start:                 start,
		LockPath:              textEnv("CLICKSYNC_LOCK_PATH", "./clicksync-state/writer.lock"),
		DialTimeout:           durationEnv("CLICKSYNC_DIAL_TIMEOUT", 10*time.Second),
		ProtocolTimeout:       durationEnv("CLICKSYNC_PROTOCOL_TIMEOUT", 90*time.Second),
		ShutdownTimeout:       durationEnv("CLICKSYNC_SHUTDOWN_TIMEOUT", 45*time.Second),
		ReconnectInitial:      durationEnv("CLICKSYNC_RECONNECT_INITIAL", time.Second),
		ReconnectMaximum:      durationEnv("CLICKSYNC_RECONNECT_MAXIMUM", 30*time.Second),
		BlockFetchRangeBlocks: intEnvValue("CLICKSYNC_BLOCKFETCH_RANGE_BLOCKS", 512),
		BlockFetchQueueSize:   intEnvValue("CLICKSYNC_BLOCKFETCH_QUEUE_SIZE", 512),
		RelayQueueSize:        intEnvValue("CLICKSYNC_RELAY_QUEUE_SIZE", 256),
		AgreedQueueSize:       intEnvValue("CLICKSYNC_AGREED_QUEUE_SIZE", 256),
		AgreedQueueBytes:      int64EnvValue("CLICKSYNC_AGREED_QUEUE_BYTES", 256<<20),
		NormalizeWorkers:      intEnvValue("CLICKSYNC_NORMALIZE_WORKERS", runtime.GOMAXPROCS(0)),
		ReorderSize:           intEnvValue("CLICKSYNC_REORDER_SIZE", 256),
		ReorderBytes:          int64EnvValue("CLICKSYNC_REORDER_BYTES", 256<<20),
		BatchBlocks:           intEnvValue("CLICKSYNC_BATCH_BLOCKS", 1024),
		BatchBytes:            int64EnvValue("CLICKSYNC_BATCH_BYTES", 128<<20),
		BatchRows:             uint64EnvValue("CLICKSYNC_BATCH_ROWS", 2_000_000),
		BatchAge:              durationEnv("CLICKSYNC_BATCH_AGE", time.Second),
		RollbackDepth:         rollbackDepth,
	}
	return cfg, cfg.Validate()
}

func RollbackDepthFromEnv() (uint32, error) {
	value, err := uint32Env("CLICKSYNC_ROLLBACK_DEPTH", 2160)
	if err != nil {
		return 0, err
	}
	if value == 0 || value > 100_000 {
		return 0, errors.New("CLICKSYNC_ROLLBACK_DEPTH must be in 1..100000")
	}
	return value, nil
}

func (c Sync) Validate() error {
	if err := c.Database.Validate(); err != nil {
		return err
	}
	switch {
	case strings.TrimSpace(c.NetworkName) == "":
		return errors.New("CARDANO_NETWORK_NAME is required")
	case c.NetworkMagic == 0:
		return errors.New("CARDANO_NETWORK_MAGIC must be non-zero")
	case len(c.Relays) < 2:
		return errors.New("at least two CARDANO_RELAYS are required")
	case strings.TrimSpace(c.LockPath) == "":
		return errors.New("CLICKSYNC_LOCK_PATH is required")
	case c.DialTimeout <= 0:
		return errors.New("CLICKSYNC_DIAL_TIMEOUT must be positive")
	case c.ProtocolTimeout <= 0:
		return errors.New("CLICKSYNC_PROTOCOL_TIMEOUT must be positive")
	case c.ShutdownTimeout <= 0:
		return errors.New("CLICKSYNC_SHUTDOWN_TIMEOUT must be positive")
	case c.ReconnectInitial <= 0 || c.ReconnectMaximum < c.ReconnectInitial:
		return errors.New("reconnect durations must be positive and ordered")
	case c.BlockFetchRangeBlocks < 1 || c.BlockFetchRangeBlocks > 8192:
		return errors.New("CLICKSYNC_BLOCKFETCH_RANGE_BLOCKS must be in 1..8192")
	case c.BlockFetchQueueSize < 1 || c.BlockFetchQueueSize > 512:
		return errors.New("CLICKSYNC_BLOCKFETCH_QUEUE_SIZE must be in 1..512")
	case c.RelayQueueSize < 1 || c.RelayQueueSize > 4096:
		return errors.New("CLICKSYNC_RELAY_QUEUE_SIZE must be in 1..4096")
	case c.AgreedQueueSize < 1 || c.AgreedQueueSize > 4096:
		return errors.New("CLICKSYNC_AGREED_QUEUE_SIZE must be in 1..4096")
	case c.AgreedQueueBytes < 1<<20 || c.AgreedQueueBytes > 4<<30:
		return errors.New("CLICKSYNC_AGREED_QUEUE_BYTES must be in 1MiB..4GiB")
	case c.NormalizeWorkers < 1 || c.NormalizeWorkers > 256:
		return errors.New("CLICKSYNC_NORMALIZE_WORKERS must be in 1..256")
	case c.ReorderSize < c.NormalizeWorkers || c.ReorderSize > 4096:
		return errors.New("CLICKSYNC_REORDER_SIZE must be between worker count and 4096")
	case c.ReorderBytes < 1<<20 || c.ReorderBytes > 4<<30:
		return errors.New("CLICKSYNC_REORDER_BYTES must be in 1MiB..4GiB")
	case c.BatchBlocks < 1 || c.BatchBlocks > 4096:
		return errors.New("CLICKSYNC_BATCH_BLOCKS must be in 1..4096")
	case c.BatchBytes < 1<<20 || c.BatchBytes > 4<<30:
		return errors.New("CLICKSYNC_BATCH_BYTES must be in 1MiB..4GiB")
	case c.BatchRows < 1 || c.BatchRows > 10_000_000:
		return errors.New("CLICKSYNC_BATCH_ROWS must be in 1..10000000")
	case c.BatchAge <= 0 || c.BatchAge > 30*time.Second:
		return errors.New("CLICKSYNC_BATCH_AGE must be in (0,30s]")
	case c.RollbackDepth == 0 || c.RollbackDepth > 100_000:
		return errors.New("CLICKSYNC_ROLLBACK_DEPTH must be in 1..100000")
	}
	_, err := ParseRelays(formatRelays(c.Relays))
	return err
}

func ParseRelays(value string) ([]Relay, error) {
	var ret []Relay
	endpoints := make(map[string]struct{})
	operators := make(map[string]struct{})
	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		address, operator, ok := strings.Cut(entry, "|")
		if !ok || strings.Contains(operator, "|") {
			return nil, fmt.Errorf("relay %q must be host:port|operator", entry)
		}
		host, portText, err := net.SplitHostPort(strings.TrimSpace(address))
		if err != nil || strings.TrimSpace(host) == "" {
			return nil, fmt.Errorf("relay %q has an invalid host:port", entry)
		}
		port, err := strconv.ParseUint(portText, 10, 16)
		if err != nil || port == 0 {
			return nil, fmt.Errorf("relay %q has an invalid port", entry)
		}
		operator = strings.TrimSpace(operator)
		if !validLabel(operator) {
			return nil, fmt.Errorf("relay %q has an invalid operator label", entry)
		}
		endpoint := strings.ToLower(net.JoinHostPort(host, strconv.FormatUint(port, 10)))
		operatorKey := strings.ToLower(operator)
		if _, exists := endpoints[endpoint]; exists {
			return nil, fmt.Errorf("duplicate relay endpoint %q", endpoint)
		}
		if _, exists := operators[operatorKey]; exists {
			return nil, fmt.Errorf("duplicate relay operator %q", operator)
		}
		endpoints[endpoint] = struct{}{}
		operators[operatorKey] = struct{}{}
		ret = append(ret, Relay{Host: endpoint, Operator: operator})
	}
	if len(ret) < 2 {
		return nil, errors.New("at least two distinct relay endpoints and operators are required")
	}
	return ret, nil
}

func ParsePoint(value string) (Point, error) {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "origin") {
		return Point{Origin: true}, nil
	}
	parts := strings.Split(value, ":")
	if len(parts) != 3 && len(parts) != 4 {
		return Point{}, errors.New("must be origin or SLOT:64_HEX_HASH:BLOCK_NUMBER[:ebb]")
	}
	slot, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return Point{}, fmt.Errorf("parse slot: %w", err)
	}
	hash, err := hex.DecodeString(parts[1])
	if err != nil {
		return Point{}, fmt.Errorf("parse hash: %w", err)
	}
	if len(hash) != 32 {
		return Point{}, fmt.Errorf("hash must be 32 bytes, got %d", len(hash))
	}
	number, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return Point{}, fmt.Errorf("parse block number: %w", err)
	}
	point := Point{Slot: slot, BlockNumber: number}
	copy(point.Hash[:], hash)
	if point.Hash == ([32]byte{}) {
		return Point{}, errors.New("hash must not be all zero")
	}
	if len(parts) == 4 {
		if !strings.EqualFold(parts[3], "ebb") {
			return Point{}, errors.New("fourth point component must be ebb")
		}
		point.IsByronEBB = true
	}
	return point, nil
}

func validLabel(value string) bool {
	if len(value) < 1 || len(value) > 64 {
		return false
	}
	for index, char := range value {
		if char >= 'a' && char <= 'z' ||
			char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' ||
			index > 0 && (char == '.' || char == '_' || char == '-') {
			continue
		}
		return false
	}
	return true
}

func formatRelays(relays []Relay) string {
	values := make([]string, len(relays))
	for index, relay := range relays {
		values[index] = relay.Host + "|" + relay.Operator
	}
	return strings.Join(values, ",")
}

func textEnv(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return strings.TrimSpace(value)
	}
	return fallback
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return -1
	}
	return parsed
}

func uint16Env(name string, fallback uint16) (uint16, error) {
	value, err := uint64Env(name, uint64(fallback), 16)
	return uint16(value), err
}

func uint32Env(name string, fallback uint32) (uint32, error) {
	value, err := uint64Env(name, uint64(fallback), 32)
	return uint32(value), err
}

func uint64Env(name string, fallback uint64, bits int) (uint64, error) {
	text, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.ParseUint(strings.TrimSpace(text), 10, bits)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return value, nil
}

func intEnv(name string, fallback int) (int, error) {
	text, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(text))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return value, nil
}

func intEnvValue(name string, fallback int) int {
	value, err := intEnv(name, fallback)
	if err != nil {
		return -1
	}
	return value
}

func int64EnvValue(name string, fallback int64) int64 {
	text, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	value, err := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
	if err != nil {
		return -1
	}
	return value
}

func uint64EnvValue(name string, fallback uint64) uint64 {
	value, err := uint64Env(name, fallback, 64)
	if err != nil {
		return 0
	}
	return value
}
