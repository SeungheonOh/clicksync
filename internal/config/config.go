package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const MainnetMagic = uint32(764824073)

const DefaultPeersText = "backbone.cardano.iog.io:3001|iog," +
	"backbone.mainnet.cardanofoundation.org:3001|cardano-foundation"

type Peer struct {
	Host     string `json:"host"`
	Operator string `json:"operator"`
}

type StartPoint struct {
	Slot uint64
	Hash [32]byte
}

type Config struct {
	ClickHouseHost     string
	ClickHousePort     uint16
	ClickHouseUser     string
	ClickHousePassword string
	ClickHouseDatabase string
	NetworkName        string
	NetworkMagic       uint32
	Peers              []Peer
	Corroboration      int
	Start              string
	StartPoint         string
	QueueCapacity      int
	HeaderBatchSize    int
	LockPath           string
	WriterCoordination string
	DialTimeout        time.Duration
	ProtocolTimeout    time.Duration
}

type PeerConfig struct {
	NetworkName   string
	NetworkMagic  uint32
	Peers         []Peer
	Corroboration int
}

func PeersFromEnv() (PeerConfig, error) {
	networkName := env("CARDANO_NETWORK_NAME", "mainnet")
	magic, err := envUint32("CARDANO_NETWORK_MAGIC", MainnetMagic)
	if err != nil {
		return PeerConfig{}, err
	}
	corroboration, err := envInt("CARDANO_CORROBORATION", 2)
	if err != nil {
		return PeerConfig{}, err
	}
	var peers []Peer
	for _, value := range strings.Split(env("CARDANO_PEERS", DefaultPeersText), ",") {
		if value = strings.TrimSpace(value); value != "" {
			peer, err := parsePeer(value)
			if err != nil {
				return PeerConfig{}, err
			}
			peers = append(peers, peer)
		}
	}
	if networkName != "mainnet" {
		return PeerConfig{}, fmt.Errorf(
			"CARDANO_NETWORK_NAME must be mainnet, got %q",
			networkName,
		)
	}
	if magic != MainnetMagic {
		return PeerConfig{}, fmt.Errorf(
			"CARDANO_NETWORK_MAGIC must be mainnet magic %d, got %d",
			MainnetMagic,
			magic,
		)
	}
	if corroboration < 2 || corroboration > len(peers) {
		return PeerConfig{}, fmt.Errorf("CARDANO_CORROBORATION must be in 2..%d", len(peers))
	}
	if err := validateIndependentPeers(peers, corroboration); err != nil {
		return PeerConfig{}, err
	}
	return PeerConfig{
		NetworkName:   networkName,
		NetworkMagic:  magic,
		Peers:         peers,
		Corroboration: corroboration,
	}, nil
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
	if cfg.QueueCapacity, err = envInt("CLICKSYNC_QUEUE_CAPACITY", 4); err != nil {
		return Config{}, err
	}
	if cfg.HeaderBatchSize, err = envInt("CLICKSYNC_HEADER_BATCH_SIZE", 32); err != nil {
		return Config{}, err
	}
	peerConfig, err := PeersFromEnv()
	if err != nil {
		return Config{}, err
	}
	cfg.NetworkMagic = peerConfig.NetworkMagic
	cfg.NetworkName = peerConfig.NetworkName
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
	case c.NetworkName != "mainnet":
		return fmt.Errorf("CARDANO_NETWORK_NAME must be mainnet, got %q", c.NetworkName)
	case c.NetworkMagic != MainnetMagic:
		return fmt.Errorf(
			"CARDANO_NETWORK_MAGIC must be mainnet magic %d, got %d",
			MainnetMagic,
			c.NetworkMagic,
		)
	case len(c.Peers) < 2:
		return errors.New("at least two independently operated CARDANO_PEERS are required")
	case c.Corroboration < 2 || c.Corroboration > len(c.Peers):
		return fmt.Errorf("CARDANO_CORROBORATION must be in 2..%d", len(c.Peers))
	case c.Start != "origin" && c.Start != "intersection":
		return errors.New("CLICKSYNC_START must be origin or intersection")
	case c.Start == "origin" && c.StartPoint != "":
		return errors.New("CLICKSYNC_START_POINT cannot be set for Origin sync")
	case c.Start == "intersection" && c.StartPoint == "":
		return errors.New("CLICKSYNC_START_POINT is required for intersection sync")
	case c.QueueCapacity < 1 || c.QueueCapacity > 32:
		return errors.New("CLICKSYNC_QUEUE_CAPACITY must be in 1..32")
	case c.HeaderBatchSize < 1 || c.HeaderBatchSize > 32:
		return errors.New("CLICKSYNC_HEADER_BATCH_SIZE must be in 1..32")
	case c.WriterCoordination != "" && c.WriterCoordination != "single-host-flock":
		return errors.New("only single-host-flock writer coordination is supported")
	}
	if err := validateIndependentPeers(c.Peers, c.Corroboration); err != nil {
		return err
	}
	if c.Start == "intersection" {
		if _, err := ParseStartPoint(c.StartPoint); err != nil {
			return fmt.Errorf("CLICKSYNC_START_POINT: %w", err)
		}
	}
	return nil
}

func parsePeer(value string) (Peer, error) {
	address, operator, ok := strings.Cut(value, "|")
	if !ok || strings.Contains(operator, "|") {
		return Peer{}, fmt.Errorf(
			"CARDANO_PEERS entry %q must be host:port|operator",
			value,
		)
	}
	address = strings.TrimSpace(address)
	operator = strings.TrimSpace(operator)
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return Peer{}, fmt.Errorf(
			"CARDANO_PEERS entry %q has invalid host:port",
			value,
		)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return Peer{}, fmt.Errorf(
			"CARDANO_PEERS entry %q has invalid port",
			value,
		)
	}
	if !validOperatorLabel(operator) {
		return Peer{}, fmt.Errorf(
			"CARDANO_PEERS entry %q has invalid operator label",
			value,
		)
	}
	return Peer{
		Host:     net.JoinHostPort(strings.ToLower(host), strconv.FormatUint(port, 10)),
		Operator: operator,
	}, nil
}

func validOperatorLabel(value string) bool {
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

func validateIndependentPeers(peers []Peer, corroboration int) error {
	addresses := make(map[string]struct{}, len(peers))
	operators := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		parsed, err := parsePeer(peer.Host + "|" + peer.Operator)
		if err != nil {
			return err
		}
		addressKey := strings.ToLower(parsed.Host)
		if _, ok := addresses[addressKey]; ok {
			return fmt.Errorf(
				"duplicate peer address %q cannot count as independent corroboration",
				peer.Host,
			)
		}
		addresses[addressKey] = struct{}{}
		operatorKey := strings.ToLower(parsed.Operator)
		if _, ok := operators[operatorKey]; ok {
			return fmt.Errorf(
				"duplicate operator %q cannot count as independent corroboration",
				peer.Operator,
			)
		}
		operators[operatorKey] = struct{}{}
	}
	if len(operators) < corroboration {
		return fmt.Errorf(
			"CARDANO_CORROBORATION %d exceeds %d independent operators",
			corroboration,
			len(operators),
		)
	}
	return nil
}

// ParseStartPoint deliberately accepts only SLOT:HASH. The block number must
// be decoded from an exact, body-hash-validated BlockFetch before the dataset
// manifest is created; it is never trusted from operator configuration.
func ParseStartPoint(value string) (StartPoint, error) {
	parts := strings.Split(strings.TrimSpace(value), ":")
	if len(parts) != 2 {
		return StartPoint{}, errors.New("must be SLOT:64_HEX_HASH")
	}
	var ret StartPoint
	slot, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return StartPoint{}, fmt.Errorf("parse slot: %w", err)
	}
	hash, err := hex.DecodeString(parts[1])
	if err != nil {
		return StartPoint{}, fmt.Errorf("parse hash: %w", err)
	}
	if len(hash) != len(ret.Hash) {
		return StartPoint{}, fmt.Errorf("hash must be 32 bytes, got %d", len(hash))
	}
	ret.Slot = slot
	copy(ret.Hash[:], hash)
	if ret.Hash == ([32]byte{}) {
		return StartPoint{}, errors.New("hash must not be all zero")
	}
	return ret, nil
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
