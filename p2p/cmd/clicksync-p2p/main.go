package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"clicksync/p2p/internal/contract"
)

const (
	mainnetMagic = uint32(764824073)
	trustLabel   = "peer-observed, structurally verified Cardano chain data"
)

var defaultPeers = []string{
	"backbone.cardano.iog.io:3001",
	"backbone.mainnet.emurgornd.com:3001",
	"backbone.mainnet.cardanofoundation.org:3001",
}

type stringList []string

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

func (s *stringList) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("peer must not be empty")
	}
	*s = append(*s, value)
	return nil
}

type config struct {
	peers        []string
	networkMagic uint
	corroborate  int
	point        string
	ackWindow    int
	dialTimeout  time.Duration
	opTimeout    time.Duration
}

type peerConnection struct {
	address  string
	version  uint16
	conn     *ouroboros.Connection
	asyncErr <-chan error
}

type peerTip struct {
	peer *peerConnection
	tip  chainsync.Tip
}

type probePayload struct {
	Mode              string   `json:"mode"`
	DatasetStatus     string   `json:"dataset_status"`
	TrustDescription  string   `json:"trust_description"`
	Era               string   `json:"era"`
	BlockType         int      `json:"block_type"`
	BlockNumber       uint64   `json:"block_number"`
	TransactionCount  int      `json:"transaction_count"`
	CorroboratedPeers []string `json:"corroborated_peers"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.opTimeout)
	defer cancel()

	if err := runProbe(ctx, cfg, logger); err != nil {
		logger.Error("direct N2N probe failed", "error", err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (config, error) {
	cfg := config{
		networkMagic: uint(mainnetMagic),
		corroborate:  2,
		ackWindow:    1,
		dialTimeout:  10 * time.Second,
		opTimeout:    90 * time.Second,
	}
	var peers stringList
	fs := flag.NewFlagSet("clicksync-p2p", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Var(&peers, "peer", "outbound Cardano N2N peer host:port (repeatable)")
	fs.UintVar(&cfg.networkMagic, "network-magic", cfg.networkMagic, "Cardano network magic")
	fs.IntVar(&cfg.corroborate, "corroborate", cfg.corroborate, "number of independent configured peers that must return the block")
	fs.StringVar(&cfg.point, "point", "", "optional stable point SLOT:64_HEX_HASH; otherwise use the first peer's observed tip")
	fs.IntVar(&cfg.ackWindow, "ack-window", cfg.ackWindow, "maximum unacknowledged events (1-8)")
	fs.DurationVar(&cfg.dialTimeout, "dial-timeout", cfg.dialTimeout, "per-peer TCP and handshake timeout")
	fs.DurationVar(&cfg.opTimeout, "timeout", cfg.opTimeout, "hard wall-clock bound for the probe")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}
	if len(peers) == 0 {
		peers = append(peers, defaultPeers...)
	}
	cfg.peers = peers
	seenPeers := make(map[string]struct{}, len(cfg.peers))
	for _, peer := range cfg.peers {
		key := strings.ToLower(peer)
		if _, ok := seenPeers[key]; ok {
			return config{}, fmt.Errorf("duplicate configured peer %q cannot count as independent corroboration", peer)
		}
		seenPeers[key] = struct{}{}
	}
	switch {
	case cfg.networkMagic == 0 || cfg.networkMagic > uint(^uint32(0)):
		return config{}, errors.New("network magic must fit a non-zero uint32")
	case cfg.corroborate < 1:
		return config{}, errors.New("corroborate must be positive")
	case cfg.corroborate > len(cfg.peers):
		return config{}, fmt.Errorf("corroborate=%d exceeds %d configured peers", cfg.corroborate, len(cfg.peers))
	case cfg.ackWindow < 1 || cfg.ackWindow > 8:
		return config{}, errors.New("ack-window must be between 1 and 8")
	case cfg.dialTimeout <= 0:
		return config{}, errors.New("dial-timeout must be positive")
	case cfg.opTimeout <= 0:
		return config{}, errors.New("timeout must be positive")
	}
	if cfg.point != "" {
		if _, err := parsePoint(cfg.point); err != nil {
			return config{}, err
		}
	}
	return cfg, nil
}

func runProbe(ctx context.Context, cfg config, logger *slog.Logger) error {
	var connected []*peerConnection
	defer func() {
		for _, p := range connected {
			if err := p.conn.Close(); err != nil {
				logger.Warn("close peer connection", "peer", p.address, "error", err)
			}
		}
	}()

	var peerTips []peerTip
	for _, address := range cfg.peers {
		if len(peerTips) >= cfg.corroborate {
			break
		}
		peer, err := dialPeer(address, uint32(cfg.networkMagic), cfg.dialTimeout, logger)
		if err != nil {
			logger.Warn("peer unavailable", "peer", address, "error", err)
			continue
		}
		connected = append(connected, peer)
		currentTip, err := peer.conn.ChainSync().Client.GetCurrentTip()
		if err != nil {
			logger.Warn("ChainSync current tip failed", "peer", address, "error", err)
			continue
		}
		if currentTip.Point.Slot == 0 || len(currentTip.Point.Hash) != 32 {
			logger.Warn("peer returned unusable tip", "peer", address)
			continue
		}
		if err := peer.checkAsyncError(); err != nil {
			logger.Warn("peer reported asynchronous protocol error", "peer", address, "error", err)
			continue
		}
		peerTips = append(peerTips, peerTip{peer: peer, tip: *currentTip})
	}
	if len(peerTips) < cfg.corroborate {
		return fmt.Errorf("only %d of %d required independent peers completed handshake and ChainSync", len(peerTips), cfg.corroborate)
	}
	var candidates []pcommon.Point
	if cfg.point != "" {
		point, err := parsePoint(cfg.point)
		if err != nil {
			return err
		}
		candidates = []pcommon.Point{point}
	} else {
		slices.SortFunc(peerTips, func(a, b peerTip) int {
			switch {
			case a.tip.BlockNumber < b.tip.BlockNumber:
				return -1
			case a.tip.BlockNumber > b.tip.BlockNumber:
				return 1
			default:
				return strings.Compare(hex.EncodeToString(a.tip.Point.Hash), hex.EncodeToString(b.tip.Point.Hash))
			}
		})
		for _, item := range peerTips {
			candidates = append(candidates, item.tip.Point)
		}
	}
	first := peerTips[0].peer
	tip := peerTips[0].tip

	point, block, corroborated, err := fetchCorroborated(candidates, peerTips, logger)
	if err != nil {
		return err
	}

	sessionID, err := newSessionID()
	if err != nil {
		return err
	}
	emitter, err := contract.NewEmitter(
		os.Stdout,
		sessionID,
		uint32(cfg.networkMagic),
		first.address,
		first.version,
		cfg.ackWindow,
	)
	if err != nil {
		return err
	}
	emitter.StartAckReader(os.Stdin)
	if _, err := emitter.Emit(ctx, "ready", false, func(env *contract.Envelope) error {
		payload, err := json.Marshal(map[string]any{
			"mode":                 "probe",
			"dataset_status":       "partial_tail",
			"trust_description":    trustLabel,
			"corroboration_target": cfg.corroborate,
		})
		env.Payload = payload
		return err
	}); err != nil {
		return err
	}

	pointEnvelope := pointToContract(point)
	tipEnvelope := tipToContract(tip)
	payload, err := json.Marshal(probePayload{
		Mode:              "probe",
		DatasetStatus:     "partial_tail",
		TrustDescription:  trustLabel,
		Era:               block.Era().Name,
		BlockType:         block.Type(),
		BlockNumber:       block.BlockNumber(),
		TransactionCount:  len(block.Transactions()),
		CorroboratedPeers: corroborated,
	})
	if err != nil {
		return err
	}
	if _, err := emitter.Emit(ctx, "roll_forward", true, func(env *contract.Envelope) error {
		env.Point = &pointEnvelope
		env.Tip = &tipEnvelope
		env.Verification = &contract.Verification{
			BodyHash:        true,
			Point:           true,
			Parent:          false,
			BlockNumber:     false,
			SlotProgression: false,
		}
		env.Payload = payload
		return nil
	}); err != nil {
		return err
	}
	if err := emitter.Drain(ctx); err != nil {
		return fmt.Errorf("wait for publication acknowledgement: %w", err)
	}
	logger.Info(
		"direct N2N probe complete",
		"peer", first.address,
		"n2n_version", first.version,
		"slot", point.Slot,
		"hash", hex.EncodeToString(point.Hash),
		"corroborated_peers", len(corroborated),
	)
	return nil
}

func fetchCorroborated(
	candidates []pcommon.Point,
	peers []peerTip,
	logger *slog.Logger,
) (pcommon.Point, lcommon.Block, []string, error) {
	for _, point := range candidates {
		var selected lcommon.Block
		var corroborated []string
		failed := false
		for _, item := range peers {
			block, err := item.peer.conn.BlockFetch().Client.GetBlock(point)
			if err != nil {
				logger.Warn("candidate BlockFetch failed", "peer", item.peer.address, "slot", point.Slot, "error", err)
				failed = true
				break
			}
			if err := verifyFetchedPoint(block, point); err != nil {
				return pcommon.Point{}, nil, nil, fmt.Errorf(
					"candidate structural mismatch from %s: %w",
					item.peer.address,
					err,
				)
			}
			if selected == nil {
				selected = block
			} else if block.Hash() != selected.Hash() || block.BlockBodyHash() != selected.BlockBodyHash() {
				return pcommon.Point{}, nil, nil, fmt.Errorf("candidate content mismatch from %s", item.peer.address)
			}
			if err := item.peer.checkAsyncError(); err != nil {
				logger.Warn("candidate peer protocol error", "peer", item.peer.address, "error", err)
				failed = true
				break
			}
			corroborated = append(corroborated, item.peer.address)
		}
		if !failed && len(corroborated) == len(peers) {
			return point, selected, corroborated, nil
		}
	}
	return pcommon.Point{}, nil, nil, fmt.Errorf(
		"no candidate point was returned identically by all %d independent peers",
		len(peers),
	)
}

func dialPeer(address string, magic uint32, timeout time.Duration, logger *slog.Logger) (*peerConnection, error) {
	blockCfg, err := blockfetch.NewConfig(
		blockfetch.WithBatchStartTimeout(timeout),
		blockfetch.WithBlockTimeout(timeout),
		blockfetch.WithRecvQueueSize(8),
	)
	if err != nil {
		return nil, err
	}
	chainCfg := chainsync.NewConfig(
		chainsync.WithPipelineLimit(1),
		chainsync.WithRecvQueueSize(8),
		chainsync.WithIntersectTimeout(timeout),
		chainsync.WithBlockTimeout(timeout),
	)
	errs := make(chan error, 10)
	asyncErr := make(chan error, 1)
	go func() {
		for err := range errs {
			if err == nil {
				continue
			}
			select {
			case asyncErr <- err:
			default:
			}
		}
		close(asyncErr)
	}()
	conn, err := ouroboros.NewConnection(
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithErrorChan(errs),
		ouroboros.WithLogger(logger),
		ouroboros.WithBlockFetchConfig(blockCfg),
		ouroboros.WithChainSyncConfig(chainCfg),
	)
	if err != nil {
		return nil, err
	}
	if err := conn.DialTimeout("tcp", address, timeout); err != nil {
		_ = conn.Close()
		return nil, err
	}
	version, _ := conn.ProtocolVersion()
	if version < 7 || version > 15 {
		_ = conn.Close()
		return nil, fmt.Errorf("peer negotiated unsupported N2N version %d", version)
	}
	return &peerConnection{
		address:  address,
		version:  version,
		conn:     conn,
		asyncErr: asyncErr,
	}, nil
}

func (p *peerConnection) checkAsyncError() error {
	select {
	case err, ok := <-p.asyncErr:
		if !ok {
			return errors.New("peer error channel closed unexpectedly")
		}
		return err
	default:
		return nil
	}
}

func verifyFetchedPoint(block lcommon.Block, requested pcommon.Point) error {
	if block == nil {
		return errors.New("nil decoded block")
	}
	if len(requested.Hash) != 32 {
		return fmt.Errorf("requested hash has %d bytes", len(requested.Hash))
	}
	if block.SlotNumber() != requested.Slot {
		return fmt.Errorf("slot mismatch: requested %d, decoded %d", requested.Slot, block.SlotNumber())
	}
	if !strings.EqualFold(block.Hash().String(), hex.EncodeToString(requested.Hash)) {
		return fmt.Errorf("hash mismatch: requested %x, decoded %s", requested.Hash, block.Hash())
	}
	return nil
}

func parsePoint(value string) (pcommon.Point, error) {
	slotText, hashText, ok := strings.Cut(value, ":")
	if !ok {
		return pcommon.Point{}, errors.New("point must be SLOT:64_HEX_HASH")
	}
	slot, err := strconv.ParseUint(slotText, 10, 64)
	if err != nil {
		return pcommon.Point{}, fmt.Errorf("parse point slot: %w", err)
	}
	hash, err := hex.DecodeString(hashText)
	if err != nil {
		return pcommon.Point{}, fmt.Errorf("parse point hash: %w", err)
	}
	if len(hash) != 32 {
		return pcommon.Point{}, fmt.Errorf("point hash must be 32 bytes, got %d", len(hash))
	}
	return pcommon.NewPoint(slot, hash), nil
}

func pointToContract(point pcommon.Point) contract.Point {
	if point.Slot == 0 && len(point.Hash) == 0 {
		return contract.Point{Origin: true}
	}
	return contract.Point{
		Slot: point.Slot,
		Hash: hex.EncodeToString(point.Hash),
	}
}

func tipToContract(tip chainsync.Tip) contract.Tip {
	return contract.Tip{
		Point:       pointToContract(tip.Point),
		BlockNumber: tip.BlockNumber,
	}
}

func newSessionID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate session ID: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}
