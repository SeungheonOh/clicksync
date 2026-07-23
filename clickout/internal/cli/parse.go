package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/clicksync-project/clickout/internal/limits"
	"github.com/clicksync-project/clickout/internal/model"
	"github.com/clicksync-project/clickout/internal/repository"
)

var ErrUsage = errors.New("invalid command usage")

type Invocation struct {
	Command string
	UTxO    *model.UTxORef
	Tx      *model.Hash32
	Hash    *model.Hash32
	Address string
	State   string
	At      model.AtPoint
	Limit   uint32
	Cursor  string
	Trace   TraceInvocation
}

type TraceInvocation struct {
	Direction repository.TraceDirection
	Seed      repository.TraceSeed
	Address   string
	Asset     model.AssetSelector
	Limits    limits.Trace
	Format    string
}

func Parse(args []string) (Invocation, error) {
	if len(args) == 0 {
		return Invocation{}, usageError("missing command")
	}
	switch args[0] {
	case "utxo":
		return parseUTxO(args[1:])
	case "tx":
		return parseHashCommand("tx", args[1:])
	case "address":
		return parseAddress(args[1:])
	case "datum":
		return parseHashCommand("datum", args[1:])
	case "redeemers":
		return parseHashCommand("redeemers", args[1:])
	case "metadata":
		return parseHashCommand("metadata", args[1:])
	case "withdrawals":
		return parseHashCommand("withdrawals", args[1:])
	case "trace":
		return parseTrace(args[1:])
	case "help", "-h", "--help":
		return Invocation{Command: "help"}, nil
	default:
		return Invocation{}, usageError("unknown command %q", args[0])
	}
}

func parseUTxO(args []string) (Invocation, error) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return Invocation{}, usageError("utxo requires TX_HASH#INDEX before options")
	}
	refText := args[0]
	flags := newFlagSet("utxo")
	at := flags.String("at", "tip", "tip or block hash")
	if err := flags.Parse(args[1:]); err != nil {
		return Invocation{}, usageError("%v", err)
	}
	if flags.NArg() != 0 {
		return Invocation{}, usageError("utxo accepts one TX_HASH#INDEX")
	}
	ref, err := model.ParseUTxORef(refText)
	if err != nil {
		return Invocation{}, usageError("%v", err)
	}
	point, err := parseAt(*at)
	if err != nil {
		return Invocation{}, usageError("%v", err)
	}
	return Invocation{Command: "utxo", UTxO: &ref, At: point}, nil
}

func parseHashCommand(command string, args []string) (Invocation, error) {
	flags := newFlagSet(command)
	if err := flags.Parse(args); err != nil {
		return Invocation{}, usageError("%v", err)
	}
	if flags.NArg() != 1 {
		return Invocation{}, usageError("%s requires one 32-byte hash", command)
	}
	hash, err := model.ParseHash32(flags.Arg(0))
	if err != nil {
		return Invocation{}, usageError("%v", err)
	}
	return Invocation{Command: command, Hash: &hash, Tx: &hash, At: model.AtPoint{Tip: true}}, nil
}

func parseAddress(args []string) (Invocation, error) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return Invocation{}, usageError("address requires ADDRESS before options")
	}
	address := args[0]
	flags := newFlagSet("address")
	state := flags.String("state", "", "current or history")
	limit := flags.Uint("limit", uint(limits.DefaultAddressPage), "page limit")
	cursor := flags.String("cursor", "", "opaque page cursor")
	if err := flags.Parse(args[1:]); err != nil {
		return Invocation{}, usageError("%v", err)
	}
	if flags.NArg() != 0 || (*state != "current" && *state != "history") {
		return Invocation{}, usageError("address requires ADDRESS and --state current|history")
	}
	if uint64(*limit) > uint64(^uint32(0)) {
		return Invocation{}, usageError("%v", limits.ErrPageOutOfRange)
	}
	limit32 := uint32(*limit)
	if err := limits.ValidatePage(limit32); err != nil {
		return Invocation{}, usageError("%v", err)
	}
	return Invocation{
		Command: "address",
		Address: address,
		State:   *state,
		Limit:   limit32,
		Cursor:  *cursor,
		At:      model.AtPoint{Tip: true},
	}, nil
}

func parseTrace(args []string) (Invocation, error) {
	flags := newFlagSet("trace")
	direction := flags.String("direction", "", "forward or reverse")
	utxo := flags.String("utxo", "", "seed UTxO")
	tx := flags.String("tx", "", "seed transaction")
	address := flags.String("address", "", "seed address")
	maxDepth := flags.Uint("max-depth", uint(limits.DefaultTraceDepth), "maximum depth")
	maxNodes := flags.Uint("max-nodes", uint(limits.DefaultTraceNodes), "maximum visited UTxOs")
	maxEdges := flags.Uint("max-edges", uint(limits.DefaultTraceEdges), "maximum transaction hyperedges")
	asset := flags.String("asset", "ada", "ada or POLICY_HEX.ASSET_NAME_HEX")
	format := flags.String("format", "jsonl", "jsonl")
	if err := flags.Parse(args); err != nil {
		return Invocation{}, usageError("%v", err)
	}
	if flags.NArg() != 0 {
		return Invocation{}, usageError("trace accepts flags only")
	}
	if *direction != string(repository.Forward) && *direction != string(repository.Reverse) {
		return Invocation{}, usageError("trace requires --direction forward|reverse")
	}
	if *format != "jsonl" {
		return Invocation{}, usageError("trace --format must be jsonl")
	}
	if uint64(*maxDepth) > uint64(^uint32(0)) ||
		uint64(*maxNodes) > uint64(^uint32(0)) ||
		uint64(*maxEdges) > uint64(^uint32(0)) {
		return Invocation{}, usageError("trace bounds overflow")
	}
	traceLimits := limits.DefaultTrace()
	traceLimits.MaxDepth = uint32(*maxDepth)
	traceLimits.MaxNodes = uint32(*maxNodes)
	traceLimits.MaxEdges = uint32(*maxEdges)
	if err := traceLimits.Validate(); err != nil {
		return Invocation{}, usageError("%v", err)
	}
	selectedAsset, err := model.ParseAssetSelector(*asset)
	if err != nil {
		return Invocation{}, usageError("%v", err)
	}
	selected := 0
	var seed repository.TraceSeed
	if *utxo != "" {
		parsed, err := model.ParseUTxORef(*utxo)
		if err != nil {
			return Invocation{}, usageError("%v", err)
		}
		seed.UTxO = &parsed
		selected++
	}
	if *tx != "" {
		parsed, err := model.ParseHash32(*tx)
		if err != nil {
			return Invocation{}, usageError("%v", err)
		}
		seed.Tx = &parsed
		selected++
	}
	if *address != "" {
		selected++
	}
	if selected != 1 {
		return Invocation{}, usageError("trace requires exactly one of --utxo, --tx, or --address")
	}
	return Invocation{
		Command: "trace",
		At:      model.AtPoint{Tip: true},
		Trace: TraceInvocation{
			Direction: repository.TraceDirection(*direction),
			Seed:      seed,
			Address:   *address,
			Asset:     selectedAsset,
			Limits:    traceLimits,
			Format:    *format,
		},
	}, nil
}

func parseAt(value string) (model.AtPoint, error) {
	if value == "tip" {
		return model.AtPoint{Tip: true}, nil
	}
	hash, err := model.ParseHash32(value)
	if err != nil {
		return model.AtPoint{}, fmt.Errorf("--at must be tip or a block hash: %w", err)
	}
	return model.AtPoint{BlockHash: &hash}, nil
}

func newFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func usageError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrUsage, fmt.Sprintf(format, args...))
}

func Usage() string {
	return strings.TrimSpace(`
clickout utxo TX_HASH#INDEX [--at tip|BLOCK_HASH]
clickout tx TX_HASH
clickout address ADDRESS --state current|history [--limit N] [--cursor C]
clickout datum DATUM_HASH
clickout redeemers TX_HASH
clickout metadata TX_HASH
clickout withdrawals TX_HASH
clickout trace --direction forward|reverse \
  (--utxo TX_HASH#INDEX|--tx TX_HASH|--address ADDRESS) \
  [--max-depth N] [--max-nodes N] [--asset ada|POLICY.NAME] --format jsonl
`)
}

func ParseUint32(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	return uint32(parsed), err
}
