package syncer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"clicksync/internal/n2n"
)

type CandidateSource interface {
	// IntersectionCandidates returns only locally committed/adopted points in
	// strict newest-to-oldest order. Origin may be present only as the final
	// element; the supervisor appends it when omitted.
	IntersectionCandidates(context.Context) ([]n2n.ChainPoint, error)
}

type Observer interface {
	Observe(context.Context, Observation) error
}

type Handler interface {
	Reconcile(context.Context, n2n.ChainPoint, SourceEvidence) (CommitOutcome, error)
	RollForward(context.Context, lcommon.Block, chainsync.Tip, SourceEvidence) (CommitOutcome, error)
	RollbackObserved(context.Context, n2n.ChainPoint, chainsync.Tip) error
	RollBackward(context.Context, n2n.ChainPoint, chainsync.Tip, RollbackEvidence) (CommitOutcome, error)
	EndAttempt(context.Context, AttemptEnd) (CommitOutcome, error)
}

type CommitOutcome struct {
	// Accepted means a roll-forward was either staged in the bounded in-memory
	// microbatch or included in a committed prefix.
	Accepted bool
	// Committed is true once the authoritative adoption/rollback event exists,
	// even if a later manifest-cache or observation update failed.
	Committed bool
	// CommittedBlocks counts the ordered roll-forward prefix made visible by
	// adoption events. It may be greater than one for a physical microbatch,
	// including a retained prefix finalized while handling RollBackward.
	CommittedBlocks uint64
	// LastCommittedPoint/Tip are required whenever CommittedBlocks is non-zero
	// from RollForward, RollBackward, or EndAttempt. They identify the exact
	// typed tail and its associated peer tip.
	LastCommittedPoint *n2n.ChainPoint
	LastCommittedTip   *chainsync.Tip
}

type PeerEvidence struct {
	Peer       n2n.Peer
	Tip        chainsync.Tip
	N2NVersion uint16
}

type SourceEvidence struct {
	Primary           PeerEvidence
	Checkpoint        n2n.ChainPoint
	CheckpointMembers []PeerEvidence
}

type RollbackEvidence struct {
	Source        SourceEvidence
	Target        n2n.ChainPoint
	BranchTip     chainsync.Tip
	Confirmations []RollbackConfirmation
}

// RollbackConfirmation retains the one negotiated session identity that
// proved both exact singleton memberships for one operator.
type RollbackConfirmation struct {
	Target     n2n.ChainPoint
	BranchTip  chainsync.Tip
	Membership PeerEvidence
	Method     RollbackProofMethod
}

type RollbackProofMethod string

const (
	// RollbackProofFollowBlockFetch identifies the primary Follow session:
	// n2n resolved the rollback target with validation-enabled exact
	// BlockFetch before delivering the target metadata and branch tip in one
	// callback.
	RollbackProofFollowBlockFetch RollbackProofMethod = "follow_block_fetch"
	// RollbackProofPairedSingleton identifies one fresh corroborator session
	// that ran singleton ChainSync membership for target, then branch tip.
	RollbackProofPairedSingleton RollbackProofMethod = "paired_singleton"
)

type RollbackReplayRequiredError struct {
	ObservedTarget n2n.ChainPoint
	DurableTip     n2n.ChainPoint
}

func (err *RollbackReplayRequiredError) Error() string {
	return fmt.Sprintf(
		"rollback target %d:%x#%d was not durable; reconnect from %d:%x#%d",
		err.ObservedTarget.Point.Slot,
		err.ObservedTarget.Point.Hash,
		err.ObservedTarget.BlockNumber,
		err.DurableTip.Point.Slot,
		err.DurableTip.Point.Hash,
		err.DurableTip.BlockNumber,
	)
}

type AttemptEnd struct {
	Source SourceEvidence
	Cause  string
}

type Transport interface {
	// Probe must open a fresh connection, so every attempt performs DNS
	// resolution again. Accepted means the exact singleton point is on the
	// peer's currently selected ChainSync chain.
	Probe(context.Context, n2n.Peer, pcommon.Point) (ProbeResult, error)
	// ProbeRollback proves target and branch membership on one fresh
	// negotiated session, returning one actual connection identity and tip.
	ProbeRollback(
		context.Context,
		n2n.Peer,
		pcommon.Point,
		pcommon.Point,
	) (RollbackProbeResult, error)
	Follow(context.Context, n2n.Peer, []n2n.ChainPoint, n2n.Handler) error
}

type TransportError struct {
	Err error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("retryable transport failure: %v", e.Err)
}

func (e *TransportError) Unwrap() error {
	return e.Err
}

func RetryableTransportError(err error) error {
	if err == nil {
		return nil
	}
	return &TransportError{Err: err}
}

type ProbeResult struct {
	Accepted   bool
	Tip        chainsync.Tip
	N2NVersion uint16
	Address    string
}

type RollbackProbeResult struct {
	TargetAccepted bool
	BranchAccepted bool
	Tip            chainsync.Tip
	N2NVersion     uint16
	Address        string
}

type CorroborationUnavailableError struct {
	Err error
}

func (err *CorroborationUnavailableError) Error() string {
	return fmt.Sprintf("checkpoint corroboration unavailable: %v", err.Err)
}

func (err *CorroborationUnavailableError) Unwrap() error {
	return err.Err
}

type CheckpointCorroborationUnavailableError struct {
	Checkpoint n2n.ChainPoint
	Confirmed  int
	Required   int
	Err        error
}

func (err *CheckpointCorroborationUnavailableError) Error() string {
	return fmt.Sprintf(
		"committed checkpoint corroboration unavailable at %d:%x#%d: got %d of %d operators: %v",
		err.Checkpoint.Point.Slot,
		err.Checkpoint.Point.Hash,
		err.Checkpoint.BlockNumber,
		err.Confirmed,
		err.Required,
		err.Err,
	)
}

func (err *CheckpointCorroborationUnavailableError) Unwrap() error {
	return err.Err
}

type Observation struct {
	Kind                  string
	Peer                  n2n.Peer
	Checkpoint            pcommon.Point
	CheckpointBlockNumber uint64
	CheckpointIsByronEBB  *bool
	Tip                   chainsync.Tip
	N2NVersion            uint16
	SelectedBodySource    bool
	Result                string
	Reason                string
	ObservedAt            time.Time
}

type Config struct {
	Peers                 []n2n.Peer
	Corroboration         int
	AllowOrigin           bool
	InitialBackoff        time.Duration
	MaxBackoff            time.Duration
	RollbackConfirmations int
	CheckpointEveryBlocks uint64
	FinalizeTimeout       time.Duration
}

type Supervisor struct {
	config     Config
	candidates CandidateSource
	handler    Handler
	observer   Observer
	transport  Transport
	wait       func(context.Context, time.Duration) error
	now        func() time.Time
}

func New(
	config Config,
	candidates CandidateSource,
	handler Handler,
	observer Observer,
	transport Transport,
) (*Supervisor, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if candidates == nil {
		return nil, errors.New("nil intersection candidate source")
	}
	if handler == nil {
		return nil, errors.New("nil publication handler")
	}
	if observer == nil {
		return nil, errors.New("nil peer observation sink")
	}
	if transport == nil {
		return nil, errors.New("nil N2N transport")
	}
	return &Supervisor{
		config:     config,
		candidates: candidates,
		handler:    handler,
		observer:   observer,
		transport:  transport,
		wait:       waitContext,
		now:        time.Now,
	}, nil
}

func validateConfig(config Config) error {
	if len(config.Peers) < 2 {
		return errors.New("at least two peers are required")
	}
	if config.Corroboration < 2 {
		return errors.New("corroboration must require at least two independent operators")
	}
	operators := make(map[string]struct{}, len(config.Peers))
	addresses := make(map[string]struct{}, len(config.Peers))
	for _, peer := range config.Peers {
		if peer.Host == "" || peer.Operator == "" {
			return errors.New("every peer requires a host and independent operator label")
		}
		hostKey := strings.ToLower(peer.Host)
		if _, duplicate := addresses[hostKey]; duplicate {
			return fmt.Errorf("duplicate peer host %q", peer.Host)
		}
		addresses[hostKey] = struct{}{}
		operatorKey := strings.ToLower(peer.Operator)
		if _, duplicate := operators[operatorKey]; duplicate {
			return fmt.Errorf(
				"duplicate independently labeled operator %q",
				peer.Operator,
			)
		}
		operators[operatorKey] = struct{}{}
	}
	if config.Corroboration > len(operators) {
		return fmt.Errorf(
			"corroboration %d exceeds %d independently labeled operators",
			config.Corroboration,
			len(operators),
		)
	}
	if config.RollbackConfirmations < 2 ||
		config.RollbackConfirmations > config.Corroboration {
		return errors.New("rollback confirmations must count 2..corroboration independent operators")
	}
	if config.InitialBackoff <= 0 || config.MaxBackoff < config.InitialBackoff {
		return errors.New("backoff must satisfy 0 < initial <= max")
	}
	if config.CheckpointEveryBlocks == 0 {
		return errors.New("checkpoint observation interval must be positive")
	}
	if config.FinalizeTimeout <= 0 {
		return errors.New("attempt finalization timeout must be positive")
	}
	return nil
}

func (s *Supervisor) Run(ctx context.Context) error {
	var (
		nextPrimary     int
		lastPrimaryHost string
		currentBackoff  = s.config.InitialBackoff
		committedBlocks uint64
		quarantined     = make(map[string]struct{})
		rangeFailures   = make(rangeRetryState)
	)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		candidates, err := s.candidates.IntersectionCandidates(ctx)
		if err != nil {
			return fmt.Errorf("load committed intersection candidates: %w", err)
		}
		checkpoint, agreeing, err := s.corroborate(ctx, candidates, quarantined)
		if err != nil {
			var unavailable *CorroborationUnavailableError
			if !errors.As(err, &unavailable) {
				return err
			}
			if waitErr := s.wait(ctx, currentBackoff); waitErr != nil {
				return waitErr
			}
			currentBackoff = nextBackoff(currentBackoff, s.config.MaxBackoff)
			continue
		}
		primaryEvidence := agreeing[nextPrimary%len(agreeing)]
		primary := primaryEvidence.Peer
		nextPrimary++
		sourceReason := "reconnect"
		switch {
		case lastPrimaryHost == "":
			sourceReason = "initial selection"
		case primary.Host != lastPrimaryHost:
			sourceReason = "peer rotation"
		}
		attempt := &attemptHandler{
			supervisor: s,
			evidence: SourceEvidence{
				Primary:           primaryEvidence,
				Checkpoint:        cloneChainPoint(checkpoint),
				CheckpointMembers: cloneAgreement(agreeing),
			},
			delegate:        s.handler,
			committedBlocks: &committedBlocks,
			quarantined:     quarantined,
			sourceReason:    sourceReason,
		}
		err = s.transport.Follow(
			ctx,
			primary,
			[]n2n.ChainPoint{checkpoint},
			attempt,
		)
		endCause := classifyAttemptEnd(ctx, err, attempt.terminalErr)
		finishErr := attempt.finish(ctx, endCause)
		err = combineAttemptErrors(err, finishErr)
		if attempt.started {
			lastPrimaryHost = primary.Host
		}
		if attempt.terminalErr != nil {
			return attempt.terminalErr
		}
		if err == nil {
			err = errors.New("N2N follow returned without a stop reason")
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if attempt.committed {
			rangeFailures.clear(primary.Operator)
		}
		var peerData *n2n.PeerDataViolation
		if errors.As(err, &peerData) {
			if observeErr := s.observeFollowFailure(
				ctx,
				attempt,
				primaryEvidence,
				"peer_data_violation",
				"quarantined",
				err.Error(),
			); observeErr != nil {
				return observeErr
			}
			quarantined[primary.Operator] = struct{}{}
			if err := s.requireRemainingOperators(quarantined); err != nil {
				return fmt.Errorf("%w: %v", err, peerData)
			}
			currentBackoff = s.config.InitialBackoff
			continue
		}
		var rangeUnavailable *n2n.RangeUnavailable
		if errors.As(err, &rangeUnavailable) {
			key := unavailableRangeKey(primary, rangeUnavailable)
			repeated := rangeFailures.repeated(primary.Operator, key)
			result := "unavailable"
			reason := "range unavailable; reconnect and re-intersect once"
			if repeated {
				result = "quarantined"
				reason = "exact source/range was unavailable after one reconnect"
			}
			if observeErr := s.observeFollowFailure(
				ctx,
				attempt,
				primaryEvidence,
				"range_unavailable",
				result,
				reason+": "+err.Error(),
			); observeErr != nil {
				return observeErr
			}
			if repeated {
				quarantined[primary.Operator] = struct{}{}
				if err := s.requireRemainingOperators(quarantined); err != nil {
					return fmt.Errorf("%w: %v", err, rangeUnavailable)
				}
			}
			currentBackoff = s.config.InitialBackoff
			continue
		}
		var transportErr *TransportError
		if !errors.As(err, &transportErr) {
			return fmt.Errorf("unclassified N2N follow failure is terminal: %w", err)
		}
		if attempt.committed {
			currentBackoff = s.config.InitialBackoff
		}
		if waitErr := s.wait(ctx, currentBackoff); waitErr != nil {
			return waitErr
		}
		currentBackoff = nextBackoff(currentBackoff, s.config.MaxBackoff)
	}
}

func (s *Supervisor) corroborate(
	ctx context.Context,
	candidates []n2n.ChainPoint,
	quarantined map[string]struct{},
) (n2n.ChainPoint, []PeerEvidence, error) {
	ordered, err := orderedCandidates(candidates, s.config.AllowOrigin)
	if err != nil {
		return n2n.ChainPoint{}, nil, err
	}
	for _, checkpoint := range ordered {
		byOperator := make(map[string]PeerEvidence, s.config.Corroboration)
		for _, peer := range s.config.Peers {
			if _, excluded := quarantined[peer.Operator]; excluded {
				continue
			}
			result, probeErr := s.transport.Probe(ctx, peer, checkpoint.Point)
			observedPeer := peer
			if result.Address != "" {
				observedPeer.Address = result.Address
			}
			observation := Observation{
				Kind:                  "checkpoint",
				Peer:                  observedPeer,
				Checkpoint:            checkpoint.Point,
				CheckpointBlockNumber: checkpoint.BlockNumber,
				CheckpointIsByronEBB:  checkpointByronEBB(checkpoint),
				ObservedAt:            s.now().UTC(),
			}
			peerQuarantined := false
			switch {
			case probeErr != nil:
				var peerData *n2n.PeerDataViolation
				var transportError *TransportError
				switch {
				case errors.As(probeErr, &peerData):
					observation.Kind = "disagreement"
					observation.Result = "quarantined"
					observation.Reason = "peer_data_violation: " + probeErr.Error()
					quarantined[peer.Operator] = struct{}{}
					peerQuarantined = true
				case errors.As(probeErr, &transportError):
					observation.Result = "unavailable"
					observation.Reason = probeErr.Error()
				default:
					observation.Result = "unavailable"
					observation.Reason = "terminal_probe_failure: " + probeErr.Error()
				}
			case !result.Accepted:
				observation.Kind = "disagreement"
				observation.Result = "disagreed"
				observation.Reason = "exact checkpoint is not on selected peer chain"
				observation.Tip = result.Tip
				observation.N2NVersion = result.N2NVersion
			default:
				observation.Tip = result.Tip
				observation.N2NVersion = result.N2NVersion
				if validationErr := validateAcceptedProbe(
					checkpoint,
					result,
				); validationErr != nil {
					observation.Kind = "disagreement"
					observation.Result = "quarantined"
					observation.Reason = "invalid_accepted_probe: " +
						validationErr.Error()
					quarantined[peer.Operator] = struct{}{}
					peerQuarantined = true
				} else {
					observation.Result = "agreed"
					acceptedPeer := peer
					acceptedPeer.Address = result.Address
					if _, exists := byOperator[peer.Operator]; !exists {
						byOperator[peer.Operator] = PeerEvidence{
							Peer:       acceptedPeer,
							Tip:        cloneTip(result.Tip),
							N2NVersion: result.N2NVersion,
						}
					}
				}
			}
			if err := s.observer.Observe(ctx, observation); err != nil {
				return n2n.ChainPoint{}, nil, fmt.Errorf("record checkpoint observation: %w", err)
			}
			if probeErr != nil {
				var peerData *n2n.PeerDataViolation
				var transportError *TransportError
				switch {
				case peerQuarantined && errors.As(probeErr, &peerData):
					if err := s.requireRemainingOperators(quarantined); err != nil {
						return n2n.ChainPoint{}, nil, fmt.Errorf("%w: %v", err, peerData)
					}
				case errors.As(probeErr, &transportError):
				default:
					return n2n.ChainPoint{}, nil, fmt.Errorf(
						"unclassified N2N probe failure is terminal: %w",
						probeErr,
					)
				}
			}
			if peerQuarantined && probeErr == nil {
				if err := s.requireRemainingOperators(
					quarantined,
				); err != nil {
					return n2n.ChainPoint{}, nil, err
				}
			}
		}
		if len(byOperator) < s.config.Corroboration {
			continue
		}
		agreeing := make([]PeerEvidence, 0, len(byOperator))
		for _, peer := range s.config.Peers {
			accepted, ok := byOperator[peer.Operator]
			if ok && accepted.Peer.Host == peer.Host {
				agreeing = append(agreeing, accepted)
			}
		}
		return checkpoint, agreeing, nil
	}
	return n2n.ChainPoint{}, nil, &CorroborationUnavailableError{
		Err: errors.New("no checkpoint corroborated by independent peers"),
	}
}

func (s *Supervisor) observeFollowFailure(
	ctx context.Context,
	attempt *attemptHandler,
	fallback PeerEvidence,
	kind string,
	result string,
	reason string,
) error {
	evidence := fallback
	if attempt.started {
		evidence = attempt.evidence.Primary
	}
	if err := s.observer.Observe(ctx, Observation{
		Kind:                  "disagreement",
		Peer:                  evidence.Peer,
		Checkpoint:            clonePoint(attempt.evidence.Checkpoint.Point),
		CheckpointBlockNumber: attempt.evidence.Checkpoint.BlockNumber,
		CheckpointIsByronEBB:  checkpointByronEBB(attempt.evidence.Checkpoint),
		Tip:                   cloneTip(evidence.Tip),
		N2NVersion:            evidence.N2NVersion,
		SelectedBodySource:    false,
		Result:                result,
		Reason:                kind + ": " + reason,
		ObservedAt:            s.now().UTC(),
	}); err != nil {
		return fmt.Errorf("record %s observation: %w", kind, err)
	}
	return nil
}

func (s *Supervisor) requireRemainingOperators(
	quarantined map[string]struct{},
) error {
	operators := make(map[string]struct{}, len(s.config.Peers))
	for _, peer := range s.config.Peers {
		if _, excluded := quarantined[peer.Operator]; !excluded {
			operators[peer.Operator] = struct{}{}
		}
	}
	if len(operators) < s.config.Corroboration {
		return fmt.Errorf(
			"peer quarantine leaves %d independent operators, need %d",
			len(operators),
			s.config.Corroboration,
		)
	}
	return nil
}

func unavailableRangeKey(peer n2n.Peer, value *n2n.RangeUnavailable) string {
	return fmt.Sprintf(
		"%s\x00%d:%x\x00%d:%x",
		peer.Operator,
		value.Start.Slot,
		value.Start.Hash,
		value.End.Slot,
		value.End.Hash,
	)
}

type rangeRetryState map[string]string

func (state rangeRetryState) repeated(operator, key string) bool {
	previous, found := state[operator]
	state[operator] = key
	return found && previous == key
}

func (state rangeRetryState) clear(operator string) {
	delete(state, operator)
}

type attemptHandler struct {
	supervisor      *Supervisor
	evidence        SourceEvidence
	delegate        Handler
	committedBlocks *uint64
	quarantined     map[string]struct{}
	committed       bool
	started         bool
	sourceReason    string
	terminalErr     error
}

func (h *attemptHandler) Reconcile(
	ctx context.Context,
	point n2n.ChainPoint,
	peer n2n.Peer,
) error {
	if !chainPointsEqual(point, h.evidence.Checkpoint) {
		return h.terminal(fmt.Errorf(
			"preflight selected uncorroborated intersection %d:%x#%d",
			point.Point.Slot,
			point.Point.Hash,
			point.BlockNumber,
		))
	}
	if peer.Host != h.evidence.Primary.Peer.Host ||
		peer.Operator != h.evidence.Primary.Peer.Operator {
		return h.terminal(errors.New("Follow callback source differs from selected peer"))
	}
	if peer.Address == "" || peer.N2NVersion == 0 || peer.Tip == nil {
		return h.terminal(errors.New("Follow callback omitted actual address, version, or tip"))
	}
	// Probe and Follow are separate connections. Replace the primary's probe
	// address/version with the actual Follow connection provenance before any
	// publication callback or selected-source observation.
	h.evidence.Primary = PeerEvidence{
		Peer:       peer,
		Tip:        cloneTip(*peer.Tip),
		N2NVersion: peer.N2NVersion,
	}
	for index := range h.evidence.CheckpointMembers {
		if h.evidence.CheckpointMembers[index].Peer.Operator == peer.Operator {
			h.evidence.CheckpointMembers[index] = h.evidence.Primary
			break
		}
	}
	if err := h.supervisor.observer.Observe(ctx, Observation{
		Kind:                  "source_change",
		Peer:                  peer,
		Checkpoint:            clonePoint(point.Point),
		CheckpointBlockNumber: point.BlockNumber,
		CheckpointIsByronEBB:  checkpointByronEBB(point),
		Tip:                   cloneTip(h.evidence.Primary.Tip),
		N2NVersion:            peer.N2NVersion,
		SelectedBodySource:    true,
		Result:                "agreed",
		Reason:                h.sourceReason,
		ObservedAt:            h.supervisor.now().UTC(),
	}); err != nil {
		return h.terminal(fmt.Errorf("record actual Follow source: %w", err))
	}
	h.started = true
	outcome, err := h.delegate.Reconcile(
		ctx,
		point,
		cloneSourceEvidence(h.evidence),
	)
	if outcome.Committed {
		h.committed = true
	}
	if err != nil {
		return h.terminal(fmt.Errorf(
			"reconciliation handler failed (committed=%t): %w",
			outcome.Committed,
			err,
		))
	}
	return nil
}

func (h *attemptHandler) RollForward(
	ctx context.Context,
	block lcommon.Block,
	tip chainsync.Tip,
	peer n2n.Peer,
) error {
	if err := h.validateCallbackPeer(peer); err != nil {
		return h.terminal(err)
	}
	h.evidence.Primary.Tip = cloneTip(tip)
	outcome, err := h.delegate.RollForward(
		ctx,
		block,
		tip,
		h.currentEvidence(),
	)
	if outcome.Committed != (outcome.CommittedBlocks > 0) {
		return h.terminal(errors.New(
			"roll-forward committed flag and committed-block count disagree",
		))
	}
	if outcome.CommittedBlocks > 0 && !outcome.Accepted && err == nil {
		return h.terminal(errors.New(
			"roll-forward committed a prefix without accepting the callback block or returning a terminal error",
		))
	}
	if outcome.CommittedBlocks > 0 {
		if tailErr := validateCommittedTail(outcome); tailErr != nil {
			return h.terminal(fmt.Errorf("roll-forward committed tail: %w", tailErr))
		}
		if orderErr := validateTailAtOrBeforeBlock(*outcome.LastCommittedPoint, block); orderErr != nil {
			return h.terminal(fmt.Errorf("roll-forward committed tail: %w", orderErr))
		}
		if !outcome.Accepted &&
			pointsEqual(outcome.LastCommittedPoint.Point, pcommon.NewPoint(
				block.SlotNumber(),
				block.Hash().Bytes(),
			)) {
			return h.terminal(errors.New(
				"unaccepted callback block was reported as the committed prefix tail",
			))
		}
	}
	if outcome.Committed {
		h.committed = true
	}
	previousCommitted := *h.committedBlocks
	if outcome.CommittedBlocks > 0 {
		if err := h.addCommittedBlocks(outcome.CommittedBlocks); err != nil {
			return h.terminal(err)
		}
	}
	if outcome.CommittedBlocks > 0 &&
		previousCommitted/h.supervisor.config.CheckpointEveryBlocks !=
			*h.committedBlocks/h.supervisor.config.CheckpointEveryBlocks {
		if err := h.confirmPublishedCheckpoint(
			ctx,
			outcome.LastCommittedPoint.Point,
			outcome.LastCommittedPoint.BlockNumber,
			outcome.LastCommittedPoint.IsByronEBB,
			*outcome.LastCommittedTip,
		); err != nil {
			return h.postCommitCorroborationFailure(
				"adoption committed before periodic corroboration failure",
				err,
			)
		}
	}
	if err != nil {
		return h.terminal(fmt.Errorf(
			"roll-forward handler failed (committed=%t, accepted=%t): %w",
			outcome.Committed,
			outcome.Accepted,
			err,
		))
	}
	if !outcome.Accepted {
		return h.terminal(errors.New("roll-forward handler returned without accepting the block"))
	}
	return nil
}

func (h *attemptHandler) RollBackward(
	ctx context.Context,
	point n2n.ChainPoint,
	tip chainsync.Tip,
	peer n2n.Peer,
) error {
	if err := h.validateCallbackPeer(peer); err != nil {
		return h.terminal(err)
	}
	if err := validateRollbackOrder(point, tip); err != nil {
		return h.terminal(err)
	}
	if err := h.delegate.RollbackObserved(ctx, point, tip); err != nil {
		return h.terminal(fmt.Errorf(
			"discard pending branch after rollback observation: %w",
			err,
		))
	}
	h.evidence.Primary.Tip = cloneTip(tip)
	confirmed := []RollbackConfirmation{{
		Target:     cloneChainPoint(point),
		BranchTip:  cloneTip(tip),
		Membership: clonePeerEvidence(h.evidence.Primary),
		Method:     RollbackProofFollowBlockFetch,
	}}
	confirmedOperators := map[string]struct{}{
		operatorKey(h.evidence.Primary.Peer.Operator): {},
	}
	confirmations := 1
	sawUnavailable := false
	for _, candidatePeer := range h.supervisor.config.Peers {
		key := operatorKey(candidatePeer.Operator)
		if _, duplicate := confirmedOperators[key]; duplicate {
			continue
		}
		if h.operatorQuarantined(candidatePeer.Operator) {
			continue
		}
		result, err := h.supervisor.transport.ProbeRollback(
			ctx,
			candidatePeer,
			point.Point,
			tip.Point,
		)
		observedPeer := candidatePeer
		if result.Address != "" {
			observedPeer.Address = result.Address
		}
		observation := Observation{
			Kind:                  "rollback",
			Peer:                  observedPeer,
			Checkpoint:            clonePoint(point.Point),
			CheckpointBlockNumber: point.BlockNumber,
			CheckpointIsByronEBB:  checkpointByronEBB(point),
			Tip:                   result.Tip,
			N2NVersion:            result.N2NVersion,
			ObservedAt:            h.supervisor.now().UTC(),
		}
		quarantine := false
		terminalProbeErr := error(nil)
		if err != nil {
			var peerData *n2n.PeerDataViolation
			var transportError *TransportError
			switch {
			case errors.Is(err, context.Canceled),
				errors.Is(err, context.DeadlineExceeded):
				observation.Result = "unavailable"
				observation.Reason = err.Error()
				terminalProbeErr = err
			case errors.As(err, &peerData):
				observation.Result = "quarantined"
				observation.Reason = "peer_data_violation: " + err.Error()
				quarantine = true
			case errors.As(err, &transportError):
				observation.Result = "unavailable"
				observation.Reason = err.Error()
				sawUnavailable = true
			default:
				observation.Result = "unavailable"
				observation.Reason = "terminal_probe_failure: " + err.Error()
				terminalProbeErr = fmt.Errorf(
					"unclassified rollback proof failure is terminal: %w",
					err,
				)
			}
		} else if !result.TargetAccepted {
			observation.Result = "quarantined"
			observation.Reason = "peer rejected the exact rollback target"
			quarantine = true
		} else if !result.BranchAccepted {
			observation.Result = "quarantined"
			observation.Reason = "peer rejected the exact rollback branch tip"
			quarantine = true
		} else if orderErr := validateRollbackProbeTip(
			point,
			tip,
			result.Tip,
		); orderErr != nil {
			observation.Result = "quarantined"
			observation.Reason = orderErr.Error()
			quarantine = true
		} else {
			observation.Result = "agreed"
			observation.Reason = "one session confirmed exact target and branch tip"
			confirmations++
			confirmedOperators[key] = struct{}{}
			confirmed = append(confirmed, RollbackConfirmation{
				Target:    cloneChainPoint(point),
				BranchTip: cloneTip(tip),
				Membership: PeerEvidence{
					Peer:       observedPeer,
					Tip:        cloneTip(result.Tip),
					N2NVersion: result.N2NVersion,
				},
				Method: RollbackProofPairedSingleton,
			})
		}
		if quarantine {
			h.quarantineOperator(candidatePeer.Operator)
		}
		if observeErr := h.supervisor.observer.Observe(ctx, observation); observeErr != nil {
			return fmt.Errorf("record rollback observation: %w", observeErr)
		}
		if terminalProbeErr != nil {
			if errors.Is(terminalProbeErr, context.Canceled) ||
				errors.Is(terminalProbeErr, context.DeadlineExceeded) {
				return terminalProbeErr
			}
			return h.terminal(terminalProbeErr)
		}
		if quarantine {
			if remainingErr := h.supervisor.requireRemainingOperators(
				h.quarantined,
			); remainingErr != nil {
				return h.terminal(remainingErr)
			}
		}
		if confirmations >= h.supervisor.config.RollbackConfirmations {
			outcome, err := h.delegate.RollBackward(ctx, point, tip, RollbackEvidence{
				Source:        h.currentEvidence(),
				Target:        cloneChainPoint(point),
				BranchTip:     cloneTip(tip),
				Confirmations: cloneRollbackConfirmations(confirmed),
			})
			if outcome.Committed {
				h.committed = true
			}
			var replay *RollbackReplayRequiredError
			if errors.As(err, &replay) {
				if !outcome.Committed ||
					outcome.CommittedBlocks != 0 ||
					outcome.LastCommittedPoint != nil ||
					outcome.LastCommittedTip != nil {
					return h.terminal(errors.New(
						"rollback replay request returned an invalid commit outcome",
					))
				}
				return RetryableTransportError(err)
			}
			if outcome.CommittedBlocks > 0 {
				if tailErr := validateCommittedTail(outcome); tailErr != nil {
					return h.terminal(fmt.Errorf("rollback committed prefix tail: %w", tailErr))
				}
				if !chainPointsEqual(*outcome.LastCommittedPoint, point) {
					return h.terminal(errors.New(
						"rollback retained-prefix tail differs from the exact rollback target",
					))
				}
				previousCommitted := *h.committedBlocks
				if addErr := h.addCommittedBlocks(outcome.CommittedBlocks); addErr != nil {
					return h.terminal(addErr)
				}
				if previousCommitted/h.supervisor.config.CheckpointEveryBlocks !=
					*h.committedBlocks/h.supervisor.config.CheckpointEveryBlocks {
					if corroborateErr := h.confirmPublishedCheckpoint(
						ctx,
						outcome.LastCommittedPoint.Point,
						outcome.LastCommittedPoint.BlockNumber,
						outcome.LastCommittedPoint.IsByronEBB,
						*outcome.LastCommittedTip,
					); corroborateErr != nil {
						return h.postCommitCorroborationFailure(
							"rollback prefix adoption committed before periodic corroboration failure",
							corroborateErr,
						)
					}
				}
			}
			if err != nil {
				return h.terminal(fmt.Errorf(
					"rollback handler failed (committed=%t): %w",
					outcome.Committed,
					err,
				))
			}
			if !outcome.Committed {
				return h.terminal(errors.New("rollback handler returned without committing"))
			}
			return nil
		}
	}
	if sawUnavailable &&
		h.remainingRollbackOperators() >=
			h.supervisor.config.RollbackConfirmations {
		return RetryableTransportError(fmt.Errorf(
			"rollback corroboration unavailable: got %d of %d required independent confirmations",
			confirmations,
			h.supervisor.config.RollbackConfirmations,
		))
	}
	return h.terminal(fmt.Errorf(
		"rollback quarantined: got %d of %d required independent confirmations",
		confirmations,
		h.supervisor.config.RollbackConfirmations,
	))
}

func (h *attemptHandler) confirmPublishedCheckpoint(
	ctx context.Context,
	checkpoint pcommon.Point,
	checkpointBlockNumber uint64,
	checkpointIsByronEBB bool,
	tip chainsync.Tip,
) error {
	confirmedOperators := map[string]struct{}{
		operatorKey(h.evidence.Primary.Peer.Operator): {},
	}
	if err := h.supervisor.observer.Observe(ctx, Observation{
		Kind:                  "checkpoint",
		Peer:                  h.evidence.Primary.Peer,
		Checkpoint:            clonePoint(checkpoint),
		CheckpointBlockNumber: checkpointBlockNumber,
		CheckpointIsByronEBB:  boolPointer(checkpointIsByronEBB),
		Tip:                   cloneTip(tip),
		N2NVersion:            h.evidence.Primary.N2NVersion,
		SelectedBodySource:    true,
		Result:                "agreed",
		Reason:                "periodic primary checkpoint",
		ObservedAt:            h.supervisor.now().UTC(),
	}); err != nil {
		return fmt.Errorf("record primary checkpoint: %w", err)
	}
	sawUnavailable := false
	for _, candidatePeer := range h.supervisor.config.Peers {
		key := operatorKey(candidatePeer.Operator)
		if _, exists := confirmedOperators[key]; exists {
			continue
		}
		if h.operatorQuarantined(candidatePeer.Operator) {
			continue
		}
		result, probeErr := h.supervisor.transport.Probe(ctx, candidatePeer, checkpoint)
		observedPeer := candidatePeer
		if result.Address != "" {
			observedPeer.Address = result.Address
		}
		observation := Observation{
			Kind:                  "checkpoint",
			Peer:                  observedPeer,
			Checkpoint:            clonePoint(checkpoint),
			CheckpointBlockNumber: checkpointBlockNumber,
			CheckpointIsByronEBB:  boolPointer(checkpointIsByronEBB),
			Tip:                   cloneTip(result.Tip),
			N2NVersion:            result.N2NVersion,
			ObservedAt:            h.supervisor.now().UTC(),
		}
		quarantine := false
		terminalProbeErr := error(nil)
		switch {
		case probeErr != nil:
			var peerData *n2n.PeerDataViolation
			var transportError *TransportError
			switch {
			case errors.Is(probeErr, context.Canceled),
				errors.Is(probeErr, context.DeadlineExceeded):
				observation.Result = "unavailable"
				observation.Reason = probeErr.Error()
				terminalProbeErr = probeErr
			case errors.As(probeErr, &peerData):
				observation.Kind = "disagreement"
				observation.Result = "quarantined"
				observation.Reason = "peer_data_violation: " +
					probeErr.Error()
				quarantine = true
			case errors.As(probeErr, &transportError):
				observation.Result = "unavailable"
				observation.Reason = probeErr.Error()
				sawUnavailable = true
			default:
				observation.Result = "unavailable"
				observation.Reason = "terminal_probe_failure: " +
					probeErr.Error()
				terminalProbeErr = fmt.Errorf(
					"unclassified checkpoint probe failure is terminal: %w",
					probeErr,
				)
			}
		case !result.Accepted:
			observation.Kind = "disagreement"
			observation.Result = "quarantined"
			observation.Reason = "peer rejected committed checkpoint"
			quarantine = true
		case result.Tip.BlockNumber < checkpointBlockNumber ||
			result.Tip.Point.Slot < checkpoint.Slot:
			observation.Kind = "disagreement"
			observation.Result = "quarantined"
			observation.Reason = "peer tip precedes committed checkpoint"
			quarantine = true
		default:
			typedCheckpoint := n2n.NewChainPoint(
				checkpoint,
				checkpointBlockNumber,
			)
			if checkpointIsByronEBB {
				typedCheckpoint = n2n.NewByronEBBChainPoint(
					checkpoint,
					checkpointBlockNumber,
				)
			}
			if validationErr := validateAcceptedProbe(
				typedCheckpoint,
				result,
			); validationErr != nil {
				observation.Kind = "disagreement"
				observation.Result = "quarantined"
				observation.Reason = "invalid_accepted_probe: " +
					validationErr.Error()
				quarantine = true
			} else {
				observation.Result = "agreed"
				observation.Reason = "periodic independent checkpoint"
				confirmedOperators[key] = struct{}{}
			}
		}
		if quarantine {
			h.quarantineOperator(candidatePeer.Operator)
		}
		if err := h.supervisor.observer.Observe(ctx, observation); err != nil {
			return fmt.Errorf("record independent checkpoint: %w", err)
		}
		if terminalProbeErr != nil {
			return terminalProbeErr
		}
		if quarantine {
			if err := h.supervisor.requireRemainingOperators(
				h.quarantined,
			); err != nil {
				return err
			}
		}
	}
	if len(confirmedOperators) < h.supervisor.config.Corroboration {
		if sawUnavailable &&
			h.remainingRollbackOperators() >=
				h.supervisor.config.Corroboration {
			checkpoint := n2n.NewChainPoint(
				checkpoint,
				checkpointBlockNumber,
			)
			if checkpointIsByronEBB {
				checkpoint = n2n.NewByronEBBChainPoint(
					checkpoint.Point,
					checkpoint.BlockNumber,
				)
			}
			return RetryableTransportError(
				&CheckpointCorroborationUnavailableError{
					Checkpoint: checkpoint,
					Confirmed:  len(confirmedOperators),
					Required:   h.supervisor.config.Corroboration,
					Err: errors.New(
						"one or more independent operators were transiently unavailable",
					),
				},
			)
		}
		return fmt.Errorf(
			"checkpoint quarantined: got %d of %d independent operators",
			len(confirmedOperators),
			h.supervisor.config.Corroboration,
		)
	}
	return nil
}

func (h *attemptHandler) postCommitCorroborationFailure(
	prefix string,
	err error,
) error {
	wrapped := fmt.Errorf("%s: %w", prefix, err)
	var transportErr *TransportError
	if errors.As(err, &transportErr) {
		return wrapped
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return RetryableTransportError(wrapped)
	}
	return h.terminal(wrapped)
}

func combineAttemptErrors(followErr, finishErr error) error {
	switch {
	case finishErr == nil:
		return followErr
	case followErr == nil:
		return finishErr
	case errorIsPeerDataViolation(followErr),
		errorIsRangeUnavailable(followErr):
		// Preserve the primary source evidence for quarantine/range policy,
		// while retaining the typed post-commit corroboration requirement.
		return errors.Join(followErr, finishErr)
	}
	var followTransport *TransportError
	if errors.As(followErr, &followTransport) {
		return errors.Join(followErr, finishErr)
	}
	// An unclassified Follow error is more severe than a retryable finalizer
	// checkpoint outage and must keep its terminal precedence.
	return followErr
}

func (h *attemptHandler) terminal(err error) error {
	if err == nil {
		return nil
	}
	if h.terminalErr == nil {
		h.terminalErr = err
	}
	return err
}

func (h *attemptHandler) validateCallbackPeer(peer n2n.Peer) error {
	expected := h.evidence.Primary.Peer
	if peer.Host != expected.Host ||
		peer.Operator != expected.Operator ||
		peer.Address != expected.Address ||
		peer.N2NVersion != expected.N2NVersion {
		return errors.New("ChainSync callback provenance differs from actual Follow source")
	}
	return nil
}

func (h *attemptHandler) finish(ctx context.Context, cause string) error {
	finalizeCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		h.supervisor.config.FinalizeTimeout,
	)
	defer cancel()
	outcome, err := h.delegate.EndAttempt(finalizeCtx, AttemptEnd{
		Source: h.currentEvidence(),
		Cause:  cause,
	})
	if outcome.CommittedBlocks > 0 && !outcome.Committed {
		return h.terminal(errors.New("attempt finalizer committed blocks without committed outcome"))
	}
	if outcome.Committed {
		h.committed = true
	}
	if outcome.CommittedBlocks > 0 {
		if tailErr := validateCommittedTail(outcome); tailErr != nil {
			return h.terminal(fmt.Errorf("attempt finalizer committed tail: %w", tailErr))
		}
		previousCommitted := *h.committedBlocks
		if addErr := h.addCommittedBlocks(outcome.CommittedBlocks); addErr != nil {
			return h.terminal(addErr)
		}
		if previousCommitted/h.supervisor.config.CheckpointEveryBlocks !=
			*h.committedBlocks/h.supervisor.config.CheckpointEveryBlocks {
			if corroborateErr := h.confirmPublishedCheckpoint(
				finalizeCtx,
				outcome.LastCommittedPoint.Point,
				outcome.LastCommittedPoint.BlockNumber,
				outcome.LastCommittedPoint.IsByronEBB,
				*outcome.LastCommittedTip,
			); corroborateErr != nil {
				return h.postCommitCorroborationFailure(
					"finalized adoption committed before periodic corroboration failure",
					corroborateErr,
				)
			}
		}
	}
	if err != nil {
		return h.terminal(fmt.Errorf(
			"attempt finalizer failed (committed=%t): %w",
			outcome.Committed,
			err,
		))
	}
	return nil
}

func (h *attemptHandler) currentEvidence() SourceEvidence {
	return cloneSourceEvidence(h.evidence)
}

func (h *attemptHandler) addCommittedBlocks(count uint64) error {
	if count == 0 {
		return nil
	}
	if math.MaxUint64-*h.committedBlocks < count {
		return errors.New("committed block counter exhausted")
	}
	*h.committedBlocks += count
	return nil
}

func validateCommittedTail(outcome CommitOutcome) error {
	if !outcome.Committed {
		return errors.New("committed-block count has no committed outcome")
	}
	if outcome.LastCommittedPoint == nil || outcome.LastCommittedTip == nil {
		return errors.New("missing exact last committed ChainPoint or peer tip")
	}
	point := outcome.LastCommittedPoint
	if point.Point.Slot == 0 && len(point.Point.Hash) == 0 {
		return errors.New("roll-forward committed tail cannot be Origin")
	}
	if len(point.Point.Hash) != 32 {
		return fmt.Errorf(
			"last committed point at slot %d has %d-byte hash",
			point.Point.Slot,
			len(point.Point.Hash),
		)
	}
	tip := outcome.LastCommittedTip
	if tip.BlockNumber < point.BlockNumber ||
		tip.Point.Slot == 0 && len(tip.Point.Hash) == 0 ||
		tip.Point.Slot < point.Point.Slot {
		return fmt.Errorf(
			"associated peer tip %d:%x#%d precedes committed tail %d:%x#%d",
			tip.Point.Slot,
			tip.Point.Hash,
			tip.BlockNumber,
			point.Point.Slot,
			point.Point.Hash,
			point.BlockNumber,
		)
	}
	return nil
}

func validateTailAtOrBeforeBlock(
	tail n2n.ChainPoint,
	block lcommon.Block,
) error {
	current := n2n.NewChainPoint(
		pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes()),
		block.BlockNumber(),
	)
	if isByronEBBBlock(block) {
		current = n2n.NewByronEBBChainPoint(current.Point, current.BlockNumber)
	}
	if pointsEqual(tail.Point, current.Point) {
		if !chainPointsEqual(tail, current) {
			return fmt.Errorf(
				"committed tail metadata %#v differs from callback block %#v",
				tail,
				current,
			)
		}
		return nil
	}
	if tail.BlockNumber > current.BlockNumber ||
		tail.Point.Slot > current.Point.Slot {
		return errors.New("committed tail is after the accepted callback block")
	}
	if tail.BlockNumber == current.BlockNumber &&
		(!current.IsByronEBB ||
			tail.IsByronEBB ||
			tail.Point.Slot >= current.Point.Slot) {
		return errors.New("same-height committed tail is not the predecessor of a Byron EBB callback")
	}
	if tail.Point.Slot == current.Point.Slot &&
		(!tail.IsByronEBB ||
			tail.BlockNumber == math.MaxUint64 ||
			tail.BlockNumber+1 != current.BlockNumber) {
		return errors.New("same-slot committed tail is not a Byron EBB predecessor")
	}
	return nil
}

func classifyAttemptEnd(ctx context.Context, followErr, terminalErr error) string {
	switch {
	case terminalErr != nil:
		return "terminal_handler_failure"
	case ctx.Err() != nil:
		return "context_shutdown"
	case errorIsPeerDataViolation(followErr):
		return "peer_data_violation"
	case errorIsRangeUnavailable(followErr):
		return "range_unavailable"
	default:
		var transportErr *TransportError
		if errors.As(followErr, &transportErr) {
			return "transport_failure"
		}
		return "unknown_follow_failure"
	}
}

func errorIsPeerDataViolation(err error) bool {
	var target *n2n.PeerDataViolation
	return errors.As(err, &target)
}

func errorIsRangeUnavailable(err error) bool {
	var target *n2n.RangeUnavailable
	return errors.As(err, &target)
}

func orderedCandidates(
	candidates []n2n.ChainPoint,
	allowOrigin bool,
) ([]n2n.ChainPoint, error) {
	ret := make([]n2n.ChainPoint, 0, len(candidates)+1)
	originPresent := false
	var previousSlot uint64
	var previousBlockNumber uint64
	for index, candidate := range candidates {
		point := candidate.Point
		if point.Slot == 0 && len(point.Hash) == 0 {
			if !allowOrigin {
				return nil, errors.New(
					"partial/intersection dataset cannot include Origin candidate",
				)
			}
			if index != len(candidates)-1 {
				return nil, errors.New("Origin intersection candidate must be last")
			}
			if candidate.BlockNumber != 0 {
				return nil, errors.New("Origin intersection cannot carry a block number")
			}
			if candidate.IsByronEBB {
				return nil, errors.New("Origin intersection cannot be a Byron EBB")
			}
			originPresent = true
			continue
		}
		if len(point.Hash) != 32 {
			return nil, fmt.Errorf(
				"intersection candidate at slot %d has %d-byte hash",
				point.Slot,
				len(point.Hash),
			)
		}
		if len(ret) > 0 {
			previous := ret[len(ret)-1]
			switch {
			case point.Slot > previousSlot:
				return nil, errors.New(
					"intersection candidates must be newest-to-oldest by slot",
				)
			case point.Slot == previousSlot:
				if previous.IsByronEBB ||
					!candidate.IsByronEBB ||
					previous.BlockNumber != candidate.BlockNumber+1 {
					return nil, errors.New(
						"equal-slot candidates must be a newer regular successor followed by its older Byron EBB predecessor",
					)
				}
			case candidate.BlockNumber > previousBlockNumber:
				return nil, errors.New(
					"intersection candidate block numbers cannot increase newest-to-oldest",
				)
			case candidate.BlockNumber == previousBlockNumber &&
				(!previous.IsByronEBB || candidate.IsByronEBB):
				return nil, errors.New(
					"equal-height candidates must be a Byron EBB followed newest-to-oldest by its regular predecessor",
				)
			}
		}
		if slices.ContainsFunc(ret, func(existing n2n.ChainPoint) bool {
			return pointsEqual(existing.Point, point)
		}) {
			return nil, errors.New("duplicate intersection candidate")
		}
		ret = append(ret, cloneChainPoint(candidate))
		previousSlot = point.Slot
		previousBlockNumber = candidate.BlockNumber
	}
	if allowOrigin && !originPresent {
		ret = append(ret, n2n.NewChainPointOrigin())
	}
	return ret, nil
}

func pointsEqual(left, right pcommon.Point) bool {
	return left.Slot == right.Slot && bytes.Equal(left.Hash, right.Hash)
}

func clonePoint(point pcommon.Point) pcommon.Point {
	return pcommon.NewPoint(point.Slot, bytes.Clone(point.Hash))
}

func cloneChainPoint(point n2n.ChainPoint) n2n.ChainPoint {
	if point.IsByronEBB {
		return n2n.NewByronEBBChainPoint(point.Point, point.BlockNumber)
	}
	return n2n.NewChainPoint(point.Point, point.BlockNumber)
}

func checkpointByronEBB(point n2n.ChainPoint) *bool {
	if point.Point.Slot == 0 && len(point.Point.Hash) == 0 {
		return nil
	}
	return boolPointer(point.IsByronEBB)
}

func boolPointer(value bool) *bool {
	ret := value
	return &ret
}

func isByronEBBBlock(block lcommon.Block) bool {
	if block == nil {
		return false
	}
	_, ok := block.Header().(*byron.ByronEpochBoundaryBlockHeader)
	return ok
}

func chainPointsEqual(left, right n2n.ChainPoint) bool {
	return left.BlockNumber == right.BlockNumber &&
		left.IsByronEBB == right.IsByronEBB &&
		pointsEqual(left.Point, right.Point)
}

func cloneTip(tip chainsync.Tip) chainsync.Tip {
	tip.Point = clonePoint(tip.Point)
	return tip
}

func cloneAgreement(source []PeerEvidence) []PeerEvidence {
	ret := make([]PeerEvidence, len(source))
	for index, evidence := range source {
		ret[index] = clonePeerEvidence(evidence)
	}
	return ret
}

func clonePeerEvidence(source PeerEvidence) PeerEvidence {
	source.Tip = cloneTip(source.Tip)
	return source
}

func cloneRollbackConfirmations(
	source []RollbackConfirmation,
) []RollbackConfirmation {
	ret := make([]RollbackConfirmation, len(source))
	for index, confirmation := range source {
		ret[index] = RollbackConfirmation{
			Target:     cloneChainPoint(confirmation.Target),
			BranchTip:  cloneTip(confirmation.BranchTip),
			Membership: clonePeerEvidence(confirmation.Membership),
			Method:     confirmation.Method,
		}
	}
	return ret
}

func cloneSourceEvidence(source SourceEvidence) SourceEvidence {
	source.Checkpoint = cloneChainPoint(source.Checkpoint)
	source.Primary.Tip = cloneTip(source.Primary.Tip)
	source.CheckpointMembers = cloneAgreement(source.CheckpointMembers)
	return source
}

func validateRollbackOrder(
	target n2n.ChainPoint,
	branch chainsync.Tip,
) error {
	targetOrigin := isOriginPoint(target.Point)
	branchOrigin := isOriginPoint(branch.Point)
	if targetOrigin {
		if target.BlockNumber != 0 || target.IsByronEBB {
			return errors.New("Origin rollback target carries block metadata")
		}
	} else if len(target.Point.Hash) != 32 {
		return fmt.Errorf(
			"rollback target at slot %d has %d-byte hash",
			target.Point.Slot,
			len(target.Point.Hash),
		)
	}
	if branchOrigin {
		if !targetOrigin || branch.BlockNumber != 0 {
			return errors.New("rollback target is after an Origin branch tip")
		}
		return nil
	}
	if len(branch.Point.Hash) != 32 {
		return fmt.Errorf(
			"rollback branch tip at slot %d has %d-byte hash",
			branch.Point.Slot,
			len(branch.Point.Hash),
		)
	}
	if target.BlockNumber > branch.BlockNumber ||
		target.Point.Slot > branch.Point.Slot {
		return errors.New("rollback target is after the reported branch tip")
	}
	if pointsEqual(target.Point, branch.Point) &&
		target.BlockNumber != branch.BlockNumber {
		return errors.New(
			"rollback target and branch tip share a point but not block metadata",
		)
	}
	return nil
}

func validateRollbackProbeTip(
	target n2n.ChainPoint,
	branch chainsync.Tip,
	remote chainsync.Tip,
) error {
	if err := validateRollbackOrder(target, branch); err != nil {
		return err
	}
	if isOriginPoint(remote.Point) {
		if !isOriginPoint(branch.Point) {
			return errors.New(
				"rollback proof remote tip precedes the exact branch tip",
			)
		}
		return nil
	}
	if len(remote.Point.Hash) != 32 {
		return fmt.Errorf(
			"rollback proof remote tip at slot %d has %d-byte hash",
			remote.Point.Slot,
			len(remote.Point.Hash),
		)
	}
	if remote.BlockNumber < branch.BlockNumber ||
		remote.Point.Slot < branch.Point.Slot {
		return errors.New(
			"rollback proof remote tip precedes the exact branch tip",
		)
	}
	return nil
}

func validateAcceptedProbe(
	checkpoint n2n.ChainPoint,
	result ProbeResult,
) error {
	if !result.Accepted {
		return errors.New("probe did not accept the exact checkpoint")
	}
	if strings.TrimSpace(result.Address) == "" {
		return errors.New("accepted probe omitted actual remote address")
	}
	if result.N2NVersion < 7 || result.N2NVersion > 15 {
		return fmt.Errorf(
			"accepted probe reported unsupported N2N version %d",
			result.N2NVersion,
		)
	}
	if err := validateRollbackOrder(
		checkpoint,
		result.Tip,
	); err != nil {
		return fmt.Errorf(
			"accepted probe remote tip is incompatible: %w",
			err,
		)
	}
	return nil
}

func isOriginPoint(point pcommon.Point) bool {
	return point.Slot == 0 && len(point.Hash) == 0
}

func operatorKey(operator string) string {
	return strings.ToLower(strings.TrimSpace(operator))
}

func (h *attemptHandler) quarantineOperator(operator string) {
	if h.quarantined == nil {
		h.quarantined = make(map[string]struct{})
	}
	h.quarantined[operator] = struct{}{}
}

func (h *attemptHandler) operatorQuarantined(operator string) bool {
	key := operatorKey(operator)
	for quarantined := range h.quarantined {
		if operatorKey(quarantined) == key {
			return true
		}
	}
	return false
}

func (h *attemptHandler) remainingRollbackOperators() int {
	seen := make(map[string]struct{}, len(h.supervisor.config.Peers))
	for _, peer := range h.supervisor.config.Peers {
		key := operatorKey(peer.Operator)
		if key == "" || h.operatorQuarantined(peer.Operator) {
			continue
		}
		seen[key] = struct{}{}
	}
	return len(seen)
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > time.Duration(math.MaxInt64/2) {
		return maximum
	}
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
