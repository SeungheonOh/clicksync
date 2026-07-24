package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func authorityMembershipTestRow(
	eventSeq uint64,
	kind string,
) authorityPhysicalMembershipRow {
	hash := authorityFill32(0x51)
	row := authorityPhysicalMembershipRow{
		EventSeq:      eventSeq,
		PublicationID: 7,
		EventKind:     kind,
		BlockHash:     string(hash[:]),
		Slot:          100,
		BlockNumber:   10,
		WriterID:      uuid.UUID(authorityFill16(0x43)),
		RecordedAt: time.Date(
			2026, time.July, 23, 12, 0, 0, 123456000, time.UTC,
		),
	}
	switch kind {
	case "adoption":
		row.Active = true
	case "invalidation":
		rollbackID := uuid.UUID(authorityFill16(0x31))
		row.RollbackID = &rollbackID
	}
	return row
}

func TestAuthorityRollbackWalkSQLShape(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		query string
		wants []string
	}{
		"hash candidates": {
			query: authorityBlockHashCandidatesSQL,
			wants: []string{
				"FROM blocks",
				"PREWHERE block_hash = ?",
				"(NOT ? OR publication_id > ?)",
				"ORDER BY block_hash, publication_id",
				"LIMIT 9",
			},
		},
		"membership": {
			query: authorityPublicationMembershipSQL,
			wants: []string{
				"FROM chain_events",
				"PREWHERE publication_id = ?",
				"AND event_seq <= ?",
				"(NOT ? OR event_seq > ?)",
				"ORDER BY publication_id, event_seq, event_kind, rollback_id",
				"LIMIT 9",
			},
		},
	} {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, want := range test.wants {
				if !strings.Contains(test.query, want) {
					t.Fatalf("query lacks %q:\n%s", want, test.query)
				}
			}
			if strings.Contains(test.query, "clicksync.") ||
				strings.Contains(test.query, "OFFSET") ||
				strings.Contains(test.query, "argMax") {
				t.Fatalf("query is not raw/stable-key bounded:\n%s", test.query)
			}
		})
	}
}

func TestDecodeAuthorityBlockHashCandidatePageReplayBounds(t *testing.T) {
	t.Parallel()
	eight := []uint64{7, 7, 7, 7, 7, 7, 7, 7}
	if publicationID, found, err := decodeAuthorityBlockHashCandidatePage(
		append(eight, 8),
	); err != nil || !found || publicationID != 7 {
		t.Fatalf(
			"eight replays plus next-key sentinel rejected: id=%d found=%v err=%v",
			publicationID,
			found,
			err,
		)
	}
	if _, _, err := decodeAuthorityBlockHashCandidatePage(
		append(eight, 7),
	); err == nil {
		t.Fatal("ninth block candidate replay was accepted")
	}
	for name, rows := range map[string][]uint64{
		"zero":      {0},
		"unordered": {8, 7},
		"over page": {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	} {
		if _, _, err := decodeAuthorityBlockHashCandidatePage(rows); err == nil {
			t.Fatalf("%s candidate corruption was accepted", name)
		}
	}
}

func TestAuthorityLifecycleSourceTaxonomy(t *testing.T) {
	t.Parallel()
	if _, _, err := decodeAuthorityBlockHashCandidatePage(
		[]uint64{0},
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("candidate semantic error = %v", err)
	}
	if _, _, _, err := decodeAuthorityMembershipEventPage(
		[]authorityPhysicalMembershipRow{
			authorityMembershipTestRow(1, "other"),
		},
		7,
		1,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("membership semantic error = %v", err)
	}
	var lifecycle authorityMembershipLifecycle
	if err := lifecycle.observe(
		"invalidation",
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("lifecycle semantic error = %v", err)
	}
	if err := validateAuthorityHistoricalSynthetic(
		authorityRecord{},
		authorityPhysicalAdoptionRow{},
		authorityPhysicalBlockRow{},
		authorityPoint{},
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("synthetic semantic error = %v", err)
	}
	if err := validateAuthorityHistoricalInvalidation(
		authorityPhysicalMembershipRow{},
		authorityPoint{},
		authorityPoint{},
		authorityPhysicalRollbackRow{},
		false,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("invalidation semantic error = %v", err)
	}
}

func TestWalkAuthorityPublicationLifecyclePreservesDependencyErrors(t *testing.T) {
	t.Parallel()
	failures := []error{
		errors.New("injected infrastructure failure"),
		&ResourceLimitError{
			Phase: "injected",
			Cause: errors.New("injected resource failure"),
		},
	}
	for _, failure := range failures {
		failure := failure
		t.Run(failure.Error(), func(t *testing.T) {
			_, err := walkAuthorityPublicationLifecycle(
				context.Background(),
				authorityRecord{},
				1,
				authorityPhysicalBlockRow{PublicationID: 7},
				authorityPoint{},
				authorityPublicationLifecycleReaders{
					nextMembership: func(
						context.Context,
						uint64,
						uint64,
						bool,
						uint64,
					) (
						authorityPhysicalMembershipRow,
						authorityPoint,
						bool,
						error,
					) {
						return authorityPhysicalMembershipRow{},
							authorityPoint{},
							false,
							failure
					},
				},
			)
			if !errors.Is(err, failure) ||
				errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("dependency error = %v", err)
			}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := walkAuthorityPublicationLifecycle(
		ctx,
		authorityRecord{},
		1,
		authorityPhysicalBlockRow{PublicationID: 7},
		authorityPoint{},
		authorityPublicationLifecycleReaders{},
	); !errors.Is(err, context.Canceled) ||
		errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("context error = %v", err)
	}
}

func TestWalkAuthorityPublicationLifecycleTypesOwnedIdentityFailure(t *testing.T) {
	t.Parallel()
	membership := authorityMembershipTestRow(1, "adoption")
	_, err := walkAuthorityPublicationLifecycle(
		context.Background(),
		authorityRecord{},
		1,
		authorityPhysicalBlockRow{PublicationID: membership.PublicationID},
		authorityPoint{},
		authorityPublicationLifecycleReaders{
			nextMembership: func(
				context.Context,
				uint64,
				uint64,
				bool,
				uint64,
			) (
				authorityPhysicalMembershipRow,
				authorityPoint,
				bool,
				error,
			) {
				return membership, authorityPoint{}, true, nil
			},
			loadAdoption: func(
				context.Context,
				uint64,
			) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
				return authorityPhysicalAdoptionRow{},
					authorityPoint{},
					false,
					nil
			},
		},
	)
	if !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("owned lifecycle identity error = %v", err)
	}
}

func TestAuthorityRollbackDescendantTaxonomy(t *testing.T) {
	t.Parallel()
	to := authorityPoint{Origin: true}
	oldTip := authorityPoint{
		Slot:        10,
		Hash:        authorityFill32(0x62),
		BlockNumber: 2,
	}
	header := authorityPhysicalRollbackRow{
		Depth:          1,
		OldTipEventSeq: 5,
	}
	if _, err := walkAuthorityRollbackDescendants(
		context.Background(),
		authorityRecord{Start: authorityPoint{Origin: true}},
		authorityPhysicalRollbackRow{},
		to,
		oldTip,
		nil,
		nil,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("owned descendant semantic error = %v", err)
	}
	if err := validateAuthorityParentProgress(
		authorityActiveBlock{PublicationID: 2},
		authorityActiveBlock{PublicationID: 1},
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("parent-progress semantic error = %v", err)
	}

	failures := []error{
		errors.New("injected resolver failure"),
		&ResourceLimitError{
			Phase: "injected",
			Cause: errors.New("injected resolver resource failure"),
		},
	}
	for _, failure := range failures {
		failure := failure
		t.Run(failure.Error(), func(t *testing.T) {
			_, err := walkAuthorityRollbackDescendants(
				context.Background(),
				authorityRecord{Start: authorityPoint{Origin: true}},
				header,
				to,
				oldTip,
				func(
					context.Context,
					uint64,
					authorityHash,
				) (authorityActiveBlock, bool, error) {
					return authorityActiveBlock{}, false, failure
				},
				nil,
			)
			if !errors.Is(err, failure) ||
				errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("resolver dependency error = %v", err)
			}
		})
	}

	visitFailure := errors.New("injected visitor failure")
	if _, err := walkAuthorityRollbackDescendants(
		context.Background(),
		authorityRecord{Start: authorityPoint{Origin: true}},
		header,
		to,
		oldTip,
		func(
			context.Context,
			uint64,
			authorityHash,
		) (authorityActiveBlock, bool, error) {
			return authorityActiveBlock{
				PublicationID: 7,
				Point:         oldTip,
			}, true, nil
		},
		func(authorityRollbackDescendant) error {
			return visitFailure
		},
	); !errors.Is(err, visitFailure) ||
		errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("visitor dependency error = %v", err)
	}
}

func TestDecodeAuthorityMembershipEventPageReplayAndTieBounds(t *testing.T) {
	t.Parallel()
	row := authorityMembershipTestRow(1, "adoption")
	eight := make([]authorityPhysicalMembershipRow, 8)
	for index := range eight {
		eight[index] = row
	}
	next := authorityMembershipTestRow(2, "invalidation")
	decoded, point, found, err := decodeAuthorityMembershipEventPage(
		append(eight, next),
		7,
		2,
	)
	if err != nil || !found || decoded.EventSeq != 1 ||
		point.Hash != authorityFill32(0x51) {
		t.Fatalf(
			"complete replay group plus next-event sentinel rejected: %#v %#v %v %v",
			decoded,
			point,
			found,
			err,
		)
	}
	if _, _, _, err := decodeAuthorityMembershipEventPage(
		append(eight, row),
		7,
		2,
	); err == nil {
		t.Fatal("ninth membership replay was accepted")
	}
	tie := row
	tie.EventKind = "invalidation"
	tie.Active = false
	rollbackID := uuid.UUID(authorityFill16(0x31))
	tie.RollbackID = &rollbackID
	if _, _, _, err := decodeAuthorityMembershipEventPage(
		[]authorityPhysicalMembershipRow{row, tie},
		7,
		2,
	); err == nil {
		t.Fatal("same-event adoption/invalidation tie was accepted")
	}
	conflict := row
	conflict.Slot++
	if _, _, _, err := decodeAuthorityMembershipEventPage(
		[]authorityPhysicalMembershipRow{row, conflict},
		7,
		2,
	); err == nil {
		t.Fatal("same-event physical conflict was accepted")
	}
	if _, _, _, err := decodeAuthorityMembershipEventPage(
		[]authorityPhysicalMembershipRow{
			authorityMembershipTestRow(2, "adoption"),
			row,
		},
		7,
		2,
	); err == nil {
		t.Fatal("descending membership page was accepted")
	}
}

func TestDecodeAuthorityMembershipEventPageRejectsCorruptShape(t *testing.T) {
	t.Parallel()
	valid := authorityMembershipTestRow(1, "adoption")
	tests := map[string]func(*authorityPhysicalMembershipRow){
		"event zero": func(row *authorityPhysicalMembershipRow) {
			row.EventSeq = 0
		},
		"publication": func(row *authorityPhysicalMembershipRow) {
			row.PublicationID = 0
		},
		"writer": func(row *authorityPhysicalMembershipRow) {
			row.WriterID = uuid.Nil
		},
		"time": func(row *authorityPhysicalMembershipRow) {
			row.RecordedAt = row.RecordedAt.Add(time.Nanosecond)
		},
		"hash": func(row *authorityPhysicalMembershipRow) {
			row.BlockHash = string(make([]byte, 32))
		},
		"adoption active": func(row *authorityPhysicalMembershipRow) {
			row.Active = false
		},
		"adoption rollback": func(row *authorityPhysicalMembershipRow) {
			value := uuid.UUID(authorityFill16(0x31))
			row.RollbackID = &value
		},
		"kind": func(row *authorityPhysicalMembershipRow) {
			row.EventKind = "other"
		},
	}
	for name, corrupt := range tests {
		name, corrupt := name, corrupt
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			row := valid
			corrupt(&row)
			if _, _, _, err := decodeAuthorityMembershipEventPage(
				[]authorityPhysicalMembershipRow{row},
				7,
				1,
			); err == nil {
				t.Fatal("corrupt membership was accepted")
			}
		})
	}
}

func TestAuthorityMembershipLifecycle(t *testing.T) {
	t.Parallel()
	var empty authorityMembershipLifecycle
	if active, err := empty.active(); err != nil || active {
		t.Fatalf("empty crash remnant lifecycle = active %v, err %v", active, err)
	}
	var adopted authorityMembershipLifecycle
	if err := adopted.observe("adoption"); err != nil {
		t.Fatal(err)
	}
	if active, err := adopted.active(); err != nil || !active {
		t.Fatalf("adopted lifecycle = active %v, err %v", active, err)
	}
	var invalidated authorityMembershipLifecycle
	if err := invalidated.observe("adoption"); err != nil {
		t.Fatal(err)
	}
	if err := invalidated.observe("invalidation"); err != nil {
		t.Fatal(err)
	}
	if active, err := invalidated.active(); err != nil || active {
		t.Fatalf("invalidated lifecycle = active %v, err %v", active, err)
	}
	for name, kinds := range map[string][]string{
		"invalidation first":  {"invalidation"},
		"second adoption":     {"adoption", "adoption"},
		"reactivation":        {"adoption", "invalidation", "adoption"},
		"second invalidation": {"adoption", "invalidation", "invalidation"},
	} {
		var lifecycle authorityMembershipLifecycle
		var err error
		for _, kind := range kinds {
			err = lifecycle.observe(kind)
			if err != nil {
				break
			}
		}
		if err == nil {
			t.Fatalf("%s lifecycle corruption was accepted", name)
		}
	}
}

func authorityMembershipFromAdoption(
	adoption authorityPhysicalAdoptionRow,
) authorityPhysicalMembershipRow {
	return authorityPhysicalMembershipRow{
		EventSeq:      adoption.EventSeq,
		PublicationID: adoption.PublicationID,
		EventKind:     "adoption",
		Active:        adoption.Active,
		BlockHash:     adoption.BlockHash,
		Slot:          adoption.Slot,
		BlockNumber:   adoption.BlockNumber,
		IsByronEBB:    adoption.IsByronEBB,
		WriterID:      adoption.WriterID,
		RecordedAt:    adoption.RecordedAt,
	}
}

func TestWalkAuthorityPublicationLifecycleCurrentAdoption(t *testing.T) {
	t.Parallel()
	record, current, point, block := authorityCurrentAdoptionFixture()
	adoptionAt := func(eventSeq uint64) authorityPhysicalAdoptionRow {
		row := current
		row.EventSeq = eventSeq
		return row
	}
	membershipAt := func(eventSeq uint64, kind string) authorityPhysicalMembershipRow {
		row := authorityMembershipFromAdoption(adoptionAt(eventSeq))
		if kind == "invalidation" {
			row.EventKind = kind
			row.Active = false
			rollbackID := uuid.UUID(authorityFill16(byte(0x30 + eventSeq)))
			row.RollbackID = &rollbackID
		}
		return row
	}
	readersFor := func(
		events []authorityPhysicalMembershipRow,
	) authorityPublicationLifecycleReaders {
		return authorityPublicationLifecycleReaders{
			nextMembership: func(
				_ context.Context,
				publicationID uint64,
				snapshot uint64,
				cursorSet bool,
				cursor uint64,
			) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
				if publicationID != current.PublicationID ||
					snapshot != record.Physical.EventSeq {
					return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
						errors.New("wrong lifecycle stable key")
				}
				for _, event := range events {
					if !cursorSet || event.EventSeq > cursor {
						return event, point, true, nil
					}
				}
				return authorityPhysicalMembershipRow{}, authorityPoint{}, false, nil
			},
			loadAdoption: func(
				_ context.Context,
				eventSeq uint64,
			) (authorityPhysicalAdoptionRow, authorityPoint, bool, error) {
				for _, event := range events {
					if event.EventSeq == eventSeq &&
						event.EventKind == "adoption" {
						return adoptionAt(eventSeq), point, true, nil
					}
				}
				return authorityPhysicalAdoptionRow{}, authorityPoint{}, false, nil
			},
			loadRollback: func(
				_ context.Context,
				eventSeq uint64,
			) (
				authorityPhysicalRollbackRow,
				authorityPoint,
				authorityPoint,
				authorityHash,
				bool,
				error,
			) {
				for _, event := range events {
					if event.EventSeq != eventSeq ||
						event.EventKind != "invalidation" {
						continue
					}
					header := authorityRollbackArtifactTestRow(
						authorityFill32(0x61),
					)
					header.EventSeq = eventSeq
					header.RollbackID = *event.RollbackID
					header.WriterID = event.WriterID
					header.RecordedAt = event.RecordedAt
					return header, authorityPoint{}, authorityPoint{},
						authorityHash{}, true, nil
				}
				return authorityPhysicalRollbackRow{}, authorityPoint{},
					authorityPoint{}, authorityHash{}, false, nil
			},
		}
	}

	exactEvents := []authorityPhysicalMembershipRow{
		membershipAt(current.EventSeq, "adoption"),
	}
	summary, err := walkAuthorityPublicationLifecycle(
		context.Background(),
		record,
		record.Physical.EventSeq,
		block,
		point,
		readersFor(exactEvents),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateAuthorityCurrentAdoptionLifecycleSummary(
		current,
		summary,
	); err != nil {
		t.Fatalf("exact current adoption lifecycle rejected: %v", err)
	}

	for name, events := range map[string][]authorityPhysicalMembershipRow{
		"orphan invalidation": {
			membershipAt(4, "invalidation"),
		},
		"duplicate logical adoption": {
			membershipAt(3, "adoption"),
			membershipAt(5, "adoption"),
		},
		"adoption invalidation reactivation": {
			membershipAt(3, "adoption"),
			membershipAt(4, "invalidation"),
			membershipAt(5, "adoption"),
		},
		"prior invalidation": {
			membershipAt(3, "adoption"),
			membershipAt(4, "invalidation"),
		},
		"wrong active adoption": {
			membershipAt(3, "adoption"),
		},
	} {
		name, events := name, events
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			summary, err := walkAuthorityPublicationLifecycle(
				context.Background(),
				record,
				record.Physical.EventSeq,
				block,
				point,
				readersFor(events),
			)
			if err == nil {
				err = validateAuthorityCurrentAdoptionLifecycleSummary(
					current,
					summary,
				)
			}
			if err == nil {
				t.Fatal("illegal current-adoption lifecycle was accepted")
			}
		})
	}
}

func TestAuthorityHistoricalArtifactBindings(t *testing.T) {
	t.Parallel()
	at := time.Date(
		2026, time.July, 23, 12, 0, 0, 123456000, time.UTC,
	)
	writer := uuid.UUID(authorityFill16(0x43))
	genesisHash := authorityFill32(0x61)
	point := authorityPoint{Hash: genesisHash}
	adoption := authorityPhysicalAdoptionRow{
		EventSeq:      1,
		PublicationID: 7,
		Active:        true,
		WriterID:      writer,
		RecordedAt:    at,
	}
	block := authorityPhysicalBlockRow{
		PublicationID: 7,
		Era:           "Byron",
		BlockType:     -1,
		Synthetic:     true,
		WriterID:      writer,
		InsertedAt:    at,
	}
	record := authorityRecord{
		ByronGenesisID:  genesisHash,
		Start:           authorityPoint{Origin: true},
		GenesisSeeded:   true,
		CompleteHistory: true,
		Physical: authorityHead{
			EventSeq: 99,
			Point:    authorityPoint{Origin: true},
		},
	}
	if err := validateAuthorityAdoptionBlockIdentity(
		adoption,
		point,
		block,
		point,
	); err != nil {
		t.Fatal(err)
	}
	if err := validateAuthorityHistoricalSynthetic(
		record,
		adoption,
		block,
		point,
	); err != nil {
		t.Fatalf("historical genesis was tied to current Physical: %v", err)
	}
	bad := block
	bad.BlockType = 0
	if err := validateAuthorityHistoricalSynthetic(
		record,
		adoption,
		bad,
		point,
	); err == nil {
		t.Fatal("non-genesis synthetic identity was accepted")
	}

	header := authorityRollbackArtifactTestRow(authorityFill32(0x62))
	membership := authorityMembershipTestRow(1, "invalidation")
	membership.RollbackID = &header.RollbackID
	membership.WriterID = header.WriterID
	membership.RecordedAt = header.RecordedAt
	blockPoint := authorityPoint{
		Slot:        membership.Slot,
		Hash:        authorityFill32(0x51),
		BlockNumber: membership.BlockNumber,
	}
	if err := validateAuthorityHistoricalInvalidation(
		membership,
		blockPoint,
		blockPoint,
		header,
		true,
	); err != nil {
		t.Fatalf("exact historical invalidation rejected: %v", err)
	}
	if err := validateAuthorityHistoricalInvalidation(
		membership,
		blockPoint,
		blockPoint,
		header,
		false,
	); err == nil {
		t.Fatal("orphan historical invalidation was accepted")
	}
	wrongHeader := header
	wrongHeader.RollbackID[0]++
	if err := validateAuthorityHistoricalInvalidation(
		membership,
		blockPoint,
		blockPoint,
		wrongHeader,
		true,
	); err == nil {
		t.Fatal("wrong historical rollback header was accepted")
	}
}

func TestWalkAuthorityRollbackDescendantsStreamsExactChain(t *testing.T) {
	t.Parallel()
	targetHash := authorityFill32(0x71)
	middleHash := authorityFill32(0x72)
	tipHash := authorityFill32(0x73)
	target := authorityActiveBlock{
		PublicationID: 1,
		Point: authorityPoint{
			Slot:        10,
			Hash:        targetHash,
			BlockNumber: 10,
		},
	}
	middle := authorityActiveBlock{
		PublicationID: 2,
		Point: authorityPoint{
			Slot:        11,
			Hash:        middleHash,
			BlockNumber: 11,
		},
		ParentHash: &targetHash,
	}
	tip := authorityActiveBlock{
		PublicationID: 3,
		Point: authorityPoint{
			Slot:        12,
			Hash:        tipHash,
			BlockNumber: 12,
		},
		ParentHash: &middleHash,
	}
	blocks := map[authorityHash]authorityActiveBlock{
		targetHash: target,
		middleHash: middle,
		tipHash:    tip,
	}
	loadedTarget := false
	resolver := func(
		_ context.Context,
		snapshot uint64,
		hash authorityHash,
	) (authorityActiveBlock, bool, error) {
		if snapshot != 9 {
			t.Fatalf("resolver snapshot = %d", snapshot)
		}
		if hash == targetHash {
			loadedTarget = true
		}
		block, found := blocks[hash]
		return block, found, nil
	}
	header := authorityPhysicalRollbackRow{Depth: 2, OldTipEventSeq: 9}
	var descendants []authorityRollbackDescendant
	depth, err := walkAuthorityRollbackDescendants(
		context.Background(),
		authorityRecord{Start: authorityPoint{Origin: true}},
		header,
		target.Point,
		tip.Point,
		resolver,
		func(descendant authorityRollbackDescendant) error {
			descendants = append(descendants, descendant)
			return nil
		},
	)
	if err != nil || depth != 2 || len(descendants) != 2 ||
		descendants[0].PublicationID != 3 ||
		descendants[1].PublicationID != 2 ||
		!loadedTarget {
		t.Fatalf(
			"streamed walk = depth %d descendants %#v targetLoaded %v err %v",
			depth,
			descendants,
			loadedTarget,
			err,
		)
	}
}

func TestWalkAuthorityRollbackDescendantsBoundaries(t *testing.T) {
	t.Parallel()
	startHash := authorityFill32(0x71)
	childHash := authorityFill32(0x72)
	start := authorityPoint{
		Slot:        10,
		Hash:        startHash,
		BlockNumber: 10,
	}
	child := authorityActiveBlock{
		PublicationID: 2,
		Point: authorityPoint{
			Slot:        11,
			Hash:        childHash,
			BlockNumber: 11,
		},
		ParentHash: &startHash,
	}
	resolveChildOnly := func(
		_ context.Context,
		_ uint64,
		hash authorityHash,
	) (authorityActiveBlock, bool, error) {
		if hash == childHash {
			return child, true, nil
		}
		return authorityActiveBlock{}, false, nil
	}
	var visited int
	depth, err := walkAuthorityRollbackDescendants(
		context.Background(),
		authorityRecord{Start: start},
		authorityPhysicalRollbackRow{Depth: 1, OldTipEventSeq: 5},
		start,
		child.Point,
		resolveChildOnly,
		func(authorityRollbackDescendant) error {
			visited++
			return nil
		},
	)
	if err != nil || depth != 1 || visited != 1 {
		t.Fatalf("partial Start adjacency = depth %d visits %d err %v", depth, visited, err)
	}

	originChild := child
	originChild.ParentHash = nil
	originResolver := func(
		_ context.Context,
		_ uint64,
		_ authorityHash,
	) (authorityActiveBlock, bool, error) {
		return originChild, true, nil
	}
	depth, err = walkAuthorityRollbackDescendants(
		context.Background(),
		authorityRecord{Start: authorityPoint{Origin: true}},
		authorityPhysicalRollbackRow{Depth: 1, OldTipEventSeq: 5},
		authorityPoint{Origin: true},
		originChild.Point,
		originResolver,
		nil,
	)
	if err != nil || depth != 1 {
		t.Fatalf("parentless Origin adjacency = depth %d err %v", depth, err)
	}

	genesisHash := authorityFill32(0x70)
	genesis := authorityActiveBlock{
		PublicationID: 1,
		Point: authorityPoint{
			Hash: genesisHash,
		},
		Synthetic: true,
	}
	syntheticChild := child
	syntheticChild.Point.Slot = 1
	syntheticChild.Point.BlockNumber = 1
	syntheticChild.ParentHash = &genesisHash
	blocks := map[authorityHash]authorityActiveBlock{
		childHash:   syntheticChild,
		genesisHash: genesis,
	}
	resolver := func(
		_ context.Context,
		_ uint64,
		hash authorityHash,
	) (authorityActiveBlock, bool, error) {
		block, found := blocks[hash]
		return block, found, nil
	}
	depth, err = walkAuthorityRollbackDescendants(
		context.Background(),
		authorityRecord{Start: authorityPoint{Origin: true}},
		authorityPhysicalRollbackRow{Depth: 1, OldTipEventSeq: 5},
		authorityPoint{Origin: true},
		syntheticChild.Point,
		resolver,
		nil,
	)
	if err != nil || depth != 1 {
		t.Fatalf("synthetic exclusion = depth %d err %v", depth, err)
	}
	if _, err := walkAuthorityRollbackDescendants(
		context.Background(),
		authorityRecord{Start: authorityPoint{Origin: true}},
		authorityPhysicalRollbackRow{Depth: 1, OldTipEventSeq: 5},
		genesis.Point,
		syntheticChild.Point,
		resolver,
		nil,
	); err == nil {
		t.Fatal("synthetic fact point was accepted as a non-Origin target")
	}
}

func TestWalkAuthorityRollbackDescendantsRejectsProgressAndMissingParent(t *testing.T) {
	t.Parallel()
	parentHash := authorityFill32(0x71)
	childHash := authorityFill32(0x72)
	child := authorityActiveBlock{
		PublicationID: 2,
		Point: authorityPoint{
			Slot:        11,
			Hash:        childHash,
			BlockNumber: 11,
		},
		ParentHash: &parentHash,
	}
	tests := map[string]authorityActiveBlock{
		"publication cycle": {
			PublicationID: 2,
			Point: authorityPoint{
				Slot:        10,
				Hash:        parentHash,
				BlockNumber: 10,
			},
		},
		"point cycle": {
			PublicationID: 1,
			Point: authorityPoint{
				Slot:        11,
				Hash:        parentHash,
				BlockNumber: 11,
			},
		},
	}
	for name, parent := range tests {
		name, parent := name, parent
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			resolver := func(
				_ context.Context,
				_ uint64,
				hash authorityHash,
			) (authorityActiveBlock, bool, error) {
				if hash == childHash {
					return child, true, nil
				}
				return parent, true, nil
			}
			if _, err := walkAuthorityRollbackDescendants(
				context.Background(),
				authorityRecord{Start: authorityPoint{Origin: true}},
				authorityPhysicalRollbackRow{Depth: 1, OldTipEventSeq: 5},
				parent.Point,
				child.Point,
				resolver,
				nil,
			); err == nil {
				t.Fatal("non-progressing parent chain was accepted")
			}
		})
	}
	missingResolver := func(
		_ context.Context,
		_ uint64,
		hash authorityHash,
	) (authorityActiveBlock, bool, error) {
		if hash == childHash {
			return child, true, nil
		}
		return authorityActiveBlock{}, false, nil
	}
	if _, err := walkAuthorityRollbackDescendants(
		context.Background(),
		authorityRecord{Start: authorityPoint{Origin: true}},
		authorityPhysicalRollbackRow{Depth: 1, OldTipEventSeq: 5},
		authorityPoint{Origin: true},
		child.Point,
		missingResolver,
		nil,
	); err == nil {
		t.Fatal("missing nonnil parent was accepted for Origin rollback")
	}
}

func TestWalkAuthorityRollbackDescendantsMaxDepthCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	resolver := func(
		context.Context,
		uint64,
		authorityHash,
	) (authorityActiveBlock, bool, error) {
		called = true
		return authorityActiveBlock{}, false, errors.New("unexpected resolver")
	}
	point := authorityPoint{Hash: authorityFill32(0x71)}
	if _, err := walkAuthorityRollbackDescendants(
		ctx,
		authorityRecord{Start: authorityPoint{Origin: true}},
		authorityPhysicalRollbackRow{
			Depth:          ^uint32(0),
			OldTipEventSeq: 5,
		},
		authorityPoint{Origin: true},
		point,
		resolver,
		nil,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("max-depth canceled walk error = %v", err)
	}
	if called {
		t.Fatal("max-depth canceled walk resolved or allocated traversal state")
	}
}

func TestAuthorityDepthZeroPriorRollbackAtPartialStartNeedsNoBlock(t *testing.T) {
	t.Parallel()
	start := authorityPoint{
		Slot:        100,
		Hash:        authorityFill32(0x71),
		BlockNumber: 10,
	}
	record := authorityRecord{
		Start: start,
		Physical: authorityHead{
			EventSeq: 77,
			Point:    start,
		},
	}
	needsActive, err := authorityDepthZeroNeedsActiveBlock(record, start, start)
	if err != nil || needsActive {
		t.Fatalf(
			"prior exact rollback head at partial Start needsActive=%v err=%v",
			needsActive,
			err,
		)
	}
	ordinary := start
	ordinary.Slot++
	ordinary.Hash = authorityFill32(0x72)
	ordinary.BlockNumber++
	needsActive, err = authorityDepthZeroNeedsActiveBlock(
		record,
		ordinary,
		ordinary,
	)
	if err != nil || !needsActive {
		t.Fatalf(
			"non-Start depth-zero tip needsActive=%v err=%v",
			needsActive,
			err,
		)
	}
}

func TestWalkAuthorityRollbackDescendantsAbsentStartPointTransitions(t *testing.T) {
	t.Parallel()
	hash := authorityFill32(0x81)
	childHash := authorityFill32(0x82)
	tests := map[string]struct {
		start authorityPoint
		child authorityPoint
	}{
		"ordinary to EBB": {
			start: authorityPoint{
				Slot:        10,
				Hash:        hash,
				BlockNumber: 5,
			},
			child: authorityPoint{
				Slot:        11,
				Hash:        childHash,
				BlockNumber: 5,
				IsByronEBB:  true,
			},
		},
		"EBB to ordinary equal slot": {
			start: authorityPoint{
				Slot:        10,
				Hash:        hash,
				BlockNumber: 5,
				IsByronEBB:  true,
			},
			child: authorityPoint{
				Slot:        10,
				Hash:        childHash,
				BlockNumber: 6,
			},
		},
	}
	for name, test := range tests {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			child := authorityActiveBlock{
				PublicationID: 2,
				Point:         test.child,
				ParentHash:    &hash,
			}
			resolver := func(
				_ context.Context,
				_ uint64,
				blockHash authorityHash,
			) (authorityActiveBlock, bool, error) {
				if blockHash == childHash {
					return child, true, nil
				}
				return authorityActiveBlock{}, false, nil
			}
			depth, err := walkAuthorityRollbackDescendants(
				context.Background(),
				authorityRecord{Start: test.start},
				authorityPhysicalRollbackRow{
					Depth:          1,
					OldTipEventSeq: 9,
				},
				test.start,
				test.child,
				resolver,
				nil,
			)
			if err != nil || depth != 1 {
				t.Fatalf("absent Start transition depth=%d err=%v", depth, err)
			}
		})
	}
}

func TestWalkAuthorityRollbackDescendantsRejectsInvalidAbsentStartTransition(t *testing.T) {
	t.Parallel()
	startHash := authorityFill32(0x81)
	childHash := authorityFill32(0x82)
	start := authorityPoint{
		Slot:        10,
		Hash:        startHash,
		BlockNumber: 5,
	}
	tests := map[string]authorityPoint{
		"EBB height": {
			Slot:        11,
			Hash:        childHash,
			BlockNumber: 6,
			IsByronEBB:  true,
		},
		"EBB slot": {
			Slot:        10,
			Hash:        childHash,
			BlockNumber: 5,
			IsByronEBB:  true,
		},
		"ordinary height": {
			Slot:        11,
			Hash:        childHash,
			BlockNumber: 5,
		},
		"ordinary slot": {
			Slot:        10,
			Hash:        childHash,
			BlockNumber: 6,
		},
		"ordinary overflow": {
			Slot:        11,
			Hash:        childHash,
			BlockNumber: 0,
		},
	}
	for name, childPoint := range tests {
		name, childPoint := name, childPoint
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gotStart := start
			if name == "ordinary overflow" {
				gotStart.BlockNumber = ^uint64(0)
			}
			child := authorityActiveBlock{
				PublicationID: 2,
				Point:         childPoint,
				ParentHash:    &startHash,
			}
			resolver := func(
				_ context.Context,
				_ uint64,
				_ authorityHash,
			) (authorityActiveBlock, bool, error) {
				return child, true, nil
			}
			if _, err := walkAuthorityRollbackDescendants(
				context.Background(),
				authorityRecord{Start: gotStart},
				authorityPhysicalRollbackRow{
					Depth:          1,
					OldTipEventSeq: 9,
				},
				gotStart,
				childPoint,
				resolver,
				nil,
			); err == nil {
				t.Fatal("invalid absent Start transition was accepted")
			}
		})
	}
}
