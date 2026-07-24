package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func authorityInvalidationTestHeader(
	depth uint32,
) authorityPhysicalRollbackRow {
	return authorityPhysicalRollbackRow{
		RollbackID: uuid.UUID(authorityFill16(0x31)),
		EventSeq:   7,
		Depth:      depth,
		WriterID:   uuid.UUID(authorityFill16(0x43)),
		RecordedAt: time.Date(
			2026, time.July, 23, 12, 0, 0, 123456000, time.UTC,
		),
	}
}

func authorityInvalidationTestPoint(value byte) authorityPoint {
	return authorityPoint{
		Slot:        uint64(value),
		Hash:        authorityFill32(value),
		BlockNumber: uint64(value),
	}
}

func TestAuthorityInvalidationSourceTaxonomy(t *testing.T) {
	t.Parallel()
	if _, _, _, err := decodeAuthorityInvalidationRows(
		[]authorityPhysicalMembershipRow{{EventSeq: 7}},
		7,
		1,
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("invalidation decoder semantic error = %v", err)
	}

	header := authorityInvalidationTestHeader(0)
	failures := []error{
		errors.New("injected invalidation query failure"),
		&ResourceLimitError{
			Phase: "injected",
			Cause: errors.New("injected invalidation resource failure"),
		},
	}
	for _, failure := range failures {
		failure := failure
		t.Run(failure.Error(), func(t *testing.T) {
			_, err := validateAuthorityRollbackInvalidationSet(
				context.Background(),
				header,
				func(
					context.Context,
					authorityRollbackDescendantVisitor,
				) (uint32, error) {
					return 0, nil
				},
				func(
					context.Context,
					uint64,
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
						nil
				},
				func(
					context.Context,
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
			)
			if !errors.Is(err, failure) ||
				errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("invalidation dependency error = %v", err)
			}
		})
	}
}

func authorityInvalidationTestRow(
	header authorityPhysicalRollbackRow,
	publicationID uint64,
	point authorityPoint,
) authorityPhysicalMembershipRow {
	rollbackID := header.RollbackID
	return authorityPhysicalMembershipRow{
		EventSeq:      header.EventSeq,
		PublicationID: publicationID,
		EventKind:     "invalidation",
		Active:        false,
		RollbackID:    &rollbackID,
		BlockHash:     string(point.Hash[:]),
		Slot:          point.Slot,
		BlockNumber:   point.BlockNumber,
		IsByronEBB:    point.IsByronEBB,
		WriterID:      header.WriterID,
		RecordedAt:    header.RecordedAt,
	}
}

func TestAuthorityInvalidationSQLShape(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		query string
		wants []string
	}{
		"exact": {
			query: authorityExactInvalidationSQL,
			wants: []string{
				"FROM chain_events",
				"PREWHERE event_kind = 'invalidation'",
				"AND event_seq = ?",
				"AND publication_id = ?",
				"ORDER BY event_kind, event_seq, publication_id, rollback_id",
				"LIMIT 9",
			},
		},
		"page": {
			query: authorityInvalidationPageSQL,
			wants: []string{
				"FROM chain_events",
				"PREWHERE event_kind = 'invalidation'",
				"AND event_seq = ?",
				"(NOT ? OR publication_id > ?)",
				"ORDER BY event_kind, event_seq, publication_id, rollback_id",
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
			for _, forbidden := range []string{
				"clicksync.",
				"OFFSET",
				"argMax",
				"count(",
			} {
				if strings.Contains(test.query, forbidden) {
					t.Fatalf(
						"query contains forbidden %q:\n%s",
						forbidden,
						test.query,
					)
				}
			}
		})
	}
}

func TestDecodeAuthorityInvalidationRowsReplayBounds(t *testing.T) {
	t.Parallel()
	header := authorityInvalidationTestHeader(1)
	point := authorityInvalidationTestPoint(0x51)
	row := authorityInvalidationTestRow(header, 9, point)
	eight := make([]authorityPhysicalMembershipRow, 8)
	for index := range eight {
		eight[index] = row
	}
	decoded, decodedPoint, found, err := decodeAuthorityInvalidationRows(
		eight,
		header.EventSeq,
		9,
	)
	if err != nil || !found || decoded.PublicationID != 9 ||
		decodedPoint != point {
		t.Fatalf(
			"eight identical invalidations rejected: row=%#v point=%#v found=%v err=%v",
			decoded,
			decodedPoint,
			found,
			err,
		)
	}
	if _, _, _, err := decodeAuthorityInvalidationRows(
		append(eight, row),
		header.EventSeq,
		9,
	); err == nil {
		t.Fatal("ninth identical invalidation was accepted")
	}
	conflict := append([]authorityPhysicalMembershipRow(nil), eight...)
	conflict[len(conflict)-1].Slot++
	if _, _, _, err := decodeAuthorityInvalidationRows(
		conflict,
		header.EventSeq,
		9,
	); err == nil {
		t.Fatal("conflicting invalidation replay was accepted")
	}
}

func TestDecodeAuthorityInvalidationPageStableKeySentinel(t *testing.T) {
	t.Parallel()
	header := authorityInvalidationTestHeader(2)
	first := authorityInvalidationTestRow(
		header,
		9,
		authorityInvalidationTestPoint(0x51),
	)
	eight := make([]authorityPhysicalMembershipRow, 8)
	for index := range eight {
		eight[index] = first
	}
	next := authorityInvalidationTestRow(
		header,
		10,
		authorityInvalidationTestPoint(0x52),
	)
	row, point, found, err := decodeAuthorityInvalidationPage(
		append(eight, next),
		header.EventSeq,
		true,
		8,
	)
	if err != nil || !found || row.PublicationID != 9 ||
		point != authorityInvalidationTestPoint(0x51) {
		t.Fatalf(
			"eight replays plus next-publication sentinel rejected: %#v %#v %v %v",
			row,
			point,
			found,
			err,
		)
	}
	if _, _, _, err := decodeAuthorityInvalidationPage(
		append(eight, first),
		header.EventSeq,
		true,
		8,
	); err == nil {
		t.Fatal("ninth first-publication replay was accepted")
	}
	for name, rows := range map[string][]authorityPhysicalMembershipRow{
		"cursor": {
			authorityInvalidationTestRow(
				header,
				8,
				authorityInvalidationTestPoint(0x51),
			),
		},
		"unordered": {
			next,
			first,
		},
		"over page": append(
			append([]authorityPhysicalMembershipRow(nil), eight...),
			next,
			next,
		),
	} {
		if _, _, _, err := decodeAuthorityInvalidationPage(
			rows,
			header.EventSeq,
			true,
			8,
		); err == nil {
			t.Fatalf("%s page corruption was accepted", name)
		}
	}
}

func TestDecodeAuthorityInvalidationRejectsCorruptShape(t *testing.T) {
	t.Parallel()
	header := authorityInvalidationTestHeader(1)
	point := authorityInvalidationTestPoint(0x51)
	valid := authorityInvalidationTestRow(header, 9, point)
	tests := map[string]func(*authorityPhysicalMembershipRow){
		"event zero": func(row *authorityPhysicalMembershipRow) {
			row.EventSeq = 0
		},
		"publication zero": func(row *authorityPhysicalMembershipRow) {
			row.PublicationID = 0
		},
		"kind": func(row *authorityPhysicalMembershipRow) {
			row.EventKind = "adoption"
		},
		"active": func(row *authorityPhysicalMembershipRow) {
			row.Active = true
		},
		"rollback nil": func(row *authorityPhysicalMembershipRow) {
			row.RollbackID = nil
		},
		"rollback zero": func(row *authorityPhysicalMembershipRow) {
			value := uuid.Nil
			row.RollbackID = &value
		},
		"hash": func(row *authorityPhysicalMembershipRow) {
			row.BlockHash = string(make([]byte, 32))
		},
		"writer": func(row *authorityPhysicalMembershipRow) {
			row.WriterID = uuid.Nil
		},
		"time": func(row *authorityPhysicalMembershipRow) {
			row.RecordedAt = row.RecordedAt.Add(time.Nanosecond)
		},
	}
	for name, corrupt := range tests {
		name, corrupt := name, corrupt
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			row := valid
			corrupt(&row)
			if _, _, _, err := decodeAuthorityInvalidationRows(
				[]authorityPhysicalMembershipRow{row},
				header.EventSeq,
				9,
			); err == nil {
				t.Fatal("corrupt invalidation was accepted")
			}
		})
	}
	for name, test := range map[string]struct {
		eventSeq      uint64
		publicationID uint64
	}{
		"zero event request": {
			eventSeq:      0,
			publicationID: 9,
		},
		"zero publication request": {
			eventSeq:      header.EventSeq,
			publicationID: 0,
		},
	} {
		if _, _, _, err := decodeAuthorityInvalidationRows(
			nil,
			test.eventSeq,
			test.publicationID,
		); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

type authorityInvalidationSetFixture struct {
	header      authorityPhysicalRollbackRow
	descendants []authorityRollbackDescendant
	invalidated []authorityRollbackDescendant
	rows        map[uint64]authorityPhysicalMembershipRow
}

func newAuthorityInvalidationSetFixture() authorityInvalidationSetFixture {
	header := authorityInvalidationTestHeader(2)
	descendants := []authorityRollbackDescendant{
		{PublicationID: 3, Point: authorityInvalidationTestPoint(0x53)},
		{PublicationID: 2, Point: authorityInvalidationTestPoint(0x52)},
	}
	rows := make(map[uint64]authorityPhysicalMembershipRow, len(descendants))
	for _, descendant := range descendants {
		rows[descendant.PublicationID] = authorityInvalidationTestRow(
			header,
			descendant.PublicationID,
			descendant.Point,
		)
	}
	return authorityInvalidationSetFixture{
		header:      header,
		descendants: descendants,
		invalidated: append([]authorityRollbackDescendant(nil), descendants...),
		rows:        rows,
	}
}

func (fixture authorityInvalidationSetFixture) stream(
	ctx context.Context,
	visit authorityRollbackDescendantVisitor,
) (uint32, error) {
	for index, descendant := range fixture.descendants {
		if err := ctx.Err(); err != nil {
			return uint32(index), err
		}
		if err := visit(descendant); err != nil {
			return uint32(index), err
		}
	}
	return uint32(len(fixture.descendants)), nil
}

func (fixture authorityInvalidationSetFixture) exact(
	_ context.Context,
	eventSeq uint64,
	publicationID uint64,
) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
	present := false
	for _, invalidated := range fixture.invalidated {
		if invalidated.PublicationID == publicationID {
			present = true
			break
		}
	}
	if !present {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false, nil
	}
	row, found := fixture.rows[publicationID]
	if !found || row.EventSeq != eventSeq {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false, nil
	}
	point, err := decodeAuthorityInvalidationRow(
		row,
		eventSeq,
		publicationID,
	)
	return row, point, err == nil, err
}

func (fixture authorityInvalidationSetFixture) next(
	_ context.Context,
	eventSeq uint64,
	cursorSet bool,
	cursor uint64,
) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
	var selected *authorityRollbackDescendant
	for index := range fixture.invalidated {
		candidate := &fixture.invalidated[index]
		if cursorSet && candidate.PublicationID <= cursor {
			continue
		}
		if selected == nil ||
			candidate.PublicationID < selected.PublicationID {
			selected = candidate
		}
	}
	if selected == nil {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false, nil
	}
	row, found := fixture.rows[selected.PublicationID]
	if !found || row.EventSeq != eventSeq {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
			errors.New("test fixture lacks invalidation row")
	}
	point, err := decodeAuthorityInvalidationRow(
		row,
		eventSeq,
		selected.PublicationID,
	)
	return row, point, err == nil, err
}

func TestValidateAuthorityRollbackInvalidationSetExact(t *testing.T) {
	t.Parallel()
	fixture := newAuthorityInvalidationSetFixture()
	complete, err := validateAuthorityRollbackInvalidationSet(
		context.Background(),
		fixture.header,
		fixture.stream,
		fixture.exact,
		fixture.next,
	)
	if err != nil || !complete {
		t.Fatalf("exact invalidation set rejected: complete=%v err=%v", complete, err)
	}
}

func TestValidateAuthorityRollbackInvalidationSetCrashCuts(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		mutate         func(*authorityInvalidationSetFixture)
		wantIncomplete bool
	}{
		"absent": {
			mutate: func(fixture *authorityInvalidationSetFixture) {
				fixture.invalidated = nil
			},
			wantIncomplete: true,
		},
		"partial": {
			mutate: func(fixture *authorityInvalidationSetFixture) {
				fixture.invalidated = fixture.invalidated[:1]
			},
		},
		"extra": {
			mutate: func(fixture *authorityInvalidationSetFixture) {
				extra := authorityRollbackDescendant{
					PublicationID: 4,
					Point:         authorityInvalidationTestPoint(0x54),
				}
				fixture.invalidated = append(fixture.invalidated, extra)
				fixture.rows[extra.PublicationID] =
					authorityInvalidationTestRow(
						fixture.header,
						extra.PublicationID,
						extra.Point,
					)
			},
		},
		"wrong same-size set": {
			mutate: func(fixture *authorityInvalidationSetFixture) {
				extra := authorityRollbackDescendant{
					PublicationID: 4,
					Point:         authorityInvalidationTestPoint(0x54),
				}
				fixture.invalidated[1] = extra
				fixture.rows[extra.PublicationID] =
					authorityInvalidationTestRow(
						fixture.header,
						extra.PublicationID,
						extra.Point,
					)
			},
		},
	} {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newAuthorityInvalidationSetFixture()
			test.mutate(&fixture)
			walked := false
			stream := fixture.stream
			if test.wantIncomplete {
				stream = func(
					ctx context.Context,
					visit authorityRollbackDescendantVisitor,
				) (uint32, error) {
					walked = true
					return fixture.stream(ctx, visit)
				}
			}
			complete, err := validateAuthorityRollbackInvalidationSet(
				context.Background(),
				fixture.header,
				stream,
				fixture.exact,
				fixture.next,
			)
			if test.wantIncomplete {
				if err != nil || complete || !walked {
					t.Fatalf(
						"absent set result complete=%v walked=%v err=%v",
						complete,
						walked,
						err,
					)
				}
				return
			}
			if err == nil {
				t.Fatal("non-exact invalidation set was accepted")
			}
		})
	}
}

func TestValidateAuthorityRollbackInvalidationSetAbsentPropagatesWalkFailure(
	t *testing.T,
) {
	t.Parallel()
	fixture := newAuthorityInvalidationSetFixture()
	fixture.invalidated = nil
	walkErr := errors.New("corrupt old-active chain")
	complete, err := validateAuthorityRollbackInvalidationSet(
		context.Background(),
		fixture.header,
		func(
			context.Context,
			authorityRollbackDescendantVisitor,
		) (uint32, error) {
			return 0, walkErr
		},
		func(
			context.Context,
			uint64,
			uint64,
		) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
				errors.New("absent set loaded an exact invalidation")
		},
		fixture.next,
	)
	if complete || !errors.Is(err, walkErr) {
		t.Fatalf(
			"absent set walk failure complete=%v err=%v",
			complete,
			err,
		)
	}
}

func TestValidateAuthorityRollbackInvalidationSetExactProvenance(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(
		*authorityInvalidationSetFixture,
	){
		"rollback": func(fixture *authorityInvalidationSetFixture) {
			row := fixture.rows[2]
			value := uuid.UUID(authorityFill16(0x32))
			row.RollbackID = &value
			fixture.rows[2] = row
		},
		"writer": func(fixture *authorityInvalidationSetFixture) {
			row := fixture.rows[2]
			row.WriterID[0]++
			fixture.rows[2] = row
		},
		"time": func(fixture *authorityInvalidationSetFixture) {
			row := fixture.rows[2]
			row.RecordedAt = row.RecordedAt.Add(time.Microsecond)
			fixture.rows[2] = row
		},
		"point": func(fixture *authorityInvalidationSetFixture) {
			row := fixture.rows[2]
			row.Slot++
			fixture.rows[2] = row
		},
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newAuthorityInvalidationSetFixture()
			mutate(&fixture)
			if complete, err := validateAuthorityRollbackInvalidationSet(
				context.Background(),
				fixture.header,
				fixture.stream,
				fixture.exact,
				fixture.next,
			); err == nil || complete {
				t.Fatalf(
					"%s provenance corruption accepted: complete=%v err=%v",
					name,
					complete,
					err,
				)
			}
		})
	}
}

func TestValidateAuthorityRollbackInvalidationSetDepthZero(t *testing.T) {
	t.Parallel()
	header := authorityInvalidationTestHeader(0)
	walked := false
	complete, err := validateAuthorityRollbackInvalidationSet(
		context.Background(),
		header,
		func(
			_ context.Context,
			_ authorityRollbackDescendantVisitor,
		) (uint32, error) {
			walked = true
			return 0, nil
		},
		func(
			context.Context,
			uint64,
			uint64,
		) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
				errors.New("depth zero loaded an exact invalidation")
		},
		func(
			context.Context,
			uint64,
			bool,
			uint64,
		) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false, nil
		},
	)
	if err != nil || !complete || !walked {
		t.Fatalf(
			"depth-zero empty set complete=%v walked=%v err=%v",
			complete,
			walked,
			err,
		)
	}
}

func TestValidateAuthorityRollbackInvalidationSetRejectsNondecreasingWalk(
	t *testing.T,
) {
	t.Parallel()
	fixture := newAuthorityInvalidationSetFixture()
	fixture.descendants[1].PublicationID = fixture.descendants[0].PublicationID
	if complete, err := validateAuthorityRollbackInvalidationSet(
		context.Background(),
		fixture.header,
		fixture.stream,
		fixture.exact,
		fixture.next,
	); err == nil || complete {
		t.Fatalf(
			"nondecreasing descendant walk accepted: complete=%v err=%v",
			complete,
			err,
		)
	}
}

func TestValidateAuthorityRollbackInvalidationSetMaxDepthCancellation(
	t *testing.T,
) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	header := authorityInvalidationTestHeader(^uint32(0))
	complete, err := validateAuthorityRollbackInvalidationSet(
		ctx,
		header,
		func(
			context.Context,
			authorityRollbackDescendantVisitor,
		) (uint32, error) {
			called = true
			return 0, nil
		},
		func(
			context.Context,
			uint64,
			uint64,
		) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
			called = true
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false, nil
		},
		func(
			context.Context,
			uint64,
			bool,
			uint64,
		) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
			called = true
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false, nil
		},
	)
	if !errors.Is(err, context.Canceled) || complete {
		t.Fatalf("max-depth canceled proof complete=%v err=%v", complete, err)
	}
	if called {
		t.Fatal("max-depth canceled proof called a dependency")
	}
}

func TestAuthorityInvalidationsUseDedicatedPhaseLimits(t *testing.T) {
	t.Parallel()
	limits := authorityInvalidationPhaseLimits()
	if limits.MaxResultRows != 9 {
		t.Fatalf("max result rows = %d", limits.MaxResultRows)
	}
}
