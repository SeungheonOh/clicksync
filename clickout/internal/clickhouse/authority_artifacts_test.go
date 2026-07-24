package clickhouse

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAuthorityExactArtifactDecoderTaxonomy(t *testing.T) {
	t.Parallel()
	tests := map[string]func() error{
		"adoption": func() error {
			_, _, _, err := decodeAuthorityPhysicalAdoptionRows(
				[]authorityPhysicalAdoptionRow{{EventSeq: 1}},
				1,
			)
			return err
		},
		"block": func() error {
			_, _, _, err := decodeAuthorityPhysicalBlockRows(
				[]authorityPhysicalBlockRow{{PublicationID: 1}},
				1,
			)
			return err
		},
		"rollback": func() error {
			_, _, _, _, _, err := decodeAuthorityPhysicalRollbackRows(
				[]authorityPhysicalRollbackRow{{EventSeq: 2}},
				2,
			)
			return err
		},
	}
	for name, decode := range tests {
		name, decode := name, decode
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := decode(); !errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("semantic decoder error = %v", err)
			}
		})
	}
}

func TestAuthorityPhysicalAdoptionSQLShape(t *testing.T) {
	t.Parallel()
	for _, want := range []string{
		"FROM chain_events",
		"PREWHERE event_kind = 'adoption'",
		"AND event_seq = ?",
		"ORDER BY event_kind, event_seq, publication_id",
		"LIMIT 9",
	} {
		if !strings.Contains(authorityPhysicalAdoptionSQL, want) {
			t.Fatalf("adoption query lacks %q: %q", want, authorityPhysicalAdoptionSQL)
		}
	}
	if strings.Contains(authorityPhysicalAdoptionSQL, "clicksync.") {
		t.Fatalf("adoption query qualifies the connection-selected DB: %q", authorityPhysicalAdoptionSQL)
	}
}

func TestAuthorityPhysicalAdoptionDuplicateBounds(t *testing.T) {
	t.Parallel()
	hash := authorityFill32(0x31)
	row := authorityPhysicalAdoptionRow{
		EventSeq:      5,
		PublicationID: 7,
		Active:        true,
		BlockHash:     string(hash[:]),
		Slot:          11,
		BlockNumber:   3,
		RecordedAt: time.Date(
			2026, time.July, 23, 12, 0, 0, 123456000, time.UTC,
		),
	}
	row.WriterID[0] = 1
	eight := make([]authorityPhysicalAdoptionRow, 8)
	for index := range eight {
		eight[index] = row
	}
	if _, _, found, err := decodeAuthorityPhysicalAdoptionRows(eight, 5); err != nil ||
		!found {
		t.Fatalf("eight identical physical rows rejected: found=%v err=%v", found, err)
	}
	if _, _, _, err := decodeAuthorityPhysicalAdoptionRows(
		append(eight, row),
		5,
	); err == nil {
		t.Fatal("ninth physical row was accepted")
	}
	conflict := append([]authorityPhysicalAdoptionRow(nil), eight...)
	conflict[len(conflict)-1].Slot++
	if _, _, _, err := decodeAuthorityPhysicalAdoptionRows(conflict, 5); err == nil {
		t.Fatal("conflicting physical rows were accepted")
	}

	alternateZone := row
	alternateZone.RecordedAt = row.RecordedAt.In(
		time.FixedZone("same instant", -5*60*60),
	)
	if !sameAuthorityPhysicalAdoptionRow(row, alternateZone) {
		t.Fatal("physical equality did not use timestamp instant equality")
	}
	if _, _, _, err := decodeAuthorityPhysicalAdoptionRows(
		[]authorityPhysicalAdoptionRow{row, alternateZone},
		5,
	); err == nil {
		t.Fatal("noncanonical duplicate timestamp escaped row validation")
	}
}

func TestAuthorityPhysicalAdoptionRejectsCorruptShape(t *testing.T) {
	t.Parallel()
	hash := authorityFill32(0x31)
	valid := authorityPhysicalAdoptionRow{
		EventSeq:      5,
		PublicationID: 7,
		Active:        true,
		BlockHash:     string(hash[:]),
		Slot:          11,
		BlockNumber:   3,
		RecordedAt: time.Date(
			2026, time.July, 23, 12, 0, 0, 123456000, time.UTC,
		),
	}
	valid.WriterID[0] = 1
	if _, _, _, err := decodeAuthorityPhysicalAdoptionRows(
		[]authorityPhysicalAdoptionRow{valid},
		0,
	); err == nil {
		t.Fatal("event-zero request was accepted")
	}
	tests := map[string]func(*authorityPhysicalAdoptionRow){
		"event zero": func(row *authorityPhysicalAdoptionRow) {
			row.EventSeq = 0
		},
		"publication zero": func(row *authorityPhysicalAdoptionRow) {
			row.PublicationID = 0
		},
		"writer zero": func(row *authorityPhysicalAdoptionRow) {
			row.WriterID = [16]byte{}
		},
		"time zero": func(row *authorityPhysicalAdoptionRow) {
			row.RecordedAt = time.Time{}
		},
		"time non UTC": func(row *authorityPhysicalAdoptionRow) {
			row.RecordedAt = row.RecordedAt.In(time.FixedZone("other", 3600))
		},
		"time beyond microseconds": func(row *authorityPhysicalAdoptionRow) {
			row.RecordedAt = row.RecordedAt.Add(time.Nanosecond)
		},
		"hash zero": func(row *authorityPhysicalAdoptionRow) {
			row.BlockHash = string(make([]byte, 32))
		},
		"hash short": func(row *authorityPhysicalAdoptionRow) {
			row.BlockHash = "short"
		},
	}
	for name, corrupt := range tests {
		name, corrupt := name, corrupt
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			row := valid
			corrupt(&row)
			if _, _, _, err := decodeAuthorityPhysicalAdoptionRows(
				[]authorityPhysicalAdoptionRow{row},
				5,
			); err == nil {
				t.Fatal("corrupt adoption was accepted")
			}
		})
	}
}

func TestAuthorityPhysicalAdoptionUsesDedicatedPhaseLimits(t *testing.T) {
	t.Parallel()
	limits := authorityPhysicalAdoptionPhaseLimits()
	if limits.MaxResultRows != 9 {
		t.Fatalf("max result rows = %d", limits.MaxResultRows)
	}
}

func TestAuthorityPhysicalBlockSQLShape(t *testing.T) {
	t.Parallel()
	for _, want := range []string{
		"publication_id, block_hash, parent_hash, slot, block_number, era,",
		"block_type, synthetic, facts_digest, writer_id, inserted_at",
		"FROM blocks",
		"PREWHERE publication_id = ?",
		"ORDER BY publication_id, block_hash",
		"LIMIT 9",
	} {
		if !strings.Contains(authorityPhysicalBlockSQL, want) {
			t.Fatalf("block query lacks %q: %q", want, authorityPhysicalBlockSQL)
		}
	}
	if strings.Contains(authorityPhysicalBlockSQL, "clicksync.") {
		t.Fatalf("block query qualifies the connection-selected DB: %q", authorityPhysicalBlockSQL)
	}
}

func TestAuthorityPhysicalBlockDuplicateBounds(t *testing.T) {
	t.Parallel()
	hash := authorityFill32(0x41)
	parent := authorityFill32(0x42)
	facts := authorityFill32(0x43)
	parentWire := string(parent[:])
	row := authorityPhysicalBlockRow{
		PublicationID: 7,
		BlockHash:     string(hash[:]),
		ParentHash:    &parentWire,
		Slot:          11,
		BlockNumber:   3,
		Era:           "Byron",
		BlockType:     0,
		FactsDigest:   string(facts[:]),
		InsertedAt: time.Date(
			2026, time.July, 23, 12, 0, 0, 123456000, time.UTC,
		),
	}
	row.WriterID[0] = 1
	eight := make([]authorityPhysicalBlockRow, 8)
	for index := range eight {
		eight[index] = row
	}
	if _, point, found, err := decodeAuthorityPhysicalBlockRows(eight, 7); err != nil ||
		!found || !point.IsByronEBB {
		t.Fatalf(
			"eight identical physical rows rejected: point=%#v found=%v err=%v",
			point,
			found,
			err,
		)
	}
	if _, _, _, err := decodeAuthorityPhysicalBlockRows(
		append(eight, row),
		7,
	); err == nil {
		t.Fatal("ninth physical block row was accepted")
	}
	conflict := append([]authorityPhysicalBlockRow(nil), eight...)
	conflict[len(conflict)-1].Slot++
	if _, _, _, err := decodeAuthorityPhysicalBlockRows(conflict, 7); err == nil {
		t.Fatal("conflicting physical block rows were accepted")
	}
}

func TestAuthorityPhysicalBlockRejectsCorruptShape(t *testing.T) {
	t.Parallel()
	hash := authorityFill32(0x41)
	parent := authorityFill32(0x42)
	facts := authorityFill32(0x43)
	parentWire := string(parent[:])
	valid := authorityPhysicalBlockRow{
		PublicationID: 7,
		BlockHash:     string(hash[:]),
		ParentHash:    &parentWire,
		Slot:          11,
		BlockNumber:   3,
		Era:           "Byron",
		BlockType:     0,
		FactsDigest:   string(facts[:]),
		InsertedAt: time.Date(
			2026, time.July, 23, 12, 0, 0, 123456000, time.UTC,
		),
	}
	valid.WriterID[0] = 1
	if _, _, _, err := decodeAuthorityPhysicalBlockRows(
		[]authorityPhysicalBlockRow{valid},
		0,
	); err == nil {
		t.Fatal("publication-zero request was accepted")
	}
	tests := map[string]func(*authorityPhysicalBlockRow){
		"publication zero": func(row *authorityPhysicalBlockRow) {
			row.PublicationID = 0
		},
		"block hash zero": func(row *authorityPhysicalBlockRow) {
			row.BlockHash = string(make([]byte, 32))
		},
		"block hash short": func(row *authorityPhysicalBlockRow) {
			row.BlockHash = "short"
		},
		"parent hash zero": func(row *authorityPhysicalBlockRow) {
			value := string(make([]byte, 32))
			row.ParentHash = &value
		},
		"parent hash short": func(row *authorityPhysicalBlockRow) {
			value := "short"
			row.ParentHash = &value
		},
		"facts digest zero": func(row *authorityPhysicalBlockRow) {
			row.FactsDigest = string(make([]byte, 32))
		},
		"facts digest short": func(row *authorityPhysicalBlockRow) {
			row.FactsDigest = "short"
		},
		"era empty": func(row *authorityPhysicalBlockRow) {
			row.Era = " \t"
		},
		"writer zero": func(row *authorityPhysicalBlockRow) {
			row.WriterID = [16]byte{}
		},
		"time zero": func(row *authorityPhysicalBlockRow) {
			row.InsertedAt = time.Time{}
		},
		"time non UTC": func(row *authorityPhysicalBlockRow) {
			row.InsertedAt = row.InsertedAt.In(time.FixedZone("other", 3600))
		},
		"time beyond microseconds": func(row *authorityPhysicalBlockRow) {
			row.InsertedAt = row.InsertedAt.Add(time.Nanosecond)
		},
	}
	for name, corrupt := range tests {
		name, corrupt := name, corrupt
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			row := valid
			corrupt(&row)
			if _, _, _, err := decodeAuthorityPhysicalBlockRows(
				[]authorityPhysicalBlockRow{row},
				7,
			); err == nil {
				t.Fatal("corrupt block was accepted")
			}
		})
	}
	originParent := valid
	originParent.ParentHash = nil
	if _, _, found, err := decodeAuthorityPhysicalBlockRows(
		[]authorityPhysicalBlockRow{originParent},
		7,
	); err != nil || !found {
		t.Fatalf("nil parent was rejected: found=%v err=%v", found, err)
	}
	for name, mutate := range map[string]func(*authorityPhysicalBlockRow){
		"non Byron": func(row *authorityPhysicalBlockRow) {
			row.Era = "Shelley"
		},
		"non EBB type": func(row *authorityPhysicalBlockRow) {
			row.BlockType = 1
		},
	} {
		row := valid
		mutate(&row)
		if _, point, _, err := decodeAuthorityPhysicalBlockRows(
			[]authorityPhysicalBlockRow{row},
			7,
		); err != nil || point.IsByronEBB {
			t.Fatalf("%s derived Byron EBB: point=%#v err=%v", name, point, err)
		}
	}
}

func TestAuthorityPhysicalBlockUsesDedicatedPhaseLimits(t *testing.T) {
	t.Parallel()
	limits := authorityPhysicalBlockPhaseLimits()
	if limits.MaxResultRows != 9 {
		t.Fatalf("max result rows = %d", limits.MaxResultRows)
	}
}

func TestValidateAuthorityPhysicalAdoptionMapping(t *testing.T) {
	t.Parallel()
	at := time.Date(
		2026, time.July, 23, 12, 0, 0, 123456000, time.UTC,
	)
	writer := [16]byte{1}
	hash := authorityFill32(0x51)
	ordinaryPoint := authorityPoint{
		Slot:        11,
		Hash:        hash,
		BlockNumber: 3,
		IsByronEBB:  true,
	}
	ordinaryAdoption := authorityPhysicalAdoptionRow{
		EventSeq:      5,
		PublicationID: 7,
		WriterID:      writer,
		RecordedAt:    at,
	}
	ordinaryBlock := authorityPhysicalBlockRow{
		PublicationID: 7,
		WriterID:      writer,
		InsertedAt:    at,
	}
	ordinaryRecord := authorityRecord{
		Physical: authorityHead{EventSeq: 5, Point: ordinaryPoint},
	}
	if err := validateAuthorityPhysicalAdoptionMapping(
		ordinaryRecord,
		ordinaryAdoption,
		ordinaryPoint,
		ordinaryBlock,
		ordinaryPoint,
	); err != nil {
		t.Fatalf("ordinary exact mapping rejected: %v", err)
	}

	requireRejected := func(
		name string,
		record authorityRecord,
		adoption authorityPhysicalAdoptionRow,
		adoptionPoint authorityPoint,
		block authorityPhysicalBlockRow,
		blockPoint authorityPoint,
	) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := validateAuthorityPhysicalAdoptionMapping(
				record,
				adoption,
				adoptionPoint,
				block,
				blockPoint,
			); err == nil {
				t.Fatal("mismatched mapping was accepted")
			}
		})
	}

	badRecord := ordinaryRecord
	badRecord.Physical.EventSeq++
	requireRejected(
		"ordinary event",
		badRecord,
		ordinaryAdoption,
		ordinaryPoint,
		ordinaryBlock,
		ordinaryPoint,
	)
	badBlock := ordinaryBlock
	badBlock.PublicationID++
	requireRejected(
		"ordinary publication",
		ordinaryRecord,
		ordinaryAdoption,
		ordinaryPoint,
		badBlock,
		ordinaryPoint,
	)
	badBlock = ordinaryBlock
	badBlock.WriterID[1] = 1
	requireRejected(
		"ordinary writer",
		ordinaryRecord,
		ordinaryAdoption,
		ordinaryPoint,
		badBlock,
		ordinaryPoint,
	)
	badBlock = ordinaryBlock
	badBlock.InsertedAt = badBlock.InsertedAt.Add(time.Microsecond)
	requireRejected(
		"ordinary time",
		ordinaryRecord,
		ordinaryAdoption,
		ordinaryPoint,
		badBlock,
		ordinaryPoint,
	)
	badPoint := ordinaryPoint
	badPoint.Slot++
	requireRejected(
		"ordinary block point",
		ordinaryRecord,
		ordinaryAdoption,
		ordinaryPoint,
		ordinaryBlock,
		badPoint,
	)
	badRecord = ordinaryRecord
	badRecord.Physical.Point.BlockNumber++
	requireRejected(
		"ordinary manifest point",
		badRecord,
		ordinaryAdoption,
		ordinaryPoint,
		ordinaryBlock,
		ordinaryPoint,
	)

	genesisHash := authorityFill32(0x61)
	genesisPoint := authorityPoint{Hash: genesisHash}
	genesisAdoption := authorityPhysicalAdoptionRow{
		EventSeq:      1,
		PublicationID: 1,
		WriterID:      writer,
		RecordedAt:    at,
	}
	genesisBlock := authorityPhysicalBlockRow{
		PublicationID: 1,
		Era:           "Byron",
		BlockType:     -1,
		Synthetic:     true,
		WriterID:      writer,
		InsertedAt:    at,
	}
	genesisRecord := authorityRecord{
		ByronGenesisID:  genesisHash,
		Start:           authorityPoint{Origin: true},
		GenesisSeeded:   true,
		CompleteHistory: true,
		Physical: authorityHead{
			EventSeq: 1,
			Point:    authorityPoint{Origin: true},
		},
	}
	if err := validateAuthorityPhysicalAdoptionMapping(
		genesisRecord,
		genesisAdoption,
		genesisPoint,
		genesisBlock,
		genesisPoint,
	); err != nil {
		t.Fatalf("exact synthetic genesis mapping rejected: %v", err)
	}

	type syntheticMutation func(
		*authorityRecord,
		*authorityPhysicalAdoptionRow,
		*authorityPoint,
		*authorityPhysicalBlockRow,
		*authorityPoint,
	)
	syntheticCorruptions := map[string]syntheticMutation{
		"physical event": func(record *authorityRecord, _ *authorityPhysicalAdoptionRow, _ *authorityPoint, _ *authorityPhysicalBlockRow, _ *authorityPoint) {
			record.Physical.EventSeq = 2
		},
		"physical point": func(record *authorityRecord, _ *authorityPhysicalAdoptionRow, _ *authorityPoint, _ *authorityPhysicalBlockRow, _ *authorityPoint) {
			record.Physical.Point = genesisPoint
		},
		"start": func(record *authorityRecord, _ *authorityPhysicalAdoptionRow, _ *authorityPoint, _ *authorityPhysicalBlockRow, _ *authorityPoint) {
			record.Start = genesisPoint
		},
		"not seeded": func(record *authorityRecord, _ *authorityPhysicalAdoptionRow, _ *authorityPoint, _ *authorityPhysicalBlockRow, _ *authorityPoint) {
			record.GenesisSeeded = false
		},
		"incomplete history": func(record *authorityRecord, _ *authorityPhysicalAdoptionRow, _ *authorityPoint, _ *authorityPhysicalBlockRow, _ *authorityPoint) {
			record.CompleteHistory = false
		},
		"slot": func(_ *authorityRecord, _ *authorityPhysicalAdoptionRow, adoptionPoint *authorityPoint, _ *authorityPhysicalBlockRow, blockPoint *authorityPoint) {
			adoptionPoint.Slot = 11
			blockPoint.Slot = 11
		},
		"block number": func(_ *authorityRecord, _ *authorityPhysicalAdoptionRow, adoptionPoint *authorityPoint, _ *authorityPhysicalBlockRow, blockPoint *authorityPoint) {
			adoptionPoint.BlockNumber = 3
			blockPoint.BlockNumber = 3
		},
		"Byron EBB": func(_ *authorityRecord, _ *authorityPhysicalAdoptionRow, adoptionPoint *authorityPoint, _ *authorityPhysicalBlockRow, blockPoint *authorityPoint) {
			adoptionPoint.IsByronEBB = true
			blockPoint.IsByronEBB = true
		},
		"genesis hash": func(_ *authorityRecord, _ *authorityPhysicalAdoptionRow, adoptionPoint *authorityPoint, _ *authorityPhysicalBlockRow, blockPoint *authorityPoint) {
			other := authorityFill32(0x62)
			adoptionPoint.Hash = other
			blockPoint.Hash = other
		},
		"parent": func(_ *authorityRecord, _ *authorityPhysicalAdoptionRow, _ *authorityPoint, block *authorityPhysicalBlockRow, _ *authorityPoint) {
			parentHash := authorityFill32(0x63)
			parent := string(parentHash[:])
			block.ParentHash = &parent
		},
		"era": func(_ *authorityRecord, _ *authorityPhysicalAdoptionRow, _ *authorityPoint, block *authorityPhysicalBlockRow, _ *authorityPoint) {
			block.Era = "Shelley"
		},
		"block type": func(_ *authorityRecord, _ *authorityPhysicalAdoptionRow, _ *authorityPoint, block *authorityPhysicalBlockRow, _ *authorityPoint) {
			block.BlockType = 0
		},
		"writer": func(_ *authorityRecord, _ *authorityPhysicalAdoptionRow, _ *authorityPoint, block *authorityPhysicalBlockRow, _ *authorityPoint) {
			block.WriterID[1] = 1
		},
		"time": func(_ *authorityRecord, _ *authorityPhysicalAdoptionRow, _ *authorityPoint, block *authorityPhysicalBlockRow, _ *authorityPoint) {
			block.InsertedAt = block.InsertedAt.Add(time.Microsecond)
		},
	}
	for name, mutate := range syntheticCorruptions {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gotRecord := genesisRecord
			gotAdoption := genesisAdoption
			gotAdoptionPoint := genesisPoint
			gotBlock := genesisBlock
			gotBlockPoint := genesisPoint
			mutate(
				&gotRecord,
				&gotAdoption,
				&gotAdoptionPoint,
				&gotBlock,
				&gotBlockPoint,
			)
			if err := validateAuthorityPhysicalAdoptionMapping(
				gotRecord,
				gotAdoption,
				gotAdoptionPoint,
				gotBlock,
				gotBlockPoint,
			); err == nil {
				t.Fatal("mismatched mapping was accepted")
			}
		})
	}
}

func authorityRollbackArtifactTestRow(
	digest authorityHash,
) authorityPhysicalRollbackRow {
	toHash := authorityFill32(0x51)
	oldHash := authorityFill32(0x52)
	toHashWire := string(toHash[:])
	oldHashWire := string(oldHash[:])
	toSlot := uint64(100)
	toNumber := uint64(10)
	oldSlot := uint64(110)
	oldNumber := uint64(11)
	group := uuid.UUID(authorityFill16(0x42))
	return authorityPhysicalRollbackRow{
		RollbackID:            uuid.UUID(authorityFill16(0x31)),
		EventSeq:              1,
		ToSlot:                &toSlot,
		ToHash:                &toHashWire,
		ToBlockNumber:         &toNumber,
		OldTipSlot:            &oldSlot,
		OldTipHash:            &oldHashWire,
		OldTipBlockNumber:     &oldNumber,
		OldTipEventSeq:        0,
		Depth:                 0,
		Reason:                "corroborated rollback",
		ObservedPeers:         []string{" relay-a ", "relay-b"},
		ObservedOperators:     []string{" Operator-A ", "operator-b"},
		CorroborationRequired: 2,
		CheckID:               uuid.UUID(authorityFill16(0x41)),
		AgreementGroup:        &group,
		CheckAttempt:          1,
		CheckedEventSeq:       0,
		EvidenceCount:         3,
		EvidenceDigest:        string(digest[:]),
		WriterID:              uuid.UUID(authorityFill16(0x43)),
		RecordedAt: time.Date(
			2026, time.July, 23, 12, 0, 0, 123456000, time.UTC,
		),
	}
}

func TestAuthorityPhysicalRollbackSQLShape(t *testing.T) {
	t.Parallel()
	for _, want := range []string{
		"rollback_id, event_seq,",
		"old_tip_event_seq, depth, reason, observed_peers, observed_operators,",
		"evidence_count, evidence_digest, writer_id, recorded_at",
		"FROM rollbacks",
		"PREWHERE event_seq = ?",
		"ORDER BY event_seq, rollback_id",
		"LIMIT 9",
	} {
		if !strings.Contains(authorityPhysicalRollbackSQL, want) {
			t.Fatalf("rollback query lacks %q: %q", want, authorityPhysicalRollbackSQL)
		}
	}
	if strings.Contains(authorityPhysicalRollbackSQL, "clicksync.") {
		t.Fatalf(
			"rollback query qualifies the connection-selected DB: %q",
			authorityPhysicalRollbackSQL,
		)
	}
}

func TestAuthorityPhysicalRollbackDuplicateBounds(t *testing.T) {
	t.Parallel()
	digest := authorityFill32(0x61)
	row := authorityRollbackArtifactTestRow(digest)
	eight := make([]authorityPhysicalRollbackRow, 8)
	for index := range eight {
		eight[index] = row
	}
	_, to, oldTip, decodedDigest, found, err :=
		decodeAuthorityPhysicalRollbackRows(eight, 1)
	if err != nil || !found ||
		to.Hash != authorityFill32(0x51) ||
		oldTip.Hash != authorityFill32(0x52) ||
		decodedDigest != digest {
		t.Fatalf(
			"eight identical rollback rows rejected: to=%#v old=%#v found=%v err=%v",
			to,
			oldTip,
			found,
			err,
		)
	}
	if _, _, _, _, _, err := decodeAuthorityPhysicalRollbackRows(
		append(eight, row),
		1,
	); err == nil {
		t.Fatal("ninth physical rollback row was accepted")
	}
	conflict := append([]authorityPhysicalRollbackRow(nil), eight...)
	conflict[len(conflict)-1].Reason = "different"
	if _, _, _, _, _, err := decodeAuthorityPhysicalRollbackRows(
		conflict,
		1,
	); err == nil {
		t.Fatal("conflicting physical rollback rows were accepted")
	}
	maximum := row
	maximum.Depth = ^uint32(0)
	maximum.EvidenceCount = 65535
	maximum.EventSeq = 7
	if _, _, _, _, found, err := decodeAuthorityPhysicalRollbackRows(
		[]authorityPhysicalRollbackRow{maximum},
		7,
	); err != nil || !found {
		t.Fatalf(
			"event-gap/full-depth/max-evidence rollback rejected: found=%v err=%v",
			found,
			err,
		)
	}
}

func TestAuthorityPhysicalRollbackRejectsCorruptShape(t *testing.T) {
	t.Parallel()
	digest := authorityFill32(0x61)
	valid := authorityRollbackArtifactTestRow(digest)
	if _, _, _, _, _, err := decodeAuthorityPhysicalRollbackRows(
		[]authorityPhysicalRollbackRow{valid},
		0,
	); err == nil {
		t.Fatal("event-zero rollback request was accepted")
	}
	tests := map[string]func(*authorityPhysicalRollbackRow){
		"rollback UUID": func(row *authorityPhysicalRollbackRow) {
			row.RollbackID = uuid.Nil
		},
		"old event max": func(row *authorityPhysicalRollbackRow) {
			row.OldTipEventSeq = ^uint64(0)
		},
		"check UUID": func(row *authorityPhysicalRollbackRow) {
			row.CheckID = uuid.Nil
		},
		"group nil": func(row *authorityPhysicalRollbackRow) {
			row.AgreementGroup = nil
		},
		"group zero": func(row *authorityPhysicalRollbackRow) {
			value := uuid.Nil
			row.AgreementGroup = &value
		},
		"writer UUID": func(row *authorityPhysicalRollbackRow) {
			row.WriterID = uuid.Nil
		},
		"attempt": func(row *authorityPhysicalRollbackRow) {
			row.CheckAttempt = 0
		},
		"time": func(row *authorityPhysicalRollbackRow) {
			row.RecordedAt = row.RecordedAt.Add(time.Nanosecond)
		},
		"reason": func(row *authorityPhysicalRollbackRow) {
			row.Reason = " "
		},
		"required": func(row *authorityPhysicalRollbackRow) {
			row.CorroborationRequired = 1
		},
		"evidence overflow": func(row *authorityPhysicalRollbackRow) {
			row.EvidenceCount = 65536
		},
		"too few observers": func(row *authorityPhysicalRollbackRow) {
			row.ObservedPeers = row.ObservedPeers[:1]
			row.ObservedOperators = row.ObservedOperators[:1]
		},
		"observer length": func(row *authorityPhysicalRollbackRow) {
			row.ObservedPeers = row.ObservedPeers[:1]
		},
		"operator empty": func(row *authorityPhysicalRollbackRow) {
			row.ObservedOperators[0] = " "
		},
		"peer empty": func(row *authorityPhysicalRollbackRow) {
			row.ObservedPeers[0] = " "
		},
		"operator duplicate": func(row *authorityPhysicalRollbackRow) {
			row.ObservedOperators[1] = " operator-A "
		},
		"target zero hash": func(row *authorityPhysicalRollbackRow) {
			value := string(make([]byte, 32))
			row.ToHash = &value
		},
		"old tip partial": func(row *authorityPhysicalRollbackRow) {
			row.OldTipHash = nil
		},
		"digest zero": func(row *authorityPhysicalRollbackRow) {
			row.EvidenceDigest = string(make([]byte, 32))
		},
	}
	for name, corrupt := range tests {
		name, corrupt := name, corrupt
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			row := valid
			row.ObservedPeers = append([]string(nil), valid.ObservedPeers...)
			row.ObservedOperators = append([]string(nil), valid.ObservedOperators...)
			corrupt(&row)
			expectedEvent := row.EventSeq
			if expectedEvent == 0 {
				expectedEvent = 1
			}
			if _, _, _, _, _, err := decodeAuthorityPhysicalRollbackRows(
				[]authorityPhysicalRollbackRow{row},
				expectedEvent,
			); err == nil {
				t.Fatal("corrupt rollback was accepted")
			}
		})
	}
}

func TestAuthorityPhysicalRollbackUsesDedicatedPhaseLimits(t *testing.T) {
	t.Parallel()
	limits := authorityPhysicalRollbackPhaseLimits()
	if limits.MaxResultRows != 9 {
		t.Fatalf("max result rows = %d", limits.MaxResultRows)
	}
}

func authorityPendingFromRollbackHeader(
	row authorityPhysicalRollbackRow,
	to authorityPoint,
	oldTip authorityPoint,
	digest authorityHash,
) authorityPendingRollback {
	return authorityPendingRollback{
		State:           "reserved",
		ID:              authorityUUID(row.RollbackID),
		EventSeq:        row.EventSeq,
		To:              to,
		OldPhysical:     authorityHead{EventSeq: row.OldTipEventSeq, Point: oldTip},
		Depth:           row.Depth,
		Reason:          row.Reason,
		Peers:           append([]string(nil), row.ObservedPeers...),
		Operators:       append([]string(nil), row.ObservedOperators...),
		Required:        row.CorroborationRequired,
		CheckID:         authorityUUID(row.CheckID),
		Group:           authorityUUID(*row.AgreementGroup),
		CheckAttempt:    row.CheckAttempt,
		CheckedEventSeq: row.CheckedEventSeq,
		EvidenceCount:   row.EvidenceCount,
		EvidenceDigest:  digest,
		WriterID:        authorityUUID(row.WriterID),
		StartedAt:       row.RecordedAt,
	}
}

func TestValidateAuthorityPendingRollbackHeaderExactFields(t *testing.T) {
	t.Parallel()
	digest := authorityFill32(0x61)
	row := authorityRollbackArtifactTestRow(digest)
	_, to, oldTip, _, _, err := decodeAuthorityPhysicalRollbackRows(
		[]authorityPhysicalRollbackRow{row},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	pending := authorityPendingFromRollbackHeader(row, to, oldTip, digest)
	if err := validateAuthorityPendingRollbackHeader(
		pending,
		row,
		to,
		oldTip,
		digest,
	); err != nil {
		t.Fatalf("exact pending/header match rejected: %v", err)
	}
	mutations := map[string]func(*authorityPendingRollback){
		"ID": func(value *authorityPendingRollback) {
			value.ID[0]++
		},
		"event": func(value *authorityPendingRollback) {
			value.EventSeq++
		},
		"target": func(value *authorityPendingRollback) {
			value.To.Slot++
		},
		"old event": func(value *authorityPendingRollback) {
			value.OldPhysical.EventSeq++
		},
		"old point": func(value *authorityPendingRollback) {
			value.OldPhysical.Point.Slot++
		},
		"depth": func(value *authorityPendingRollback) {
			value.Depth++
		},
		"reason": func(value *authorityPendingRollback) {
			value.Reason += "!"
		},
		"peers": func(value *authorityPendingRollback) {
			value.Peers[0] = "other"
		},
		"operators": func(value *authorityPendingRollback) {
			value.Operators[0] = "other"
		},
		"required": func(value *authorityPendingRollback) {
			value.Required++
		},
		"check": func(value *authorityPendingRollback) {
			value.CheckID[0]++
		},
		"group": func(value *authorityPendingRollback) {
			value.Group[0]++
		},
		"attempt": func(value *authorityPendingRollback) {
			value.CheckAttempt++
		},
		"checked": func(value *authorityPendingRollback) {
			value.CheckedEventSeq++
		},
		"count": func(value *authorityPendingRollback) {
			value.EvidenceCount++
		},
		"digest": func(value *authorityPendingRollback) {
			value.EvidenceDigest[0]++
		},
		"writer": func(value *authorityPendingRollback) {
			value.WriterID[0]++
		},
		"time": func(value *authorityPendingRollback) {
			value.StartedAt = value.StartedAt.Add(time.Microsecond)
		},
	}
	for name, mutate := range mutations {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			corrupt := pending
			corrupt.Peers = append([]string(nil), pending.Peers...)
			corrupt.Operators = append([]string(nil), pending.Operators...)
			mutate(&corrupt)
			if err := validateAuthorityPendingRollbackHeader(
				corrupt,
				row,
				to,
				oldTip,
				digest,
			); err == nil {
				t.Fatal("pending/header field mismatch was accepted")
			}
		})
	}
}

func authorityRollbackArtifactEvidenceRow(
	t *testing.T,
	group [16]byte,
	ordinal uint32,
	operator string,
	peer string,
	result string,
) authorityObservationRow {
	t.Helper()
	row := commitmentTestRow(t, group, 1, ordinal, byte(ordinal))
	row.Observation.Kind = "rollback"
	row.Observation.Operator = operator
	row.Observation.PeerHost = peer
	row.Observation.CheckedEventSeq = 0
	row.Observation.ProofMethod = "paired_chain_sync_singleton"
	row.Observation.Result = result
	row.Observation.PointVerified = result == "agreed"
	row.Observation.SelectedBodySource = false
	row.Observation.BodyHashVerified = false
	row.Observation.ParentVerified = false
	if err := finalizeAuthorityObservationIdentity(&row.Observation); err != nil {
		t.Fatal(err)
	}
	payload, err := canonicalAuthorityObservationPayload(row.Observation)
	if err != nil {
		t.Fatal(err)
	}
	row.OperatorKey = strings.ToLower(strings.TrimSpace(operator))
	row.Digest = authorityHash(sha256.Sum256([]byte(payload)))
	return row
}

func TestValidateAuthorityFinalizedRollbackHeaderEvidenceMap(t *testing.T) {
	t.Parallel()
	group := authorityFill16(0x42)
	evidence := []authorityObservationRow{
		authorityRollbackArtifactEvidenceRow(
			t, group, 1, "operator-a", "relay-a", "agreed",
		),
		authorityRollbackArtifactEvidenceRow(
			t, group, 2, "operator-b", "relay-b", "agreed",
		),
		authorityRollbackArtifactEvidenceRow(
			t, group, 3, "operator-c", "relay-c", "unavailable",
		),
	}
	commitment, err := canonicalAuthorityEvidenceCommitment(evidence, group, 1)
	if err != nil {
		t.Fatal(err)
	}
	row := authorityRollbackArtifactTestRow(commitment.Digest)
	_, to, _, digest, _, err := decodeAuthorityPhysicalRollbackRows(
		[]authorityPhysicalRollbackRow{row},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	reference := authorityEvidenceReference{
		CheckID:   authorityFill16(0x41),
		Group:     group,
		Attempt:   1,
		Required:  2,
		Confirmed: 2,
		Checked:   authorityHead{EventSeq: 0, Point: to},
		Count:     commitment.Count,
		Digest:    commitment.Digest,
	}
	head := authorityHead{EventSeq: 1, Point: to}
	lastAgreedAt := row.RecordedAt
	record := authorityRecord{
		Physical:           head,
		LastAgreed:         &head,
		LastAgreedAt:       &lastAgreedAt,
		LastAgreedEvidence: &reference,
	}
	if err := validateAuthorityFinalizedRollbackHeader(
		record,
		row,
		to,
		digest,
		evidence,
	); err != nil {
		t.Fatalf("exact finalized rollback/evidence map rejected: %v", err)
	}

	reordered := row
	reordered.ObservedPeers = []string{"relay-b", " relay-a "}
	reordered.ObservedOperators = []string{"operator-b", " Operator-A "}
	if err := validateAuthorityFinalizedRollbackHeader(
		record,
		reordered,
		to,
		digest,
		evidence,
	); err != nil {
		t.Fatalf("equivalent observer map ordering rejected: %v", err)
	}

	badPeers := row
	badPeers.ObservedPeers = []string{"relay-b", "relay-a"}
	if err := validateAuthorityFinalizedRollbackHeader(
		record,
		badPeers,
		to,
		digest,
		evidence,
	); err == nil {
		t.Fatal("swapped peer/operator associations were accepted")
	}
	omitted := row
	omitted.ObservedPeers = omitted.ObservedPeers[:1]
	omitted.ObservedOperators = omitted.ObservedOperators[:1]
	if err := validateAuthorityFinalizedRollbackHeader(
		record,
		omitted,
		to,
		digest,
		evidence,
	); err == nil {
		t.Fatal("incomplete agreed evidence map was accepted")
	}

	badRecord := record
	badRecord.Physical.EventSeq++
	if err := validateAuthorityFinalizedRollbackHeader(
		badRecord,
		row,
		to,
		digest,
		evidence,
	); err == nil {
		t.Fatal("wrong physical event was accepted")
	}
	badRecord = record
	badLastAgreedAt := lastAgreedAt.Add(time.Microsecond)
	badRecord.LastAgreedAt = &badLastAgreedAt
	if err := validateAuthorityFinalizedRollbackHeader(
		badRecord,
		row,
		to,
		digest,
		evidence,
	); err == nil {
		t.Fatal("wrong last-agreed finalization time was accepted")
	}
	badRecord = record
	badReference := reference
	badReference.Checked.EventSeq++
	badRecord.LastAgreedEvidence = &badReference
	if err := validateAuthorityFinalizedRollbackHeader(
		badRecord,
		row,
		to,
		digest,
		evidence,
	); err == nil {
		t.Fatal("wrong last-agreed checked event was accepted")
	}
	badRecord = record
	badReference = reference
	badReference.Digest[0]++
	badRecord.LastAgreedEvidence = &badReference
	if err := validateAuthorityFinalizedRollbackHeader(
		badRecord,
		row,
		to,
		digest,
		evidence,
	); err == nil {
		t.Fatal("wrong last-agreed evidence digest was accepted")
	}
}
