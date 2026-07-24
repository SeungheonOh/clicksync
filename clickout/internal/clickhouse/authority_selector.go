package clickhouse

import (
	"context"
	"errors"
	"fmt"
)

const authorityCutoffCandidateSQL = `
SELECT event_seq, publication_id
FROM chain_events
PREWHERE event_kind = 'adoption'
  AND event_seq <= ?
ORDER BY event_kind, event_seq DESC, publication_id DESC
LIMIT 9`

type authorityCutoffCandidateRow struct {
	EventSeq      uint64 `ch:"event_seq"`
	PublicationID uint64 `ch:"publication_id"`
}

// authorityCutoff is the exact latest logical adoption at or before a query
// head. The zero value is the canonical "no adoption yet" cutoff.
type authorityCutoff struct {
	AdoptionEventSeq uint64
	PublicationID    uint64
}

// authoritySelection deliberately keeps the manifest's safe Effective ceiling
// separate from the historical head selected for query execution.
type authoritySelection struct {
	AuthorityEffective authorityHead
	QueryHead          authorityHead
	Cutoff             authorityCutoff
}

type authorityBlockCandidate struct {
	Block    authorityActiveBlock
	Adoption authorityPhysicalAdoptionRow
}

func decodeAuthorityCutoffCandidates(
	rows []authorityCutoffCandidateRow,
	queryEventSeq uint64,
) (authorityCutoff, error) {
	if len(rows) == 0 {
		return authorityCutoff{}, nil
	}
	if len(rows) > 9 {
		return authorityCutoff{},
			invalidAuthorityError(
				errors.New("authority cutoff adoption read exceeds LIMIT 9"),
			)
	}
	top := authorityCutoff{
		AdoptionEventSeq: rows[0].EventSeq,
		PublicationID:    rows[0].PublicationID,
	}
	if top.AdoptionEventSeq == 0 ||
		top.AdoptionEventSeq > queryEventSeq ||
		top.PublicationID == 0 {
		return authorityCutoff{},
			invalidAuthorityError(
				errors.New("authority cutoff adoption lies outside the query head"),
			)
	}
	replays := 1
	for index := 1; index < len(rows); index++ {
		row := rows[index]
		if row.EventSeq > rows[index-1].EventSeq ||
			row.EventSeq == 0 ||
			row.EventSeq > queryEventSeq ||
			row.PublicationID == 0 {
			return authorityCutoff{},
				invalidAuthorityError(
					errors.New("authority cutoff adoption rows are not ordered/bounded"),
				)
		}
		if row.EventSeq == rows[index-1].EventSeq &&
			row.PublicationID > rows[index-1].PublicationID {
			return authorityCutoff{},
				invalidAuthorityError(
					errors.New("authority cutoff adoption publications are not ordered"),
				)
		}
		if row.EventSeq < top.AdoptionEventSeq {
			// A lower logical event is only the sentinel proving that the top
			// replay group ended before the raw LIMIT.
			continue
		}
		replays++
		if replays >= 9 {
			return authorityCutoff{},
				invalidAuthorityError(
					errors.New(
						"authority cutoff adoption has at least nine physical rows",
					),
				)
		}
		if row.PublicationID != top.PublicationID {
			return authorityCutoff{},
				invalidAuthorityError(
					errors.New("authority cutoff adoption rows conflict"),
				)
		}
	}
	return top, nil
}

func (store *Store) loadAuthorityCutoff(
	ctx context.Context,
	queryEventSeq uint64,
) (authorityCutoff, error) {
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"authority_cutoff_candidates",
		hydrationPhaseLimits(9),
	)
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		authorityCutoffCandidateSQL,
		queryEventSeq,
	)
	if err != nil {
		return authorityCutoff{},
			mapQueryError("authority_cutoff_candidates", err)
	}
	defer rows.Close()
	candidates := make([]authorityCutoffCandidateRow, 0, 9)
	for rows.Next() {
		var row authorityCutoffCandidateRow
		if err := rows.ScanStruct(&row); err != nil {
			return authorityCutoff{},
				fmt.Errorf("scan authority cutoff candidate: %w", err)
		}
		candidates = append(candidates, row)
	}
	if err := rows.Err(); err != nil {
		return authorityCutoff{},
			fmt.Errorf("iterate authority cutoff candidates: %w", err)
	}
	return decodeAuthorityCutoffCandidates(candidates, queryEventSeq)
}

type authorityCutoffArtifacts struct {
	Adoption      authorityPhysicalAdoptionRow
	AdoptionPoint authorityPoint
	Block         authorityPhysicalBlockRow
	BlockPoint    authorityPoint
}

type authorityCutoffArtifactReaders struct {
	loadAdoption func(
		context.Context,
		uint64,
	) (authorityPhysicalAdoptionRow, authorityPoint, bool, error)
	loadBlock func(
		context.Context,
		uint64,
	) (authorityPhysicalBlockRow, authorityPoint, bool, error)
}

func bindAuthorityCutoffArtifacts(
	ctx context.Context,
	record authorityRecord,
	cutoff authorityCutoff,
	readers authorityCutoffArtifactReaders,
) (authorityCutoffArtifacts, bool, error) {
	if err := ctx.Err(); err != nil {
		return authorityCutoffArtifacts{}, false, err
	}
	if cutoff == (authorityCutoff{}) {
		return authorityCutoffArtifacts{}, false, nil
	}
	if cutoff.AdoptionEventSeq == 0 || cutoff.PublicationID == 0 {
		return authorityCutoffArtifacts{}, false,
			invalidAuthorityError(
				errors.New("authority cutoff is only partially zero"),
			)
	}
	adoption, adoptionPoint, found, err := readers.loadAdoption(
		ctx,
		cutoff.AdoptionEventSeq,
	)
	if err != nil {
		return authorityCutoffArtifacts{}, false, err
	}
	if !found ||
		adoption.EventSeq != cutoff.AdoptionEventSeq ||
		adoption.PublicationID != cutoff.PublicationID {
		return authorityCutoffArtifacts{}, false,
			invalidAuthorityError(
				errors.New("authority cutoff differs from its exact adoption"),
			)
	}
	block, blockPoint, found, err := readers.loadBlock(
		ctx,
		cutoff.PublicationID,
	)
	if err != nil {
		return authorityCutoffArtifacts{}, false, err
	}
	if !found {
		return authorityCutoffArtifacts{}, false,
			invalidAuthorityError(
				errors.New("authority cutoff adoption has no exact block"),
			)
	}
	if err := validateAuthorityAdoptionBlockIdentity(
		adoption,
		adoptionPoint,
		block,
		blockPoint,
	); err != nil {
		return authorityCutoffArtifacts{}, false, invalidAuthorityError(err)
	}
	if block.Synthetic {
		if err := validateAuthorityHistoricalSynthetic(
			record,
			adoption,
			block,
			blockPoint,
		); err != nil {
			return authorityCutoffArtifacts{}, false, invalidAuthorityError(err)
		}
	}
	return authorityCutoffArtifacts{
		Adoption:      adoption,
		AdoptionPoint: adoptionPoint,
		Block:         block,
		BlockPoint:    blockPoint,
	}, true, nil
}

func (store *Store) bindAuthorityCutoff(
	ctx context.Context,
	record authorityRecord,
	cutoff authorityCutoff,
) (authorityCutoffArtifacts, bool, error) {
	return bindAuthorityCutoffArtifacts(
		ctx,
		record,
		cutoff,
		authorityCutoffArtifactReaders{
			loadAdoption: store.loadAuthorityPhysicalAdoption,
			loadBlock:    store.loadAuthorityPhysicalBlock,
		},
	)
}

type authoritySelectionCutoffReaders struct {
	load func(context.Context, uint64) (authorityCutoff, error)
	bind func(
		context.Context,
		authorityRecord,
		authorityCutoff,
	) (authorityCutoffArtifacts, bool, error)
}

func selectAndBindAuthorityCutoff(
	ctx context.Context,
	record authorityRecord,
	queryEventSeq uint64,
	readers authoritySelectionCutoffReaders,
) (authorityCutoff, authorityCutoffArtifacts, bool, error) {
	cutoff, err := readers.load(ctx, queryEventSeq)
	if err != nil {
		return authorityCutoff{}, authorityCutoffArtifacts{}, false, err
	}
	artifacts, found, err := readers.bind(ctx, record, cutoff)
	if err != nil {
		return authorityCutoff{}, authorityCutoffArtifacts{}, false, err
	}
	if (cutoff == (authorityCutoff{})) != !found {
		return authorityCutoff{}, authorityCutoffArtifacts{}, false,
			invalidAuthorityError(
				errors.New("authority cutoff/artifact presence differs"),
			)
	}
	return cutoff, artifacts, found, nil
}

func ensureAuthoritySelectionServable(record authorityRecord) error {
	if record.Servable {
		return nil
	}
	reason := record.TrustStatus
	if record.TrustReason != "" {
		reason += ": " + record.TrustReason
	}
	return newSnapshotUnavailableError(reason, &record)
}

func selectAuthorityAtTipWithReaders(
	ctx context.Context,
	record authorityRecord,
	readers authoritySelectionCutoffReaders,
) (authoritySelection, error) {
	if err := ensureAuthoritySelectionServable(record); err != nil {
		return authoritySelection{}, err
	}
	cutoff, _, _, err := selectAndBindAuthorityCutoff(
		ctx,
		record,
		record.Effective.EventSeq,
		readers,
	)
	if err != nil {
		return authoritySelection{}, err
	}
	return authoritySelection{
		AuthorityEffective: record.Effective,
		QueryHead:          record.Effective,
		Cutoff:             cutoff,
	}, nil
}

func (store *Store) authoritySelectionCutoffReaders() authoritySelectionCutoffReaders {
	return authoritySelectionCutoffReaders{
		load: store.loadAuthorityCutoff,
		bind: store.bindAuthorityCutoff,
	}
}

func (store *Store) selectAuthorityAtTip(
	ctx context.Context,
	record authorityRecord,
) (authoritySelection, error) {
	return selectAuthorityAtTipWithReaders(
		ctx,
		record,
		store.authoritySelectionCutoffReaders(),
	)
}

type authorityAtBlockReaders struct {
	nextCandidate func(
		context.Context,
		authorityHash,
		bool,
		uint64,
	) (uint64, bool, error)
	loadBlock func(
		context.Context,
		uint64,
	) (authorityPhysicalBlockRow, authorityPoint, bool, error)
	lifecycle func(
		context.Context,
		authorityRecord,
		uint64,
		authorityPhysicalBlockRow,
		authorityPoint,
	) (authorityPublicationLifecycleSummary, error)
	cutoff authoritySelectionCutoffReaders
}

func selectAuthorityAtBlockWithReaders(
	ctx context.Context,
	record authorityRecord,
	hash authorityHash,
	readers authorityAtBlockReaders,
) (authoritySelection, error) {
	if err := ensureAuthoritySelectionServable(record); err != nil {
		return authoritySelection{}, err
	}
	if hash == (authorityHash{}) {
		return authoritySelection{}, ErrNotFound
	}
	var (
		cursorSet bool
		cursor    uint64
		active    *authorityBlockCandidate
	)
	for {
		if err := ctx.Err(); err != nil {
			return authoritySelection{}, err
		}
		publicationID, found, err := readers.nextCandidate(
			ctx,
			hash,
			cursorSet,
			cursor,
		)
		if err != nil {
			return authoritySelection{}, err
		}
		if !found {
			break
		}
		if publicationID == 0 || (cursorSet && publicationID <= cursor) {
			return authoritySelection{}, fmt.Errorf(
				"%w: block-hash candidate cursor did not advance",
				ErrInvalidDataset,
			)
		}
		block, point, found, err := readers.loadBlock(ctx, publicationID)
		if err != nil {
			return authoritySelection{}, err
		}
		if !found || block.PublicationID != publicationID || point.Hash != hash {
			return authoritySelection{}, fmt.Errorf(
				"%w: block-hash candidate differs from exact publication",
				ErrInvalidDataset,
			)
		}
		lifecycle, err := readers.lifecycle(
			ctx,
			record,
			record.Effective.EventSeq,
			block,
			point,
		)
		if err != nil {
			return authoritySelection{}, err
		}
		if lifecycle.Active {
			if !lifecycle.AdoptionFound ||
				lifecycle.Adoption.EventSeq == 0 ||
				lifecycle.Adoption.EventSeq > record.Effective.EventSeq ||
				lifecycle.Adoption.PublicationID != publicationID {
				return authoritySelection{}, fmt.Errorf(
					"%w: active block lacks one bounded adoption event",
					ErrInvalidDataset,
				)
			}
			if active != nil {
				return authoritySelection{}, fmt.Errorf(
					"%w: block hash has multiple active publications",
					ErrInvalidDataset,
				)
			}
			parent, err := decodeAuthorityBlockParent(block)
			if err != nil {
				return authoritySelection{}, invalidAuthorityError(err)
			}
			active = &authorityBlockCandidate{
				Block: authorityActiveBlock{
					PublicationID:    publicationID,
					AdoptionEventSeq: lifecycle.Adoption.EventSeq,
					Point:            point,
					ParentHash:       parent,
					Synthetic:        block.Synthetic,
				},
			}
		}
		cursorSet = true
		cursor = publicationID
	}
	if active == nil {
		return authoritySelection{}, ErrNotFound
	}
	cutoff, artifacts, found, err := selectAndBindAuthorityCutoff(
		ctx,
		record,
		active.Block.AdoptionEventSeq,
		readers.cutoff,
	)
	if err != nil {
		return authoritySelection{}, err
	}
	expected := authorityCutoff{
		AdoptionEventSeq: active.Block.AdoptionEventSeq,
		PublicationID:    active.Block.PublicationID,
	}
	if !found || cutoff != expected {
		return authoritySelection{}, fmt.Errorf(
			"%w: AtBlock cutoff differs from selected active adoption",
			ErrInvalidDataset,
		)
	}
	if artifacts.Adoption.EventSeq != expected.AdoptionEventSeq ||
		artifacts.Adoption.PublicationID != expected.PublicationID ||
		artifacts.AdoptionPoint != active.Block.Point ||
		artifacts.AdoptionPoint.Hash != hash ||
		artifacts.Block.PublicationID != expected.PublicationID ||
		artifacts.BlockPoint != active.Block.Point ||
		artifacts.BlockPoint.Hash != hash {
		return authoritySelection{}, fmt.Errorf(
			"%w: AtBlock rebound artifacts differ from selected active publication",
			ErrInvalidDataset,
		)
	}
	active.Adoption = artifacts.Adoption
	queryPoint := active.Block.Point
	if active.Block.Synthetic {
		if active.Block.AdoptionEventSeq != 1 {
			return authoritySelection{}, fmt.Errorf(
				"%w: synthetic AtBlock is not genesis event one",
				ErrInvalidDataset,
			)
		}
		queryPoint = authorityPoint{Origin: true}
	}
	return authoritySelection{
		AuthorityEffective: record.Effective,
		QueryHead: authorityHead{
			EventSeq: active.Block.AdoptionEventSeq,
			Point:    queryPoint,
		},
		Cutoff: cutoff,
	}, nil
}

func (store *Store) selectAuthorityAtBlock(
	ctx context.Context,
	record authorityRecord,
	hash authorityHash,
) (authoritySelection, error) {
	return selectAuthorityAtBlockWithReaders(
		ctx,
		record,
		hash,
		authorityAtBlockReaders{
			nextCandidate: store.nextAuthorityBlockHashCandidate,
			loadBlock:     store.loadAuthorityPhysicalBlock,
			lifecycle:     store.authorityPublicationLifecycleAt,
			cutoff:        store.authoritySelectionCutoffReaders(),
		},
	)
}
