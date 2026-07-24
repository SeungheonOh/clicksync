package clickhouse

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

const authorityExactInvalidationSQL = `
SELECT
    event_seq, publication_id, event_kind, active, rollback_id, block_hash,
    slot, block_number, is_byron_ebb, writer_id, recorded_at
FROM chain_events
PREWHERE event_kind = 'invalidation'
  AND event_seq = ?
  AND publication_id = ?
ORDER BY event_kind, event_seq, publication_id, rollback_id
LIMIT 9`

const authorityInvalidationPageSQL = `
SELECT
    event_seq, publication_id, event_kind, active, rollback_id, block_hash,
    slot, block_number, is_byron_ebb, writer_id, recorded_at
FROM chain_events
PREWHERE event_kind = 'invalidation'
  AND event_seq = ?
WHERE (NOT ? OR publication_id > ?)
ORDER BY event_kind, event_seq, publication_id, rollback_id
LIMIT 9`

func decodeAuthorityInvalidationRow(
	row authorityPhysicalMembershipRow,
	eventSeq uint64,
	publicationID uint64,
) (point authorityPoint, err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if eventSeq == 0 || publicationID == 0 ||
		row.EventSeq != eventSeq ||
		row.PublicationID != publicationID ||
		row.EventKind != "invalidation" {
		return authorityPoint{}, errors.New(
			"physical invalidation differs from its exact event/publication",
		)
	}
	point, err = decodeAuthorityPhysicalMembershipRow(
		row,
		publicationID,
		eventSeq,
	)
	if err != nil {
		return authorityPoint{}, err
	}
	return point, nil
}

func decodeAuthorityInvalidationRows(
	rows []authorityPhysicalMembershipRow,
	eventSeq uint64,
	publicationID uint64,
) (
	row authorityPhysicalMembershipRow,
	point authorityPoint,
	found bool,
	err error,
) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if eventSeq == 0 || publicationID == 0 {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
			errors.New("physical invalidation request has a zero stable key")
	}
	if len(rows) == 0 {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false, nil
	}
	if len(rows) >= 9 {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
			errors.New(
				"physical invalidation has at least nine rows for one publication",
			)
	}
	var first authorityPhysicalMembershipRow
	for index, row := range rows {
		decoded, err := decodeAuthorityInvalidationRow(
			row,
			eventSeq,
			publicationID,
		)
		if err != nil {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
				err
		}
		if index == 0 {
			first = row
			point = decoded
			continue
		}
		if !sameAuthorityPhysicalMembershipRow(first, row) {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
				errors.New("physical invalidation rows conflict")
		}
	}
	return first, point, true, nil
}

func decodeAuthorityInvalidationPage(
	rows []authorityPhysicalMembershipRow,
	eventSeq uint64,
	cursorSet bool,
	cursor uint64,
) (
	row authorityPhysicalMembershipRow,
	point authorityPoint,
	found bool,
	err error,
) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if eventSeq == 0 {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
			errors.New("physical invalidation page request has event zero")
	}
	if len(rows) == 0 {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false, nil
	}
	if len(rows) > 9 {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
			errors.New("physical invalidation page exceeds LIMIT 9")
	}
	for index, row := range rows {
		if row.EventSeq != eventSeq ||
			row.PublicationID == 0 ||
			(cursorSet && row.PublicationID <= cursor) ||
			(index > 0 &&
				row.PublicationID < rows[index-1].PublicationID) {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
				errors.New(
					"physical invalidation page is outside its stable key",
				)
		}
	}
	publicationID := rows[0].PublicationID
	end := 1
	for end < len(rows) && rows[end].PublicationID == publicationID {
		end++
	}
	return decodeAuthorityInvalidationRows(
		rows[:end],
		eventSeq,
		publicationID,
	)
}

func authorityInvalidationPhaseLimits() phaseLimits {
	return hydrationPhaseLimits(9)
}

func (store *Store) loadAuthorityExactInvalidation(
	ctx context.Context,
	eventSeq uint64,
	publicationID uint64,
) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false, err
	}
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"authority_exact_invalidation",
		authorityInvalidationPhaseLimits(),
	)
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		authorityExactInvalidationSQL,
		eventSeq,
		publicationID,
	)
	if err != nil {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
			mapQueryError("authority_exact_invalidation", err)
	}
	defer rows.Close()
	physical := make([]authorityPhysicalMembershipRow, 0, 9)
	for rows.Next() {
		var row authorityPhysicalMembershipRow
		if err := rows.ScanStruct(&row); err != nil {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
				fmt.Errorf("scan exact physical invalidation: %w", err)
		}
		physical = append(physical, row)
	}
	if err := rows.Err(); err != nil {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
			fmt.Errorf("iterate exact physical invalidation: %w", err)
	}
	return decodeAuthorityInvalidationRows(
		physical,
		eventSeq,
		publicationID,
	)
}

func (store *Store) nextAuthorityInvalidation(
	ctx context.Context,
	eventSeq uint64,
	cursorSet bool,
	cursor uint64,
) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false, err
	}
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"authority_invalidation_page",
		authorityInvalidationPhaseLimits(),
	)
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		authorityInvalidationPageSQL,
		eventSeq,
		cursorSet,
		cursor,
	)
	if err != nil {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
			mapQueryError("authority_invalidation_page", err)
	}
	defer rows.Close()
	physical := make([]authorityPhysicalMembershipRow, 0, 9)
	for rows.Next() {
		var row authorityPhysicalMembershipRow
		if err := rows.ScanStruct(&row); err != nil {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
				fmt.Errorf("scan physical invalidation page: %w", err)
		}
		physical = append(physical, row)
	}
	if err := rows.Err(); err != nil {
		return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
			fmt.Errorf("iterate physical invalidation page: %w", err)
	}
	return decodeAuthorityInvalidationPage(
		physical,
		eventSeq,
		cursorSet,
		cursor,
	)
}

func validateAuthorityInvalidationHeader(
	row authorityPhysicalMembershipRow,
	header authorityPhysicalRollbackRow,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if row.EventKind != "invalidation" ||
		row.EventSeq != header.EventSeq ||
		row.RollbackID == nil ||
		*row.RollbackID != header.RollbackID ||
		row.WriterID != header.WriterID ||
		!row.RecordedAt.Equal(header.RecordedAt) {
		return errors.New(
			"physical invalidation differs from exact rollback header provenance",
		)
	}
	return nil
}

func validateAuthorityInvalidationDescendant(
	row authorityPhysicalMembershipRow,
	point authorityPoint,
	header authorityPhysicalRollbackRow,
	descendant authorityRollbackDescendant,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	if err := validateAuthorityInvalidationHeader(row, header); err != nil {
		return err
	}
	if row.PublicationID != descendant.PublicationID ||
		point != descendant.Point {
		return errors.New(
			"physical invalidation differs from exact rollback descendant",
		)
	}
	return nil
}

type authorityInvalidationExactReader func(
	context.Context,
	uint64,
	uint64,
) (authorityPhysicalMembershipRow, authorityPoint, bool, error)

type authorityInvalidationPageReader func(
	context.Context,
	uint64,
	bool,
	uint64,
) (authorityPhysicalMembershipRow, authorityPoint, bool, error)

type authorityRollbackDescendantStream func(
	context.Context,
	authorityRollbackDescendantVisitor,
) (uint32, error)

func validateAuthorityRollbackInvalidationSet(
	ctx context.Context,
	header authorityPhysicalRollbackRow,
	streamDescendants authorityRollbackDescendantStream,
	loadExact authorityInvalidationExactReader,
	nextInvalidation authorityInvalidationPageReader,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if header.EventSeq == 0 || header.RollbackID == uuid.Nil ||
		header.WriterID == uuid.Nil {
		return false, invalidAuthorityError(
			errors.New(
				"rollback invalidation proof has an invalid header identity",
			),
		)
	}
	if streamDescendants == nil || loadExact == nil ||
		nextInvalidation == nil {
		return false, invalidAuthorityError(
			errors.New(
				"rollback invalidation proof has a nil stream dependency",
			),
		)
	}

	var (
		logicalCount uint64
		cursorSet    bool
		cursor       uint64
	)
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		row, _, found, err := nextInvalidation(
			ctx,
			header.EventSeq,
			cursorSet,
			cursor,
		)
		if err != nil {
			return false, err
		}
		if !found {
			break
		}
		if row.PublicationID == 0 ||
			(cursorSet && row.PublicationID <= cursor) {
			return false, invalidAuthorityError(
				errors.New(
					"rollback invalidation stream did not advance its publication key",
				),
			)
		}
		if err := validateAuthorityInvalidationHeader(row, header); err != nil {
			return false, err
		}
		logicalCount++
		if logicalCount > uint64(header.Depth) {
			return false, invalidAuthorityError(
				fmt.Errorf(
					"rollback invalidation logical count exceeds depth %d",
					header.Depth,
				),
			)
		}
		cursorSet = true
		cursor = row.PublicationID
	}

	hasInvalidations := logicalCount > 0
	if hasInvalidations && logicalCount != uint64(header.Depth) {
		return false, invalidAuthorityError(
			fmt.Errorf(
				"rollback invalidation logical count %d differs from depth %d",
				logicalCount,
				header.Depth,
			),
		)
	}

	var (
		previousSet         bool
		previousPublication uint64
	)
	walked, err := streamDescendants(
		ctx,
		func(descendant authorityRollbackDescendant) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if descendant.PublicationID == 0 ||
				(previousSet &&
					descendant.PublicationID >= previousPublication) {
				return invalidAuthorityError(
					errors.New(
						"rollback descendants do not strictly decrease by publication",
					),
				)
			}
			previousSet = true
			previousPublication = descendant.PublicationID
			if !hasInvalidations {
				return nil
			}
			row, point, found, err := loadExact(
				ctx,
				header.EventSeq,
				descendant.PublicationID,
			)
			if err != nil {
				return err
			}
			if !found {
				return invalidAuthorityError(
					errors.New(
						"rollback descendant is missing its exact invalidation",
					),
				)
			}
			return validateAuthorityInvalidationDescendant(
				row,
				point,
				header,
				descendant,
			)
		},
	)
	if err != nil {
		return false, err
	}
	if walked != header.Depth {
		return false, invalidAuthorityError(
			errors.New(
				"rollback descendant walk differs from header depth",
			),
		)
	}
	if !hasInvalidations && header.Depth > 0 {
		return false, nil
	}
	return true, nil
}

func (store *Store) validateAuthorityRollbackInvalidations(
	ctx context.Context,
	record authorityRecord,
	header authorityPhysicalRollbackRow,
	to authorityPoint,
	oldTip authorityPoint,
) (bool, error) {
	return validateAuthorityRollbackInvalidationSet(
		ctx,
		header,
		func(
			streamCtx context.Context,
			visit authorityRollbackDescendantVisitor,
		) (uint32, error) {
			return store.reconstructAuthorityRollbackDescendants(
				streamCtx,
				record,
				header,
				to,
				oldTip,
				visit,
			)
		},
		store.loadAuthorityExactInvalidation,
		store.nextAuthorityInvalidation,
	)
}
