package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/google/uuid"
)

// LockAssertion is the actual single-host authority. WriterAudit rows are
// operational history only and are never consulted as a lock or fencing
// mechanism.
type LockAssertion interface {
	AssertHeld() error
	Path() string
}

type WriterAudit struct {
	DatasetID [16]byte
	OwnerID   [16]byte
	BuildID   string
	Hostname  string
	ProcessID uint32
	StartedAt time.Time
}

type WriterAuditStatus struct {
	DatasetID     [16]byte
	Revision      uint64
	OwnerID       [16]byte
	BuildID       string
	State         string
	HeartbeatAt   time.Time
	ReleasedAt    *time.Time
	ReleaseReason string
}

// LatestWriterAudit is a read-only operational status surface. Authority
// remains the live flock; this history can only describe the latest persisted
// audit transition for the current manifest dataset.
func (d *DB) LatestWriterAudit(
	ctx context.Context,
) (WriterAuditStatus, bool, error) {
	identity, found, err := d.LoadManifestIdentityIfExists(ctx)
	if err != nil || !found {
		return WriterAuditStatus{}, false, err
	}
	const query = `
SELECT
    dataset_id, revision, owner_id, build_id, state, heartbeat_at, released_at, release_reason
FROM clicksync.writer_audit
WHERE dataset_id = ?
  AND revision =
  (
      SELECT max(revision)
      FROM clicksync.writer_audit
      WHERE dataset_id = ?
  )`
	rows, err := d.conn.Query(
		ctx,
		query,
		uuid.UUID(identity.DatasetID),
		uuid.UUID(identity.DatasetID),
	)
	if err != nil {
		return WriterAuditStatus{}, false, fmt.Errorf("query latest writer audit: %w", err)
	}
	defer rows.Close()
	var statuses []WriterAuditStatus
	for rows.Next() {
		var (
			datasetID uuid.UUID
			ownerID   uuid.UUID
			status    WriterAuditStatus
		)
		if err := rows.Scan(
			&datasetID,
			&status.Revision,
			&ownerID,
			&status.BuildID,
			&status.State,
			&status.HeartbeatAt,
			&status.ReleasedAt,
			&status.ReleaseReason,
		); err != nil {
			return WriterAuditStatus{}, false, fmt.Errorf("scan latest writer audit: %w", err)
		}
		copy(status.DatasetID[:], datasetID[:])
		copy(status.OwnerID[:], ownerID[:])
		status.HeartbeatAt = status.HeartbeatAt.UTC()
		if status.ReleasedAt != nil {
			released := status.ReleasedAt.UTC()
			status.ReleasedAt = &released
		}
		statuses = append(statuses, status)
	}
	if err := rows.Err(); err != nil {
		return WriterAuditStatus{}, false, fmt.Errorf("iterate latest writer audit: %w", err)
	}
	return uniqueLatestWriterAudit(statuses)
}

func uniqueLatestWriterAudit(
	statuses []WriterAuditStatus,
) (WriterAuditStatus, bool, error) {
	if len(statuses) == 0 {
		return WriterAuditStatus{}, false, nil
	}
	first := statuses[0]
	for _, status := range statuses[1:] {
		if !sameWriterAuditStatus(first, status) {
			return WriterAuditStatus{}, false, errors.New(
				"latest writer audit revision has conflicting physical rows",
			)
		}
	}
	return first, true, nil
}

func sameWriterAuditStatus(left, right WriterAuditStatus) bool {
	if left.DatasetID != right.DatasetID ||
		left.Revision != right.Revision ||
		left.OwnerID != right.OwnerID ||
		left.BuildID != right.BuildID ||
		left.State != right.State ||
		!left.HeartbeatAt.Equal(right.HeartbeatAt) ||
		left.ReleaseReason != right.ReleaseReason {
		return false
	}
	if left.ReleasedAt == nil || right.ReleasedAt == nil {
		return left.ReleasedAt == nil && right.ReleasedAt == nil
	}
	return left.ReleasedAt.Equal(*right.ReleasedAt)
}

func (d *DB) BeginWriterAudit(
	ctx context.Context,
	lock LockAssertion,
	audit WriterAudit,
) error {
	if err := validateWriterAudit(lock, audit); err != nil {
		return err
	}
	startedAt := audit.StartedAt.UTC()
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	revision, err := d.nextWriterAuditRevision(ctx, audit.DatasetID)
	if err != nil {
		return err
	}
	const query = `INSERT INTO clicksync.writer_audit
(
    dataset_id, revision, owner_id, state, build_id, hostname, process_id,
    acquired_at, heartbeat_at, released_at, release_reason, lock_path
) VALUES (
    ?, ?, ?, 'active', ?, ?, ?,
    fromUnixTimestamp64Micro(?), fromUnixTimestamp64Micro(?), NULL, '', ?
)`
	if err := lock.AssertHeld(); err != nil {
		return fmt.Errorf("writer flock is not held before audit activation: %w", err)
	}
	return d.conn.Exec(
		ctx,
		query,
		uuid.UUID(audit.DatasetID),
		revision,
		uuid.UUID(audit.OwnerID),
		audit.BuildID,
		audit.Hostname,
		audit.ProcessID,
		startedAt.UnixMicro(),
		startedAt.UnixMicro(),
		lock.Path(),
	)
}

func (d *DB) HeartbeatWriterAudit(
	ctx context.Context,
	lock LockAssertion,
	audit WriterAudit,
	at time.Time,
) error {
	if err := validateWriterAudit(lock, audit); err != nil {
		return err
	}
	if err := lock.AssertHeld(); err != nil {
		return fmt.Errorf("writer flock is not held before audit heartbeat: %w", err)
	}
	if err := d.assertLatestActiveAudit(ctx, audit); err != nil {
		return err
	}
	revision, err := d.nextWriterAuditRevision(ctx, audit.DatasetID)
	if err != nil {
		return err
	}
	const query = `INSERT INTO clicksync.writer_audit
(
    dataset_id, revision, owner_id, state, build_id, hostname, process_id,
    acquired_at, heartbeat_at, released_at, release_reason, lock_path
)
SELECT
    dataset_id, ?, owner_id, 'active', build_id, hostname, process_id,
    acquired_at, fromUnixTimestamp64Micro(?), NULL, '', lock_path
FROM
(
    SELECT *
    FROM clicksync.writer_audit
    WHERE dataset_id = ?
      AND owner_id = ?
      AND state = 'active'
    ORDER BY revision DESC
    LIMIT 1
)`
	err = d.conn.Exec(
		ctx,
		query,
		revision,
		at.UTC().UnixMicro(),
		uuid.UUID(audit.DatasetID),
		uuid.UUID(audit.OwnerID),
	)
	if err != nil {
		return fmt.Errorf("insert writer audit heartbeat: %w", err)
	}
	return nil
}

func (d *DB) ReleaseWriterAudit(
	ctx context.Context,
	lock LockAssertion,
	audit WriterAudit,
	at time.Time,
	reason string,
) error {
	if err := validateWriterAudit(lock, audit); err != nil {
		return err
	}
	// Record the graceful release while the real flock is still held.
	if err := lock.AssertHeld(); err != nil {
		return fmt.Errorf("writer flock is not held before audit release: %w", err)
	}
	if err := d.assertLatestActiveAudit(ctx, audit); err != nil {
		return err
	}
	revision, err := d.nextWriterAuditRevision(ctx, audit.DatasetID)
	if err != nil {
		return err
	}
	const query = `INSERT INTO clicksync.writer_audit
(
    dataset_id, revision, owner_id, state, build_id, hostname, process_id,
    acquired_at, heartbeat_at, released_at, release_reason, lock_path
)
SELECT
    dataset_id, ?, owner_id, 'released', build_id, hostname, process_id,
    acquired_at, fromUnixTimestamp64Micro(?), fromUnixTimestamp64Micro(?), ?, lock_path
FROM
(
    SELECT *
    FROM clicksync.writer_audit
    WHERE dataset_id = ?
      AND owner_id = ?
      AND state = 'active'
    ORDER BY revision DESC
    LIMIT 1
)`
	err = d.conn.Exec(
		ctx,
		query,
		revision,
		at.UTC().UnixMicro(),
		at.UTC().UnixMicro(),
		reason,
		uuid.UUID(audit.DatasetID),
		uuid.UUID(audit.OwnerID),
	)
	if err != nil {
		return fmt.Errorf("insert writer audit release: %w", err)
	}
	return nil
}

func (d *DB) assertLatestActiveAudit(ctx context.Context, audit WriterAudit) error {
	const query = `
SELECT count()
FROM
(
    SELECT owner_id, state
    FROM clicksync.writer_audit
    WHERE dataset_id = ?
    ORDER BY revision DESC
    LIMIT 1
)
WHERE owner_id = ?
  AND state = 'active'`
	var matches uint64
	if err := d.conn.QueryRow(
		ctx,
		query,
		uuid.UUID(audit.DatasetID),
		uuid.UUID(audit.OwnerID),
	).Scan(&matches); err != nil {
		return fmt.Errorf("read active writer audit: %w", err)
	}
	if matches != 1 {
		return errors.New("latest writer audit row is not active for this owner")
	}
	return nil
}

func (d *DB) nextWriterAuditRevision(ctx context.Context, datasetID [16]byte) (uint64, error) {
	var revision uint64
	if err := d.conn.QueryRow(
		ctx,
		`SELECT max(revision) FROM clicksync.writer_audit WHERE dataset_id = ?`,
		uuid.UUID(datasetID),
	).Scan(&revision); err != nil {
		return 0, fmt.Errorf("read writer audit revision: %w", err)
	}
	if revision == math.MaxUint64 {
		return 0, errors.New("writer audit revision space exhausted")
	}
	return revision + 1, nil
}

func validateWriterAudit(lock LockAssertion, audit WriterAudit) error {
	if lock == nil {
		return errors.New("writer audit requires the real flock assertion")
	}
	if audit.DatasetID == ([16]byte{}) || audit.OwnerID == ([16]byte{}) {
		return errors.New("writer audit dataset and owner IDs must be non-zero")
	}
	if audit.BuildID == "" || audit.Hostname == "" || audit.ProcessID == 0 {
		return errors.New("writer audit build, hostname, and process ID are required")
	}
	if audit.ProcessID > math.MaxUint32 {
		return fmt.Errorf("writer audit process ID %d exceeds UInt32", audit.ProcessID)
	}
	return nil
}

func NewWriterAudit(datasetID, ownerID [16]byte, buildID string, startedAt time.Time) (WriterAudit, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return WriterAudit{}, fmt.Errorf("read hostname for writer audit: %w", err)
	}
	processID := os.Getpid()
	if processID <= 0 || uint64(processID) > math.MaxUint32 {
		return WriterAudit{}, fmt.Errorf("process ID %d does not fit UInt32", processID)
	}
	audit := WriterAudit{
		DatasetID: datasetID,
		OwnerID:   ownerID,
		BuildID:   buildID,
		Hostname:  hostname,
		ProcessID: uint32(processID),
		StartedAt: startedAt.UTC(),
	}
	return audit, validateWriterAudit(noopAuditLock{}, audit)
}

// noopAuditLock is used only for value validation during construction. Store
// writes still require the caller's real held lock.
type noopAuditLock struct{}

func (noopAuditLock) AssertHeld() error { return nil }
func (noopAuditLock) Path() string      { return "" }
