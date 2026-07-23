package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestNativeMillionRowMultipartManifestHeadIsPKBounded(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, lock, _, _, _, _ := nativeRollbackFixture(t, ctx)
	defer db.Close()
	defer lock.Release()

	base, found, err := db.loadLatestManifestRecord(ctx)
	if err != nil || !found {
		t.Fatalf("load scale base found=%t err=%v", found, err)
	}
	partStats := func() (uint64, uint64) {
		t.Helper()
		var parts, rows uint64
		if err := db.conn.QueryRow(
			ctx,
			`SELECT count(), sum(rows)
FROM system.parts
WHERE active
  AND database = 'clicksync'
  AND table = 'dataset_manifest'`,
		).Scan(&parts, &rows); err != nil {
			t.Fatal(err)
		}
		return parts, rows
	}
	initialActiveParts, initialPhysicalRows := partStats()
	if err := db.conn.Exec(ctx, `SYSTEM STOP MERGES clicksync.dataset_manifest`); err != nil {
		t.Fatal(err)
	}
	defer db.conn.Exec(context.Background(), `SYSTEM START MERGES clicksync.dataset_manifest`)
	const (
		rangeParts          = uint64(4)
		baselineRowsPerPart = uint64(250)
		rowsPerPart         = uint64(250_000)
		baselineRangeStart  = uint64(100)
		inflatedRangeStart  = uint64(10_000)
	)
	for part := uint64(0); part < rangeParts; part++ {
		firstRevision := baselineRangeStart + part*baselineRowsPerPart
		insert := fmt.Sprintf(
			`INSERT INTO clicksync.dataset_manifest
SELECT base.* REPLACE(toUInt64(%d + generated.number) AS revision)
FROM
(
    SELECT *
    FROM clicksync.dataset_manifest
    PREWHERE manifest_key = 1
    ORDER BY revision DESC
    LIMIT 1
) AS base
CROSS JOIN numbers(%d) AS generated
SETTINGS max_block_size = 1000000`,
			firstRevision,
			baselineRowsPerPart,
		)
		if err := db.conn.Exec(ctx, insert); err != nil {
			t.Fatalf("insert baseline range part %d: %v", part, err)
		}
	}

	baselinePredecessor := base
	baselinePredecessor.Revision = 1_100
	baselinePredecessor.TransitionKind = "physical_reconcile"
	baselinePrevious := manifestHash(0xed)
	baselinePredecessor.PreviousRowDigest = &baselinePrevious
	baselinePredecessor.TransitionID = [16]byte{}
	baselinePredecessor.RowDigest = [32]byte{}
	baselinePredecessor.UpdatedAt = baselinePredecessor.UpdatedAt.Add(time.Second)
	if err := finalizeManifestRecord(&baselinePredecessor); err != nil {
		t.Fatal(err)
	}
	baselineLatest, err := appendManifestTransition(
		baselinePredecessor,
		"physical_reconcile",
		baselinePredecessor.UpdatedAt.Add(time.Second),
		func(*manifestRecord) error { return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	insertNativeManifestRecord(t, ctx, db, baselinePredecessor)
	insertNativeManifestRecord(t, ctx, db, baselineLatest)
	baselineCtx := clickhouse.Context(
		ctx,
		clickhouse.WithQueryID("manifest-baseline-head-proof"),
	)
	if _, found, err := db.loadLatestManifestRecord(
		baselineCtx,
	); err != nil || !found {
		t.Fatalf("baseline bounded loader found=%t err=%v", found, err)
	}
	baselineActiveParts, baselinePhysicalRows := partStats()

	for part := uint64(0); part < rangeParts; part++ {
		firstRevision := inflatedRangeStart + part*rowsPerPart
		insert := fmt.Sprintf(
			`INSERT INTO clicksync.dataset_manifest
SELECT base.* REPLACE(toUInt64(%d + generated.number) AS revision)
FROM
(
    SELECT *
    FROM clicksync.dataset_manifest
    PREWHERE manifest_key = 1
    ORDER BY revision DESC
    LIMIT 1
) AS base
CROSS JOIN numbers(%d) AS generated
SETTINGS max_block_size = 1000000`,
			firstRevision,
			rowsPerPart,
		)
		if err := db.conn.Exec(ctx, insert); err != nil {
			t.Fatalf("insert inflated range part %d: %v", part, err)
		}
	}

	predecessor := base
	predecessor.Revision = inflatedRangeStart + rangeParts*rowsPerPart
	predecessor.TransitionKind = "physical_reconcile"
	previous := manifestHash(0xee)
	predecessor.PreviousRowDigest = &previous
	predecessor.TransitionID = [16]byte{}
	predecessor.RowDigest = [32]byte{}
	predecessor.UpdatedAt = predecessor.UpdatedAt.Add(time.Second)
	if err := finalizeManifestRecord(&predecessor); err != nil {
		t.Fatal(err)
	}
	latest, err := appendManifestTransition(
		predecessor,
		"physical_reconcile",
		predecessor.UpdatedAt.Add(time.Second),
		func(*manifestRecord) error { return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	insertNativeManifestRecord(t, ctx, db, predecessor)
	insertNativeManifestRecord(t, ctx, db, latest)

	activeParts, physicalRows := partStats()
	if baselineActiveParts <= initialActiveParts ||
		baselinePhysicalRows != initialPhysicalRows+1_002 ||
		activeParts < baselineActiveParts+rangeParts ||
		physicalRows != baselinePhysicalRows+1_000_002 {
		t.Fatalf(
			"manifest initial=%d/%d baseline=%d/%d inflated=%d/%d",
			initialActiveParts,
			initialPhysicalRows,
			baselineActiveParts,
			baselinePhysicalRows,
			activeParts,
			physicalRows,
		)
	}

	proofCtx := clickhouse.Context(
		ctx,
		clickhouse.WithQueryID("manifest-million-head-proof"),
	)
	got, found, err := db.loadLatestManifestRecord(proofCtx)
	if err != nil || !found || got.RowDigest != latest.RowDigest {
		t.Fatalf("bounded latest found=%t revision=%d err=%v", found, got.Revision, err)
	}
	if err := db.conn.Exec(ctx, `SYSTEM FLUSH LOGS`); err != nil {
		t.Fatal(err)
	}
	readMetrics := func(queryID string) (uint64, uint64) {
		t.Helper()
		var readRows, resultRows uint64
		if err := db.conn.QueryRow(
			ctx,
			`SELECT read_rows, result_rows
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_id = ?
ORDER BY event_time_microseconds DESC
LIMIT 1`,
			queryID,
		).Scan(&readRows, &resultRows); err != nil {
			t.Fatal(err)
		}
		return readRows, resultRows
	}
	baselineReadRows, baselineResultRows := readMetrics(
		"manifest-baseline-head-proof",
	)
	readRows, resultRows := readMetrics("manifest-million-head-proof")
	t.Logf(
		"manifest scale initial=%d/%d baseline=%d/%d inflated=%d/%d reads=%d/%d->%d/%d",
		initialActiveParts,
		initialPhysicalRows,
		baselineActiveParts,
		baselinePhysicalRows,
		activeParts,
		physicalRows,
		baselineReadRows,
		baselineResultRows,
		readRows,
		resultRows,
	)
	if baselineReadRows == 0 ||
		readRows > 512 ||
		readRows > baselineReadRows*2 ||
		baselineResultRows > manifestLatestReadLimit ||
		resultRows > manifestLatestReadLimit {
		t.Fatalf(
			"manifest bounded baseline=%d/%d inflated=%d/%d",
			baselineReadRows,
			baselineResultRows,
			readRows,
			resultRows,
		)
	}

	rows, err := db.conn.Query(
		ctx,
		"EXPLAIN indexes=1 "+boundedManifestHeadQuery,
	)
	if err != nil {
		t.Fatal(err)
	}
	var explainLines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		explainLines = append(explainLines, line)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	explain := strings.Join(explainLines, "\n")
	if !strings.Contains(explain, "PrimaryKey") ||
		!strings.Contains(explain, "Granules") {
		t.Fatalf("manifest EXPLAIN lacks PK/granule pruning:\n%s", explain)
	}
	t.Logf(
		"multipart manifest proof: initial_active_parts=%d initial_physical_rows=%d baseline_range_parts=4 baseline_revision_ranges=100..1099 baseline_active_parts=%d baseline_physical_rows=%d inflated_range_parts=4 inflated_revision_ranges=10000..1009999 active_parts=%d physical_rows=%d baseline_read_rows=%d baseline_result_rows=%d inflated_read_rows=%d inflated_result_rows=%d\n%s",
		initialActiveParts,
		initialPhysicalRows,
		baselineActiveParts,
		baselinePhysicalRows,
		activeParts,
		physicalRows,
		baselineReadRows,
		baselineResultRows,
		readRows,
		resultRows,
		explain,
	)
}

func insertNativeManifestRecord(
	t *testing.T,
	ctx context.Context,
	db *DB,
	record manifestRecord,
) {
	t.Helper()
	raw, err := manifestDBRowFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := db.conn.PrepareBatch(ctx, "INSERT INTO clicksync.dataset_manifest")
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.AppendStruct(&raw); err != nil {
		_ = batch.Abort()
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}
