package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func authorityLoaderObservation(
	present bool,
	revision uint64,
	discovery byte,
	latest byte,
	predecessor byte,
) authorityHeadObservation {
	return authorityHeadObservation{
		Present:                present,
		Revision:               revision,
		HasPredecessor:         present && revision > 1,
		DiscoveryFingerprint:   authorityFill32(discovery),
		LatestFingerprint:      authorityFill32(latest),
		PredecessorFingerprint: authorityFill32(predecessor),
	}
}

func authorityLoaderAttemptReader(
	t *testing.T,
	attempts []authorityHeadAttempt,
) authorityHeadAttemptReader {
	t.Helper()
	index := 0
	return func(context.Context) (authorityHeadAttempt, error) {
		if index >= len(attempts) {
			t.Fatalf("authority attempt reader exhausted after %d reads", index)
		}
		result := attempts[index]
		index++
		return result, nil
	}
}

func TestAuthorityManifestSQLUsesIndependentBoundedExactGroups(t *testing.T) {
	for name, sql := range map[string]string{
		"discovery": manifestDiscoverySQL,
		"revision":  manifestRevisionSQL,
	} {
		for _, forbidden := range []string{"argMax", "max(", "count(", "FINAL"} {
			if strings.Contains(sql, forbidden) {
				t.Fatalf("%s SQL contains %q: %s", name, forbidden, sql)
			}
		}
		if !strings.Contains(sql, "LIMIT 9") ||
			!strings.Contains(sql, "PREWHERE manifest_key = 1") {
			t.Fatalf("%s SQL is not bounded/exact: %s", name, sql)
		}
	}
	if !strings.Contains(manifestRevisionSQL, "AND revision = ?") {
		t.Fatalf("revision SQL is not independently exact-keyed: %s", manifestRevisionSQL)
	}
}

func TestAuthorityRevisionGroupReplayBoundsAndConflict(t *testing.T) {
	record := validOfficialGenesisAuthority(t)
	for _, count := range []int{1, manifestPhysicalReplayLimit} {
		rows := make([]authorityRecord, count)
		for index := range rows {
			rows[index] = record
		}
		got, err := validateAuthorityRevisionRecords(rows, record.Revision, "latest")
		if err != nil || got.RowDigest != record.RowDigest {
			t.Fatalf("%d identical rows: got=%+v err=%v", count, got, err)
		}
	}
	nine := make([]authorityRecord, manifestRawReadLimit)
	for index := range nine {
		nine[index] = record
	}
	if _, err := validateAuthorityRevisionRecords(
		nine,
		record.Revision,
		"latest",
	); err == nil {
		t.Fatal("ninth physical manifest replay was accepted")
	}

	conflict := record
	conflict.TrustReason = "different but individually canonical"
	if err := finalizeAuthorityRecord(&conflict); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(conflict); err != nil {
		t.Fatalf("conflicting fixture is not individually valid: %v", err)
	}
	if _, err := validateAuthorityRevisionRecords(
		[]authorityRecord{record, conflict},
		record.Revision,
		"latest",
	); err == nil {
		t.Fatal("conflicting physical manifest rows were accepted")
	}
}

func TestAuthorityRevisionGroupRejectsWrongRevisionAndDigest(t *testing.T) {
	record := validOfficialGenesisAuthority(t)
	if _, err := validateAuthorityRevisionRecords(
		[]authorityRecord{record},
		record.Revision+1,
		"latest",
	); err == nil {
		t.Fatal("wrong exact revision was accepted")
	}
	corrupt := record
	corrupt.RowDigest[0] ^= 1
	if _, err := validateAuthorityRevisionRecords(
		[]authorityRecord{corrupt},
		record.Revision,
		"latest",
	); err == nil {
		t.Fatal("corrupt canonical digest was accepted")
	}
}

func TestAuthorityPredecessorRequiresExactDigestAndImmutableIdentity(t *testing.T) {
	predecessor := validOfficialGenesisAuthority(t)
	latest := predecessor
	latest.Revision = 2
	latest.TransitionKind = "physical_reconcile"
	previousDigest := predecessor.RowDigest
	latest.PreviousRowDigest = &previousDigest
	latest.UpdatedAt = latest.UpdatedAt.Add(time.Microsecond)
	if err := finalizeAuthorityRecord(&latest); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(latest); err != nil {
		t.Fatalf("latest fixture is not canonical: %v", err)
	}
	if err := validateAuthorityPredecessor(latest, &predecessor); err != nil {
		t.Fatalf("exact predecessor rejected: %v", err)
	}
	if err := validateAuthorityPredecessor(latest, nil); err == nil {
		t.Fatal("missing predecessor accepted")
	}

	wrongDigest := latest
	digest := authorityFill32(0x91)
	wrongDigest.PreviousRowDigest = &digest
	if err := finalizeAuthorityRecord(&wrongDigest); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(wrongDigest); err != nil {
		t.Fatalf("wrong-digest fixture is not canonical: %v", err)
	}
	if err := validateAuthorityPredecessor(
		wrongDigest,
		&predecessor,
	); err == nil {
		t.Fatal("wrong predecessor digest accepted")
	}

	wrongIdentity := predecessor
	wrongIdentity.NetworkName = "different-mainnet-name"
	if err := finalizeAuthorityRecord(&wrongIdentity); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(wrongIdentity); err != nil {
		t.Fatalf("wrong-identity predecessor is not canonical: %v", err)
	}
	identityBoundLatest := latest
	identityDigest := wrongIdentity.RowDigest
	identityBoundLatest.PreviousRowDigest = &identityDigest
	if err := finalizeAuthorityRecord(&identityBoundLatest); err != nil {
		t.Fatal(err)
	}
	if err := validateAuthorityPredecessor(
		identityBoundLatest,
		&wrongIdentity,
	); err == nil {
		t.Fatal("predecessor immutable identity mismatch accepted")
	}
}

func TestAuthorityRevisionRawReplaysMustBeByteEquivalent(t *testing.T) {
	digest := authorityFill32(0x31)
	rows := []authorityDBRow{
		{ManifestKey: 1, Revision: 1, RowDigest: string(digest[:])},
		{
			ManifestKey: 1,
			Revision:    1,
			RowDigest:   string(digest[:]),
			TrustReason: "same digest but different raw row",
		},
	}
	if err := validateAuthorityRawRevisionReplays(rows, "latest"); err == nil {
		t.Fatal("non-byte-equivalent raw replays were accepted")
	}
	rows[1] = rows[0]
	if err := validateAuthorityRawRevisionReplays(rows, "latest"); err != nil {
		t.Fatalf("byte-equivalent raw replays rejected: %v", err)
	}
}

func TestAuthorityHeadObservationFingerprintsEveryRawGroup(t *testing.T) {
	latestDigest := authorityFill32(0x11)
	predecessorDigest := authorityFill32(0x22)
	discovery := []authorityManifestDiscoveryRow{{
		Revision:  2,
		RowDigest: string(latestDigest[:]),
	}}
	latest := []authorityDBRow{{
		ManifestKey: 1,
		Revision:    2,
		RowDigest:   string(latestDigest[:]),
		TrustReason: "first",
	}}
	predecessor := []authorityDBRow{{
		ManifestKey: 1,
		Revision:    1,
		RowDigest:   string(predecessorDigest[:]),
	}}
	base, err := makeAuthorityHeadObservation(discovery, latest, predecessor)
	if err != nil {
		t.Fatal(err)
	}

	changedLatest := append([]authorityDBRow(nil), latest...)
	changedLatest[0].TrustReason = "changed with the same row digest"
	changed, err := makeAuthorityHeadObservation(
		discovery,
		changedLatest,
		predecessor,
	)
	if err != nil {
		t.Fatal(err)
	}
	if base == changed || base.LatestFingerprint == changed.LatestFingerprint {
		t.Fatal("same-max same-digest raw content change was not observed")
	}

	changedPredecessor := append([]authorityDBRow(nil), predecessor...)
	changedPredecessor[0].TrustReason = "changed predecessor"
	changed, err = makeAuthorityHeadObservation(
		discovery,
		latest,
		changedPredecessor,
	)
	if err != nil {
		t.Fatal(err)
	}
	if base == changed ||
		base.PredecessorFingerprint == changed.PredecessorFingerprint {
		t.Fatal("raw predecessor group change was not observed")
	}

	changedDiscovery := append([]authorityManifestDiscoveryRow(nil), discovery...)
	changedDiscoveryDigest := authorityFill32(0x33)
	changedDiscovery[0].RowDigest = string(changedDiscoveryDigest[:])
	changed, err = makeAuthorityHeadObservation(
		changedDiscovery,
		latest,
		predecessor,
	)
	if err != nil {
		t.Fatal(err)
	}
	if base == changed ||
		base.DiscoveryFingerprint == changed.DiscoveryFingerprint {
		t.Fatal("raw discovery change was not observed")
	}
}

func TestAuthorityHeadObservationFingerprintIsPhysicalOrderIndependent(t *testing.T) {
	discovery := []authorityManifestDiscoveryRow{
		{Revision: 2, RowDigest: "b"},
		{Revision: 2, RowDigest: "a"},
	}
	latest := []authorityDBRow{
		{ManifestKey: 1, Revision: 2, RowDigest: "a"},
		{ManifestKey: 1, Revision: 2, RowDigest: "b"},
	}
	first, err := makeAuthorityHeadObservation(discovery, latest, nil)
	if err != nil {
		t.Fatal(err)
	}
	latest[0], latest[1] = latest[1], latest[0]
	second, err := makeAuthorityHeadObservation(discovery, latest, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("exact raw group fingerprint depends on physical row order")
	}
}

func TestAuthorityDiscoveryRejectsRevisionZeroAndRevisionOneLowerHistory(
	t *testing.T,
) {
	digest := authorityFill32(0x41)
	for _, revisionOneRows := range []int{1, manifestPhysicalReplayLimit} {
		t.Run(fmt.Sprintf("%d_revision_one_rows", revisionOneRows), func(t *testing.T) {
			discovery := make(
				[]authorityManifestDiscoveryRow,
				0,
				revisionOneRows+1,
			)
			for range revisionOneRows {
				discovery = append(discovery, authorityManifestDiscoveryRow{
					Revision:  1,
					RowDigest: string(digest[:]),
				})
			}
			discovery = append(discovery, authorityManifestDiscoveryRow{
				Revision:  0,
				RowDigest: string(digest[:]),
			})
			observation, err := makeAuthorityHeadObservation(discovery, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			shapeErr := validateAuthorityDiscoveryRows(discovery, observation)
			if shapeErr == nil {
				t.Fatalf(
					"%d revision-one replays plus revision zero were accepted",
					revisionOneRows,
				)
			}

			_, err = stabilizeAuthorityHead(
				context.Background(),
				authorityLoaderAttemptReader(t, []authorityHeadAttempt{
					{
						Observation:   observation,
						Found:         true,
						ValidationErr: invalidAuthorityError(shapeErr),
					},
					{
						Observation:   observation,
						Found:         true,
						ValidationErr: invalidAuthorityError(shapeErr),
					},
				}),
				func(context.Context, authorityHeadAttempt) (int, error) {
					t.Fatal("resolver called for revision-zero history")
					return 0, nil
				},
			)
			if !errors.Is(err, ErrInvalidDataset) {
				t.Fatalf("stable revision-zero history error = %v", err)
			}
		})
	}
}

func TestStabilizeAuthorityHeadReturnsOnlyStableSemanticError(t *testing.T) {
	bad := invalidAuthorityError(errors.New("semantic corruption"))
	observation := authorityLoaderObservation(true, 7, 1, 2, 3)
	_, err := stabilizeAuthorityHead(
		context.Background(),
		authorityLoaderAttemptReader(t, []authorityHeadAttempt{
			{Observation: observation, Found: true, ValidationErr: bad},
			{Observation: observation, Found: true, ValidationErr: bad},
		}),
		func(context.Context, authorityHeadAttempt) (int, error) {
			t.Fatal("resolver called for invalid authority")
			return 0, nil
		},
	)
	if !errors.Is(err, bad) {
		t.Fatalf("stable semantic error = %v", err)
	}
	if !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("stable semantic error is not dataset corruption: %v", err)
	}
}

func TestStabilizeAuthorityHeadRetriesErrorWhenObservationChanges(t *testing.T) {
	bad := errors.New("transient old-head corruption")
	old := authorityLoaderObservation(true, 7, 1, 2, 3)
	next := authorityLoaderObservation(true, 8, 4, 5, 6)
	value, err := stabilizeAuthorityHead(
		context.Background(),
		authorityLoaderAttemptReader(t, []authorityHeadAttempt{
			{Observation: old, Found: true, ValidationErr: bad},
			{Observation: next, Found: true},
			{Observation: next, Found: true},
			{Observation: next, Found: true},
		}),
		func(context.Context, authorityHeadAttempt) (int, error) {
			return 42, nil
		},
	)
	if err != nil || value != 42 {
		t.Fatalf("changed observation result = %d, %v", value, err)
	}
}

func TestStabilizeAuthorityHeadRetriesCorruptToValidSameMax(t *testing.T) {
	corrupt := authorityLoaderObservation(true, 9, 1, 2, 3)
	valid := corrupt
	valid.LatestFingerprint = authorityFill32(4)
	value, err := stabilizeAuthorityHead(
		context.Background(),
		authorityLoaderAttemptReader(t, []authorityHeadAttempt{
			{
				Observation:   corrupt,
				Found:         true,
				ValidationErr: errors.New("conflicting lower digest"),
			},
			{Observation: valid, Found: true},
			{Observation: valid, Found: true},
			{Observation: valid, Found: true},
		}),
		func(context.Context, authorityHeadAttempt) (string, error) {
			return "stable", nil
		},
	)
	if err != nil || value != "stable" {
		t.Fatalf("same-max repair result = %q, %v", value, err)
	}
}

func TestStabilizeAuthorityHeadRetriesPresenceChange(t *testing.T) {
	present := authorityLoaderObservation(true, 3, 1, 2, 3)
	absent := authorityLoaderObservation(false, 0, 4, 0, 0)
	value, err := stabilizeAuthorityHead(
		context.Background(),
		authorityLoaderAttemptReader(t, []authorityHeadAttempt{
			{
				Observation:   present,
				Found:         true,
				ValidationErr: errors.New("disappearing row"),
			},
			{Observation: absent},
			{Observation: absent},
			{Observation: absent},
		}),
		func(_ context.Context, attempt authorityHeadAttempt) (bool, error) {
			return attempt.Found, nil
		},
	)
	if err != nil || value {
		t.Fatalf("stable absence result = %t, %v", value, err)
	}
}

func TestStabilizeAuthorityHeadRetainsResolverErrorUntilStable(t *testing.T) {
	old := authorityLoaderObservation(true, 3, 1, 2, 3)
	next := authorityLoaderObservation(true, 4, 4, 5, 6)
	notFound := errors.New("selection not found")
	calls := 0
	_, err := stabilizeAuthorityHead(
		context.Background(),
		authorityLoaderAttemptReader(t, []authorityHeadAttempt{
			{Observation: old, Found: true},
			{Observation: next, Found: true},
			{Observation: next, Found: true},
			{Observation: next, Found: true},
		}),
		func(context.Context, authorityHeadAttempt) (int, error) {
			calls++
			return 0, notFound
		},
	)
	if !errors.Is(err, notFound) || calls != 2 {
		t.Fatalf("stable resolver error = %v, calls=%d", err, calls)
	}
}

func TestStabilizeAuthorityHeadChurnEndsOnlyWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reads := 0
	read := func(context.Context) (authorityHeadAttempt, error) {
		reads++
		if reads == 8 {
			cancel()
		}
		marker := byte(reads)
		return authorityHeadAttempt{
			Observation: authorityLoaderObservation(
				true,
				uint64(reads),
				marker,
				marker,
				marker,
			),
			Found: true,
		}, nil
	}
	_, err := stabilizeAuthorityHead(
		ctx,
		read,
		func(context.Context, authorityHeadAttempt) (int, error) {
			return 1, nil
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("churn error = %v after %d reads", err, reads)
	}
}

func TestStabilizeAuthorityHeadPropagatesInfrastructureError(t *testing.T) {
	infrastructure := errors.New("clickhouse unavailable")
	_, err := stabilizeAuthorityHead(
		context.Background(),
		func(context.Context) (authorityHeadAttempt, error) {
			return authorityHeadAttempt{}, infrastructure
		},
		func(context.Context, authorityHeadAttempt) (int, error) {
			t.Fatal("resolver called after infrastructure failure")
			return 0, nil
		},
	)
	if !errors.Is(err, infrastructure) || errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("infrastructure error was reclassified: %v", err)
	}
}

func TestSnapshotUnavailableErrorIsTyped(t *testing.T) {
	err := &SnapshotUnavailableError{Reason: "manifest absent"}
	if !errors.Is(err, ErrSnapshotUnavailable) ||
		!strings.Contains(err.Error(), "manifest absent") {
		t.Fatalf("unavailable error is not typed/diagnostic: %v", err)
	}
}

func TestStabilizeAuthorityHeadHonorsAlreadyCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := stabilizeAuthorityHead(
		ctx,
		func(context.Context) (authorityHeadAttempt, error) {
			t.Fatal("reader called after cancellation")
			return authorityHeadAttempt{}, nil
		},
		func(context.Context, authorityHeadAttempt) (time.Time, error) {
			return time.Time{}, nil
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled stable load = %v", err)
	}
}
