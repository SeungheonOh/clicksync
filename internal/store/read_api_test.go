package store

import (
	"testing"
	"time"
)

func TestUniqueManifestIdentityNoRowsDuplicatesAndConflict(t *testing.T) {
	if identity, found, err := uniqueManifestIdentity(nil); err != nil || found ||
		identity != (ManifestIdentity{}) {
		t.Fatalf("empty identity result = %+v found=%t err=%v", identity, found, err)
	}
	identity := ManifestIdentity{
		DatasetID:    id16(0x11),
		NetworkMagic: 764824073,
		NetworkName:  "mainnet",
	}
	got, found, err := uniqueManifestIdentity([]ManifestIdentity{identity, identity})
	if err != nil || !found || got != identity {
		t.Fatalf("duplicate identity result = %+v found=%t err=%v", got, found, err)
	}
	conflict := identity
	conflict.DatasetID = id16(0x12)
	if _, _, err := uniqueManifestIdentity([]ManifestIdentity{identity, conflict}); err == nil {
		t.Fatal("conflicting latest manifest rows were accepted")
	}
}

func TestUniqueLatestWriterAuditNoRowsDuplicatesAndConflict(t *testing.T) {
	if status, found, err := uniqueLatestWriterAudit(nil); err != nil || found ||
		status != (WriterAuditStatus{}) {
		t.Fatalf("empty audit result = %+v found=%t err=%v", status, found, err)
	}
	released := time.Date(2026, 7, 23, 12, 0, 1, 123456000, time.UTC)
	status := WriterAuditStatus{
		DatasetID:     id16(0x21),
		Revision:      3,
		OwnerID:       id16(0x22),
		BuildID:       "test-build",
		State:         "released",
		HeartbeatAt:   released,
		ReleasedAt:    &released,
		ReleaseReason: "complete",
	}
	duplicate := status
	duplicateReleased := released
	duplicate.ReleasedAt = &duplicateReleased
	got, found, err := uniqueLatestWriterAudit([]WriterAuditStatus{status, duplicate})
	if err != nil || !found || !sameWriterAuditStatus(got, status) {
		t.Fatalf("duplicate audit result = %+v found=%t err=%v", got, found, err)
	}
	conflict := duplicate
	conflict.State = "active"
	conflict.ReleasedAt = nil
	if _, _, err := uniqueLatestWriterAudit([]WriterAuditStatus{status, conflict}); err == nil {
		t.Fatal("conflicting latest writer audit rows were accepted")
	}
}
