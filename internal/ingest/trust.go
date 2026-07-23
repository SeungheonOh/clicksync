package ingest

import (
	"context"
	"time"

	"clicksync/internal/n2n"
	"clicksync/internal/store"
	"clicksync/internal/syncer"
	"clicksync/internal/writerlock"
)

type trustController struct {
	db          *store.DB
	lock        *writerlock.Lock
	writerID    [16]byte
	writerBuild string
}

func (controller *trustController) BeginCheck(
	ctx context.Context,
	expected *n2n.ChainPoint,
	required int,
	at time.Time,
) (syncer.CheckIdentity, error) {
	return controller.db.BeginTrustCheck(
		ctx,
		controller.lock,
		expected,
		required,
		at,
		controller.writerID,
		controller.writerBuild,
	)
}

func (controller *trustController) FinalizeCheck(
	ctx context.Context,
	check syncer.CheckIdentity,
	forceDispute bool,
	reason string,
	at time.Time,
) (syncer.TrustResolution, error) {
	return controller.db.FinalizeTrustCheck(
		ctx,
		controller.lock,
		check,
		forceDispute,
		reason,
		at,
		controller.writerID,
		controller.writerBuild,
	)
}
