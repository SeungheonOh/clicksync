package genesis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"clicksync/internal/model"
	"clicksync/internal/publication"
	"clicksync/internal/store"
)

type State interface {
	GenesisState(context.Context) (startOrigin bool, seeded bool, err error)
	RecoverGenesisPublication(context.Context, model.Hash32) (store.GenesisPublication, bool, error)
	MarkGenesisSeeded(context.Context, store.LockAssertion, []store.GenesisPublication, [16]byte, string, time.Time) error
}

type Publisher interface {
	PublishBatch(context.Context, publication.Batch) (publication.BatchResult, error)
}

// EnsurePublished is restart-safe across orphan fact attempts, a lost adoption
// response, and a crash between adoption and the complete-history marker.
func EnsurePublished(
	ctx context.Context,
	state State,
	publisher Publisher,
	lock store.LockAssertion,
	bundle Bundle,
	writerID [16]byte,
	writerBuild string,
	now time.Time,
) error {
	if state == nil || publisher == nil || lock == nil {
		return errors.New("genesis state, publisher, and writer flock are required")
	}
	origin, seeded, err := state.GenesisState(ctx)
	if err != nil {
		return err
	}
	if !origin {
		return errors.New("official genesis cannot seed a partial-history dataset")
	}
	digest, err := publication.FactsDigest(bundle.Block, bundle.Source)
	if err != nil {
		return fmt.Errorf("digest official genesis facts: %w", err)
	}
	genesisPublication, found, err := state.RecoverGenesisPublication(ctx, digest)
	if err != nil {
		return err
	}
	if seeded {
		if !found {
			return errors.New("genesis marker exists without the exact active distribution")
		}
		return nil
	}
	if !found {
		result, err := publisher.PublishBatch(ctx, publication.Batch{
			Items: []publication.BatchItem{{
				Block:  bundle.Block,
				Source: bundle.Source,
			}},
			FirstStagedAt: now.UTC(),
		})
		if err != nil {
			return err
		}
		if len(result.PublicationIDs) != 1 {
			return errors.New("official genesis publication returned an invalid identity set")
		}
		genesisPublication = store.GenesisPublication{
			PublicationID:    result.PublicationIDs[0],
			FactsDigest:      digest,
			TransactionCount: AVVMEntries,
			OutputCount:      AVVMEntries,
			InitialSupply:    InitialSupply,
		}
	}
	return state.MarkGenesisSeeded(
		ctx,
		lock,
		[]store.GenesisPublication{genesisPublication},
		writerID,
		writerBuild,
		now.UTC(),
	)
}
