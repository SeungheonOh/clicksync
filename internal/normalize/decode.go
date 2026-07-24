package normalize

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"

	"cardano-clicksync/internal/model"
)

// Decode decodes one already-agreed raw block exactly once and immediately
// projects it to SQL-facing facts. Every gOuroboros validation facility is
// disabled; failures here are CBOR decoding or fact representability errors.
func Decode(blockType uint, raw []byte) (model.Block, error) {
	if len(raw) == 0 {
		return model.Block{}, errors.New("empty raw block CBOR")
	}
	block, err := ledger.NewBlockFromCbor(
		blockType,
		raw,
		lcommon.VerifyConfig{
			SkipBodyHashValidation:    true,
			SkipTransactionValidation: true,
			SkipStakePoolValidation:   true,
		},
	)
	if err != nil {
		return model.Block{}, fmt.Errorf("decode block type %d: %w", blockType, err)
	}
	return Bundle(block)
}
