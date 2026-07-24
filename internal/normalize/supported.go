package normalize

import (
	"fmt"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ensureSupportedTransaction limits projection to transaction shapes the fact
// schema can represent. This is a compatibility boundary, not ledger validity.
func ensureSupportedTransaction(tx lcommon.Transaction) error {
	switch tx.Type() {
	case ledger.TxTypeByron,
		ledger.TxTypeShelley,
		ledger.TxTypeAllegra,
		ledger.TxTypeMary,
		ledger.TxTypeAlonzo,
		ledger.TxTypeBabbage,
		ledger.TxTypeConway:
		return nil
	case ledger.TxTypeDijkstra:
		dijkstraTx, ok := tx.(*ledger.DijkstraTransaction)
		if !ok {
			return fmt.Errorf("Dijkstra transaction has unexpected concrete type %T", tx)
		}
		if count := len(dijkstraTx.Body.TxSubTransactions.Items()); count > 0 {
			return fmt.Errorf(
				"unsupported Dijkstra nested transaction semantics (%d sub-transactions)",
				count,
			)
		}
		return nil
	default:
		return fmt.Errorf("unsupported transaction type %d", tx.Type())
	}
}
