package normalize

import (
	"fmt"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const (
	maxAddressSize   = 256
	maxAssetNameSize = 32
)

func rejectUnsupportedTransaction(tx lcommon.Transaction) error {
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

func transactionBodyCBOR(tx lcommon.Transaction) ([]byte, error) {
	switch value := tx.(type) {
	case *ledger.ByronTransaction:
		return value.Body.Cbor(), nil
	case *ledger.ShelleyTransaction:
		return value.Body.Cbor(), nil
	case *ledger.AllegraTransaction:
		return value.Body.Cbor(), nil
	case *ledger.MaryTransaction:
		return value.Body.Cbor(), nil
	case *ledger.AlonzoTransaction:
		return value.Body.Cbor(), nil
	case *ledger.BabbageTransaction:
		return value.Body.Cbor(), nil
	case *ledger.ConwayTransaction:
		return value.Body.Cbor(), nil
	case *ledger.DijkstraTransaction:
		return value.Body.Cbor(), nil
	default:
		return nil, fmt.Errorf("unsupported concrete transaction type %T", tx)
	}
}

func verifyTransactionID(decoded lcommon.Blake2b256, bodyCBOR []byte) error {
	computed := lcommon.Blake2b256Hash(bodyCBOR)
	if computed != decoded {
		return fmt.Errorf(
			"transaction ID mismatch: decoded %s, computed %s",
			decoded,
			computed,
		)
	}
	return nil
}
