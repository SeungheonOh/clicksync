import type { Block, Point, Transaction, TransactionOutputReference } from '@cardano-ogmios/schema'

export type ChainPoint = Point | 'origin'

export interface BlockRow {
  network_magic: number
  block_hash: string
  previous_block_hash: string | null
  slot: number | null
  block_number: number
  era: string
  block_type: string
  size_bytes: number | null
  tx_count: number
  issuer_vkey: string | null
  ingest_seq: string
}

export interface TransactionRow {
  network_magic: number
  block_hash: string
  block_number: number
  tx_hash: string
  tx_index: number
  parent_tx_hash: string | null
  subtx_index: number | null
  is_valid: boolean
  is_applied: boolean
  fee_lovelace: string | null
  invalid_before: number | null
  invalid_after: number | null
  metadata_json: string | null
  certificates_json: string | null
  withdrawals_json: string | null
  proposals_json: string | null
  votes_json: string | null
  tx_cbor: string | null
  ingest_seq: string
}

export interface TxInputRow {
  network_magic: number
  block_hash: string
  block_number: number
  tx_hash: string
  input_kind: 'spend' | 'collateral' | 'reference'
  input_index: number
  source_tx_hash: string
  source_output_index: number
  is_consumed: boolean
  ingest_seq: string
}

export interface TxOutputRow {
  network_magic: number
  block_hash: string
  block_number: number
  tx_hash: string
  output_index: number
  output_kind: 'regular' | 'collateral_return'
  address: string
  lovelace: string
  datum_hash: string | null
  inline_datum: string | null
  reference_script_json: string | null
  is_produced: boolean
  ingest_seq: string
}

export interface OutputAssetRow {
  network_magic: number
  block_hash: string
  block_number: number
  tx_hash: string
  output_index: number
  policy_id: string
  asset_name: string
  quantity: string
  is_produced: boolean
  ingest_seq: string
}

export interface MintAssetRow {
  network_magic: number
  block_hash: string
  block_number: number
  tx_hash: string
  policy_id: string
  asset_name: string
  quantity: string
  is_applied: boolean
  ingest_seq: string
}

export interface ChainEventRow {
  network_magic: number
  event_seq: string
  block_hash: string
  slot: number | null
  block_number: number
  is_canonical: boolean
  rollback_id: string | null
  writer_id: string
}

export interface RollbackRow {
  network_magic: number
  rollback_id: string
  rollback_to_hash: string | null
  rollback_to_slot: number | null
  old_tip_hash: string | null
  old_tip_slot: number | null
  depth: number
  event_seq: string
  reason: 'chain_sync' | 'intersection'
  writer_id: string
}

export interface CanonicalPointRow {
  block_hash: string
  slot: number | null
  block_number: number
  canonical_event_seq: string
}

export interface NormalizedBlock {
  point: Exclude<ChainPoint, 'origin'> | null
  block: BlockRow
  transactions: TransactionRow[]
  inputs: TxInputRow[]
  outputs: TxOutputRow[]
  outputAssets: OutputAssetRow[]
  mintAssets: MintAssetRow[]
}

export interface NormalizeContext {
  networkMagic: number
  eventSeq: bigint
}

export type OgmiosBlock = Block
export type OgmiosTransaction = Transaction
export type OgmiosInput = TransactionOutputReference
