import type {
  Assets,
  SubTransaction,
  Transaction,
  TransactionOutput,
  TransactionOutputReference,
} from '@cardano-ogmios/schema'

import type {
  MintAssetRow,
  NormalizedBlock,
  NormalizeContext,
  OgmiosBlock,
  OutputAssetRow,
  TransactionRow,
  TxInputRow,
  TxOutputRow,
} from '../domain.js'

interface RowContext {
  networkMagic: number
  blockHash: string
  blockNumber: number
  ingestSeq: string
}

interface TransactionPosition {
  txIndex: number
  parentTxHash: string | null
  subtxIndex: number | null
}

interface NormalizedRows {
  transactions: TransactionRow[]
  inputs: TxInputRow[]
  outputs: TxOutputRow[]
  outputAssets: OutputAssetRow[]
  mintAssets: MintAssetRow[]
}

/** Normalize an Ogmios v7 block into immutable ClickHouse fact rows. */
export function normalizeBlock(block: OgmiosBlock, context: NormalizeContext): NormalizedBlock {
  const ingestSeq = context.eventSeq.toString()
  const blockContext: RowContext = {
    networkMagic: context.networkMagic,
    blockHash: block.id,
    blockNumber: block.height,
    ingestSeq,
  }
  const rows: NormalizedRows = {
    transactions: [],
    inputs: [],
    outputs: [],
    outputAssets: [],
    mintAssets: [],
  }
  const transactions = block.type === 'ebb' ? [] : (block.transactions ?? [])

  for (const [txIndex, transaction] of transactions.entries()) {
    const isValid = transaction.spends === 'inputs'
    appendTransaction(rows, blockContext, transaction, {
      txIndex,
      parentTxHash: null,
      subtxIndex: null,
    }, isValid, unsignedDecimalOrNull(transaction.fee?.ada.lovelace, 'transaction fee'))

    appendInputs(rows.inputs, blockContext, transaction.id, 'spend', transaction.inputs, isValid)
    appendInputs(
      rows.inputs,
      blockContext,
      transaction.id,
      'collateral',
      transaction.collaterals ?? [],
      !isValid,
    )
    appendInputs(
      rows.inputs,
      blockContext,
      transaction.id,
      'reference',
      transaction.references ?? [],
      false,
    )
    appendOutputs(rows, blockContext, transaction.id, transaction.outputs ?? [], 'regular', isValid)

    if (transaction.collateralReturn !== undefined) {
      appendOutput(
        rows,
        blockContext,
        transaction.id,
        transaction.outputs?.length ?? 0,
        transaction.collateralReturn,
        'collateral_return',
        !isValid,
      )
    }
    appendMint(rows.mintAssets, blockContext, transaction.id, transaction.mint, isValid)

    if (block.era === 'dijkstra') {
      for (const [subtxIndex, subTransaction] of (transaction.subTransactions ?? []).entries()) {
        appendSubTransaction(
          rows,
          blockContext,
          subTransaction,
          { txIndex, parentTxHash: transaction.id, subtxIndex },
          isValid,
        )
      }
    }
  }

  const hasSlot = block.type !== 'ebb'

  return {
    point: hasSlot ? { id: block.id, slot: block.slot } : null,
    block: {
      network_magic: context.networkMagic,
      block_hash: block.id,
      previous_block_hash: block.ancestor === 'genesis' ? null : block.ancestor,
      slot: hasSlot ? block.slot : null,
      block_number: block.height,
      era: block.era,
      block_type: block.type,
      size_bytes: hasSlot ? block.size.bytes : null,
      tx_count: transactions.length,
      issuer_vkey: hasSlot ? block.issuer.verificationKey : null,
      ingest_seq: ingestSeq,
    },
    ...rows,
  }
}

function appendSubTransaction(
  rows: NormalizedRows,
  context: RowContext,
  transaction: SubTransaction,
  position: TransactionPosition,
  isValid: boolean,
): void {
  appendTransaction(rows, context, transaction, position, isValid, null)
  appendInputs(rows.inputs, context, transaction.id, 'spend', transaction.inputs, isValid)
  appendInputs(
    rows.inputs,
    context,
    transaction.id,
    'reference',
    transaction.references ?? [],
    false,
  )
  appendOutputs(rows, context, transaction.id, transaction.outputs ?? [], 'regular', isValid)
  appendMint(rows.mintAssets, context, transaction.id, transaction.mint, isValid)
}

function appendTransaction(
  rows: NormalizedRows,
  context: RowContext,
  transaction: Transaction | SubTransaction,
  position: TransactionPosition,
  isValid: boolean,
  feeLovelace: string | null,
): void {
  rows.transactions.push({
    network_magic: context.networkMagic,
    block_hash: context.blockHash,
    block_number: context.blockNumber,
    tx_hash: transaction.id,
    tx_index: position.txIndex,
    parent_tx_hash: position.parentTxHash,
    subtx_index: position.subtxIndex,
    is_valid: isValid,
    is_applied: isValid,
    fee_lovelace: feeLovelace,
    invalid_before: transaction.validityInterval?.invalidBefore ?? null,
    invalid_after: transaction.validityInterval?.invalidAfter ?? null,
    metadata_json: jsonOrNull(transaction.metadata),
    certificates_json: jsonOrNull(transaction.certificates),
    withdrawals_json: jsonOrNull(transaction.withdrawals),
    proposals_json: jsonOrNull(transaction.proposals),
    votes_json: jsonOrNull(transaction.votes),
    tx_cbor: transaction.cbor ?? null,
    ingest_seq: context.ingestSeq,
  })
}

function appendInputs(
  rows: TxInputRow[],
  context: RowContext,
  txHash: string,
  kind: TxInputRow['input_kind'],
  inputs: readonly TransactionOutputReference[],
  isConsumed: boolean,
): void {
  for (const [inputIndex, input] of inputs.entries()) {
    rows.push({
      network_magic: context.networkMagic,
      block_hash: context.blockHash,
      block_number: context.blockNumber,
      tx_hash: txHash,
      input_kind: kind,
      input_index: inputIndex,
      source_tx_hash: input.transaction.id,
      source_output_index: input.index,
      is_consumed: isConsumed,
      ingest_seq: context.ingestSeq,
    })
  }
}

function appendOutputs(
  rows: NormalizedRows,
  context: RowContext,
  txHash: string,
  outputs: readonly TransactionOutput[],
  kind: TxOutputRow['output_kind'],
  isProduced: boolean,
): void {
  for (const [outputIndex, output] of outputs.entries()) {
    appendOutput(rows, context, txHash, outputIndex, output, kind, isProduced)
  }
}

function appendOutput(
  rows: NormalizedRows,
  context: RowContext,
  txHash: string,
  outputIndex: number,
  output: TransactionOutput,
  kind: TxOutputRow['output_kind'],
  isProduced: boolean,
): void {
  rows.outputs.push({
    network_magic: context.networkMagic,
    block_hash: context.blockHash,
    block_number: context.blockNumber,
    tx_hash: txHash,
    output_index: outputIndex,
    output_kind: kind,
    address: output.address,
    lovelace: unsignedDecimal(output.value.ada.lovelace, 'output lovelace'),
    datum_hash: output.datumHash ?? null,
    inline_datum: output.datum ?? null,
    reference_script_json: jsonOrNull(output.script),
    is_produced: isProduced,
    ingest_seq: context.ingestSeq,
  })

  for (const [policyId, assets] of Object.entries(output.value)) {
    if (policyId === 'ada') continue

    for (const [assetName, quantity] of Object.entries(assets)) {
      rows.outputAssets.push({
        network_magic: context.networkMagic,
        block_hash: context.blockHash,
        block_number: context.blockNumber,
        tx_hash: txHash,
        output_index: outputIndex,
        policy_id: policyId,
        asset_name: assetName,
        quantity: unsignedDecimal(quantity, 'output asset quantity'),
        is_produced: isProduced,
        ingest_seq: context.ingestSeq,
      })
    }
  }
}

function appendMint(
  rows: MintAssetRow[],
  context: RowContext,
  txHash: string,
  mint: Assets | undefined,
  isApplied: boolean,
): void {
  if (mint === undefined) return

  for (const [policyId, assets] of Object.entries(mint)) {
    for (const [assetName, quantity] of Object.entries(assets)) {
      rows.push({
        network_magic: context.networkMagic,
        block_hash: context.blockHash,
        block_number: context.blockNumber,
        tx_hash: txHash,
        policy_id: policyId,
        asset_name: assetName,
        quantity: signedDecimal(quantity, 'mint quantity'),
        is_applied: isApplied,
        ingest_seq: context.ingestSeq,
      })
    }
  }
}

const UINT64_MAX = (1n << 64n) - 1n
const INT64_MIN = -(1n << 63n)
const INT64_MAX = (1n << 63n) - 1n

function unsignedDecimal(value: bigint, label: string): string {
  if (value < 0n || value > UINT64_MAX) {
    throw new RangeError(`${label} is outside ClickHouse UInt64: ${value}`)
  }

  return value.toString(10)
}

function signedDecimal(value: bigint, label: string): string {
  if (value < INT64_MIN || value > INT64_MAX) {
    throw new RangeError(`${label} is outside ClickHouse Int64: ${value}`)
  }

  return value.toString(10)
}

function unsignedDecimalOrNull(value: bigint | undefined, label: string): string | null {
  return value === undefined ? null : unsignedDecimal(value, label)
}

function jsonOrNull(value: unknown): string | null {
  if (value === undefined) return null

  return JSON.stringify(value, (_key, nestedValue: unknown) =>
    typeof nestedValue === 'bigint' ? nestedValue.toString(10) : nestedValue,
  )
}
