# Final Correctness Audit Update

This report reflects the current implementation state after the final seven-issue audit pass.

## Issues Addressed

| Issue | Status | Evidence |
| --- | --- | --- |
| Reconciliation gaps were represented as ledger transactions | FIXED | `IngestService` now saves only observed transactions to the ledger and records balance gaps as `Discrepancy` records. `IngestServiceTest.testReconciliationRecordIsDeterministic` asserts two observed ledger rows and one discrepancy. |
| Merchant key contradiction in deduplication | FIXED | Ingest-time deduplication intentionally excludes merchant because SMS/email names can differ. Same-channel different-body messages remain distinct. `testSmsAndEmailForSameTransactionMergeDespiteMerchantDifferences` covers the cross-channel merge. |
| DynamoDB race and compatibility gaps | FIXED | `DynamoDbLedgerStore` preflights and rereads message pointers after conditional-write failures, validates category and merchant compatibility, and requires target transaction existence before update. Integration tests cover concurrent idempotency, atomic message-index collision, pagination, direct Dynamo ingestion, and conflicting category rejection. |
| End-to-end coverage only used backfill before consistency | FIXED | `EndToEndTest` now ingests corpora directly into DynamoDB before running SQL backfill and consistency verification. |
| Perf harness hid errors and lacked measured counters | FIXED | `PerfTest` now fails on write errors, uses local DynamoDB credentials/region, isolates each run, and prints real Query `ScannedCount`/`Count` metrics plus deterministic GetItem call counts. `runPerf` is available as a Gradle task. |
| Incident blast radius was missing | FIXED | The legacy whole-rupee amount bug affects exactly 38 messages in `fixtures/corpus-a.jsonl`: whole-rupee transaction amount first, decimal balance/limit later. `AmountsTest.corpusABlastRadiusForLegacyWholeRupeeBugIsExact` locks this count. |
| Docs were stale or contradictory | FIXED | README, decision log, and incident report now state that reconciliation stays outside ledger transactions and that perf metrics must come from the harness output. |

## Verification Notes

Local verification and CI status should be taken from the newest commit and workflow run. `verify.sh` intentionally compiles the pure-JDK path only and excludes the DynamoDB implementation plus adapter because they depend on AWS SDK jars resolved by Gradle.

## Remaining Limits

- DynamoDB Local must be available for integration tests and `runPerf`.
- DynamoDB `GetItem` does not expose `Count` or `ScannedCount`; the perf harness reports point-read call counts for message lookup instead of inventing scan counters.
