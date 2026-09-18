# Final Correctness Audit — Current State

This report reflects the implementation state after the complete audit and fix pass.
Current commit inspected: `834fd623` (parent), changes applied in this pass.

## Issues Confirmed and Fixed in This Pass

| Issue | Severity | Evidence |
| --- | --- | --- |
| `categorizeTransfers()` used `BigDecimal.equals()` (scale-sensitive) | P1 | `IngestService.java` line 287 changed from `.equals()` to `.compareTo() == 0`. `transferDetectionMatchesDifferentScalesViaCompareTo` test added. |
| `SqlLedgerStore.validateCompatibleIdentity()` did not check category or merchant | P1 | Lines 224–231 updated to match `DynamoDbLedgerStore.isCompatible()`. `conflictingCategoryForOwnedSourceIsRejected` and `conflictingMerchantForOwnedSourceIsRejected` tests added to `SqlLedgerStoreTest`. |
| Stale `Reports.java` class comment claimed reconciliation not written | P2 | Updated to describe the correct implemented state. |
| Stale `README.md` "What is missing" section | P2 | Section replaced with "Current implementation state" listing all implemented components. `verify.sh` count claim corrected from 323 to 257. |
| Incident `INC-2026-09-11.md` status still "OPEN" | P2 | Status changed to RESOLVED with date and fix summary. |
| `DynamoDbLedgerAdapter` discrepancy in-memory design undocumented | P3 | Javadoc added explaining why DynamoDB has no durable discrepancy partition in this scope. |
| DECISION_LOG entry 10 references stale workflow run | P3 | Disclaimer added noting the CI run reference is historical; readers must check Actions tab. Entry 11 added for the BigDecimal and SQL compatibility fixes. |

## Previously Fixed Issues (Retained)

| Issue | Status |
| --- | --- |
| Reconciliation gaps synthesized as fake ledger transactions | FIXED — `IngestService` saves only observed transactions; balance gaps are `Discrepancy` records |
| UUID-based reconciliation identity | FIXED — deterministic discrepancy deduplication via account+epoch+amount key |
| `SqlLedgerStore.save()` DELETE-then-INSERT with no unique constraint | FIXED — MERGE INTO on txn_id + ledger_sources ownership index |
| No idempotency test for repeated in-memory ingestion | FIXED — `testReingestionIdempotency` in `IngestServiceTest` |
| DynamoDB race on concurrent identical writes | FIXED — TransactWriteItems with conditional-write + retry on cancellation |
| End-to-end test only did backfill before consistency check | FIXED — `EndToEndTest` ingests corpora directly into DynamoDB first |
| PerfTest hid write errors and lacked measured counters | FIXED — fails on any write error; prints real `ScannedCount`/`Count` + GetItem call counts |
| Incident blast radius unverified | FIXED — exactly 38 corpus-a messages locked by `AmountsTest.corpusABlastRadiusForLegacyWholeRupeeBugIsExact` |

## Remaining Limitations (Acknowledged, Not Defects)

- DynamoDB Local must be running for integration tests and `runPerf`.
- Cross-run deduplication for messages that arrive in separate ingestion runs with no overlapping source IDs is not possible without additional bank reference data (documented in DECISION_LOG entry 1).
- `DynamoDbLedgerAdapter` holds discrepancies in memory only — no durable DynamoDB schema for discrepancies (documented in class Javadoc).
- Performance numbers in README are design expectations. Measured values require running `./gradlew runPerf` with DynamoDB Local active.
