# Final Correctness Audit Update

This report supersedes earlier optimistic claims. The latest audit found that
several previous fixes were incomplete: transaction identity still drifted when a
lower-sorting source ID arrived, SQL legacy migration collapsed ambiguous rows,
`ConsistencyChecker` was still query-specific, and backfill timeout state did
not prove worker termination.

## Issues Addressed

| Issue | Severity | Location | Status | Evidence of Fix |
|-------|----------|----------|--------|-----------------|
| SQL identity drift after additional source IDs | P0 | `SqlLedgerStore`, `V4__source_message_index.sql` | FIXED | Source-message index retains existing `txn_id`; regression covers lower-sorting source ID through a second store instance. |
| Legacy migration could merge independent identical visible rows | P0 | `SqlLedgerStore.migrate` | FIXED | New identity is source-set based; identical visible rows with different source IDs are preserved. |
| DynamoDB save computed a new key before checking existing message IDs | P1 | `DynamoDbLedgerStore.save` | PARTIALLY FIXED | Preflight message-index lookup now routes overlapping source IDs to the existing document key. Integration tests were skipped locally because DynamoDB Local was unavailable. |
| Consistency checker was query-specific and positional | P1 | `ConsistencyChecker.check` | FIXED | Checker now compares complete SQL/document snapshots and reports deterministic SQL-only, document-only, duplicate, source-collision, and field mismatch divergences. |
| Backfill timeout did not expose executor termination | P1 | `Backfill.run` | FIXED | `Result` includes lifecycle status and executor termination; tests cover interruptible and interruption-ignoring workers. |
| Reconciliation synthetic ID used insufficient evidence | P0 | `IngestService` | PARTIALLY FIXED | ID now includes direction, amount, balance-gap reason, checkpoint source IDs, and deterministic gap index. Frozen `Category` prevents a dedicated reconciliation category. |

## Verification

- `gradle clean test`: passed locally under native Gradle using JDK 17. Test report: 33 tests, 0 failures, 5 skipped DynamoDB integration tests.
- Exact `bash -lc './verify.sh'`: unavailable in this Windows environment because `/bin/bash` is missing.
- Native equivalent of `verify.sh`: passed. It compiles all main Java files except `DynamoDbLedgerStore.java` and runs `SelfCheck`.

## Remaining Limitations

- Local Java is JDK 17, while the requested target is Java 21.
- DynamoDB Local integration tests were skipped, so DynamoDB behavior is compile-verified and unit-reasoned here, not integration-verified.
- The frozen `Category` enum prevents representing reconciliation adjustments with a dedicated category; synthetic records are labeled through merchant/source metadata instead.
