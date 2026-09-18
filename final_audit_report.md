# Final Correctness Audit - Current State

This report reflects the implementation state after the latest correctness pass.

## Confirmed Current Behavior

- `fixtures/corpus-a-totals.json` expects 256 observed transactions, not 257.
- The missing `7500.00` debit is represented as an allowed balance-gap discrepancy, not as a fabricated ledger transaction.
- `SelfCheck` fails on transaction-count mismatches, unexpected discrepancies, failed writes, or malformed input records.
- DynamoDB integration tests are required in CI; the workflow now fails if the DynamoDB integration suite is absent or skipped.

## Fixes in This Pass

| Area | Fix |
| --- | --- |
| Same-second grouping | Same-channel different-body messages remain separate; same-run grouping is limited to exact duplicates and SMS/email corroboration. |
| Same-second adversarial test | Same account, same bank timestamp, same direction, same amount, and same merchant still remain separate when same-channel message bodies differ. |
| Consistency comparison | SQL/document comparison now canonicalizes instant timestamps, source-message ID order, merchant formatting, and decimal values before reporting field divergence. |
| DynamoDB concurrency | Integration tests cover concurrent same-source writes, compatible concurrent source merges, duplicate-new-source retry, and conflict atomicity. |
| DynamoDB pagination | Integration tests cover empty reads, exactly one transaction, and forced one-item multipage query/scan/total reads. |
| Merchant/category selection | Merchant is chosen deterministically from the most informative group evidence; `MICRO` uses any UPI evidence in the merged group, so input order does not decide category. |
| Transfer detection | Timing and amount are no longer sufficient. Transfer classification also requires matching transfer-reference text such as `IMPS/P2A` or `NEFT`. |
| Account 3310 balance handling | Balance reliability exclusions are configurable through `ledger.accounts-without-reliable-balances`; default remains `3310` because card alerts report available limit rather than account balance. |
| Discrepancy identity | Balance-gap discrepancy notes now include type and source-message evidence, and deduplication uses stored discrepancy fields so reruns remain idempotent. |
| Malformed records | JSONL read accounting now returns valid records plus malformed line diagnostics; ingestion stats expose malformed counts. |
| Backfill reporting | Backfill now returns structured failure details and classifies retryable versus non-retryable failures with exponential backoff. |
| CI verification | CI parses JUnit XML and fails if DynamoDB integration tests are skipped. |

## Verification

Final verification must be read from the latest GitHub Actions run for the final commit. Local Windows verification before commit passed:

- `./gradlew clean test`: 62 tests completed, 0 failed, 12 DynamoDB tests skipped locally because DynamoDB Local was not running.
- `./verify.sh` via Git Bash: passed.
- `./gradlew -q --console=plain selfCheck`: passed with 522 messages read, 256 transactions written, 43 skipped unsupported messages, 0 malformed records, 0 failed writes, and the expected `-7500.00` discrepancy.

## Remaining Limits

- Local Windows verification cannot prove DynamoDB integration when DynamoDB Local is not running; CI is the required source of truth for those tests.
- `Category` is frozen, so there is no `UNKNOWN`; uncertain transfer cases remain as their original `SPEND` or `INCOME`.
