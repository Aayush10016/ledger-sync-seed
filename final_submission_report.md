# Final Submission Report

This report intentionally avoids hard-coded historical commit IDs. For the
submitted state, use:

```bash
git rev-parse HEAD
gh run list --repo Aayush10016/ledger-sync-seed --commit "$(git rev-parse HEAD)"
```

The final response accompanying the submission should cite the exact pushed
commit SHA and successful GitHub Actions run URL for that SHA.

## Validation Scope

- `./gradlew clean test`
- `./verify.sh`
- DynamoDB Local startup in CI
- DynamoDB integration test execution with zero skips in CI
- `./gradlew -q --console=plain runPerf -Pcount=100000` in CI

## Current Functional Results

- Corpus A expected observed transactions: `256`
- Produced observed transactions in self-check: `256`
- Skipped unsupported messages: `43`
- Malformed records: `0`
- Failed writes: `0`
- Expected discrepancy: `-7500.00` for account `4821`, recorded as a reconciliation discrepancy with source-message evidence.

## Key Correctness Controls

- Source-message ownership is enforced in SQL and DynamoDB.
- Same-channel same-second messages with different bodies remain separate.
- Same-second/same-merchant collisions are tested explicitly and are not merged without stronger evidence.
- SMS/email corroboration can merge one real cross-channel transaction without depending on merchant string equality.
- Consistency checking uses the union of complete SQL and DynamoDB snapshots.
- Consistency checking canonicalizes timestamp instants, source-message ID order, merchant formatting, and decimal values before reporting mismatches.
- DynamoDB concurrency tests cover same-source idempotency, compatible concurrent source merges, duplicate-new-source retry, conditional conflicts, and atomic no-partial-write behavior.
- DynamoDB pagination tests cover empty reads, one returned transaction, and forced one-item multipage query/scan/total reads.
- Merchant and category selection are deterministic and independent of input order.
- Transfer classification requires transfer-reference evidence, not only amount and timing.
- Balance reliability exclusions are configurable with `ledger.accounts-without-reliable-balances`; default is `3310`.
- Malformed JSONL records are counted and reported separately from parser skips.
- Backfill exposes structured failure details and explicit retry classification.
- CI fails if DynamoDB integration tests are skipped.

## Known Boundaries

- The frozen `Category` enum has no `UNKNOWN`; uncertain transfer candidates remain as their original `SPEND` or `INCOME`.
- Cross-run deduplication without overlapping source IDs or bank reference data cannot be proven safely.
- DynamoDB `GetItem` does not expose `Count`/`ScannedCount`; the benchmark reports point-read call counts for message lookup.
