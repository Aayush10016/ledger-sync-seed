# Decision Log

## 1. Transaction identity
Problem: source-message sets are stable evidence, but visible transaction fields can collide for independent purchases.
Options considered: visible-field identity, first-source identity, source-set identity plus persistent source ownership.
Decision: use deterministic source-set identity for new records and preserve persistent ownership through source-message indexes.
Evidence: SQL and DynamoDB tests cover independent identical purchases, repeated ingestion, and delayed source-message merge.
Limitation: two independently ingested, non-overlapping source messages for the same real-world transaction cannot be proven identical without additional bank reference data.

## 2. Source-message ownership
Problem: every source message must point to exactly one transaction.
Decision: SQL uses `ledger_sources`; DynamoDB uses `MSG#<messageId>` ownership records written in `TransactWriteItems`.
Rejected: treating source IDs as an unindexed comma-separated field.
Evidence: conflict and atomicity tests assert no partial index after failed writes.

## 3. DynamoDB document model
Problem: support account-month, category totals, and message lookup directly.
Decision: store transaction rows under `PK=ACCT#account`, `SK=TXN#yyyy-MM#epoch#txnId`; category totals under `CAT#category`; message pointers under `PK=MSG#id`.
Rejected: full-table scans for user-facing queries.
Evidence: integration tests query account-month, totals, message lookup, and pagination.

## 4. Conditional writes and idempotency
Problem: retries and concurrent ingestion must not double-count totals.
Decision: create transaction, category total update, and message indexes in one DynamoDB transaction; duplicate transactions update message ownership without updating totals.
Evidence: concurrent independent-client test and repeated-ingestion tests verify one transaction and one total increment.

## 5. Pagination
Problem: DynamoDB Query/Scan returns partial pages.
Decision: loop on `LastEvaluatedKey` for account-month, category totals, and full scans.
Evidence: integration tests force one-item pages and verify no missing results.

## 6. Backfill
Problem: SQL may contain historical duplicates and backfill may be rerun.
Decision: deduplicate by transaction identity before writing, rely on idempotent document-store save, and report timeout/termination state explicitly.
Evidence: backfill tests cover timeout and interruption behavior; end-to-end test backfills then runs consistency check.

## 7. Consistency checking
Problem: evaluator may mutate DynamoDB records, indexes, or totals.
Decision: compare complete SQL/document snapshots, field values, duplicate identities, category totals, and message-index ownership.
Rejected: count-only and positional list comparison.
Evidence: checker tests cover SQL-only, document-only, field mismatch, duplicate identity, category-total mismatch, and message-index mismatch.

## 8. Reconciliation
Problem: a balance gap is evidence of an unresolved discrepancy, not proof of merchant/time/category.
Decision: save discrepancy records and label synthetic adjustment transactions with deterministic `recon-` IDs and reconciliation metadata.
Limitation: frozen `Category` prevents a dedicated reconciliation category.

## 9. Incident prevention
Problem: integer rupee amounts were skipped, causing the parser to read available balance as transaction amount.
Decision: allow optional decimal places in transaction amounts while keeping balance parsing explicit.
Evidence: amount tests cover `Rs.5` and balance extraction separately.

## 10. CI and verification
Problem: CI previously did not run from push events and lacked a working wrapper/readiness setup.
Decision: add Gradle wrapper, Java 21 toolchain, DynamoDB dummy AWS credentials, explicit readiness loop, and manual workflow verification.
Evidence: workflow run `35350797544` passed on commit `f562092`; new changes require a fresh run.
