# Decision Log

## 1. Transaction identity and source ownership
Problem: Every source message must point to exactly one transaction, but visible transaction fields can collide for independent purchases.
Decision: Use deterministic source-set identity for new records and preserve persistent ownership through source-message indexes. SQL uses `ledger_sources`; DynamoDB uses `MSG#<messageId>` ownership records written in `TransactWriteItems`.
Evidence: SQL and DynamoDB tests assert no partial index after failed writes and exactly one transaction owner per source ID, covering repeated ingestion and delayed merges.
Limitation: Two independently ingested, non-overlapping source messages for the same real-world transaction cannot be proven identical without additional bank reference data.

## 2. DynamoDB document model
Problem: support account-month, category totals, and message lookup directly.
Decision: store transaction rows under `PK=ACCT#account`, `SK=TXN#yyyy-MM#epoch#txnId`; category totals under `CAT#category`; message pointers under `PK=MSG#id`.
Rejected: full-table scans for user-facing queries.
Evidence: integration tests query account-month, totals, message lookup, and pagination.

## 3. Conditional writes and idempotency
Problem: retries and concurrent ingestion must not double-count totals.
Decision: create transaction, category total update, and message indexes in one DynamoDB transaction; duplicate transactions update message ownership without updating totals.
Race handling: conditional-write cancellation no longer depends on AWS cancellation-reason ordering. The store re-reads source-message pointers and the candidate transaction key, then only routes to an existing transaction after compatibility checks that include account, time, direction, amount, category, and merchant.
Evidence: concurrent independent-client, concurrent compatible update, duplicate-new-source retry, repeated-ingestion, atomicity collision, and conflicting-category tests verify one transaction, one total increment, retry correctness, and no partial message index.

## 4. Pagination
Problem: DynamoDB Query/Scan returns partial pages.
Decision: loop on `LastEvaluatedKey` for account-month, category totals, and full scans.
Evidence: integration tests cover empty reads, exactly one returned transaction, one-item pages, empty filtered scan pages, and multiple pages without missing results.

## 5. Backfill
Problem: SQL may contain historical duplicates and backfill may be rerun.
Decision: deduplicate by transaction identity before writing, rely on idempotent document-store save, classify retryable versus non-retryable failures explicitly, and report timeout/termination/failure details structurally.
Evidence: backfill tests cover timeout, interruption behavior, unterminated workers, and non-retryable failure details; end-to-end test backfills then runs consistency check.

## 6. Consistency checking
Problem: evaluator may mutate DynamoDB records, indexes, or totals.
Decision: compare complete SQL/document snapshots, field values, duplicate identities, category totals, and message-index ownership.
Rejected: count-only and positional list comparison.
Evidence: checker tests cover SQL-only, document-only, field mismatch, duplicate identity, category-total mismatch, message-index mismatch, and false-positive resistance for timestamp offsets, source-ID ordering, and merchant formatting.

## 7. Reconciliation
Problem: a balance gap is evidence of an unresolved discrepancy, not proof of merchant/time/category.
Decision: save discrepancy records only. Do not synthesize ledger transactions for missing money, because that would pollute the transaction ledger with inferred events.
Limitation: frozen `Category` prevents a dedicated reconciliation category, so reconciliation remains outside `NormalizedTxn`.

## 8. Incident prevention
Problem: integer rupee amounts were skipped, causing the parser to read available balance as transaction amount.
Decision: allow optional decimal places in transaction amounts while keeping balance parsing explicit.
Blast radius: 38 messages in `fixtures/corpus-a.jsonl` had a whole-rupee transaction amount followed by a decimal balance or limit that the legacy regex would have selected.
Evidence: amount tests cover `Rs.5`, multiple monetary values, available balances, limits, OTP exclusion, unrelated numbers, and the exact 38-message corpus count.

## 9. CI and verification
Problem: CI previously did not run from push events and lacked a working wrapper/readiness setup.
Decision: add Gradle wrapper, Java 21 toolchain, DynamoDB dummy AWS credentials, explicit readiness loop, manual workflow verification, and a JUnit XML assertion that DynamoDB integration tests executed without skips.
Evidence: workflow run `35353694066` passed on commit `c8a6bc0`. The workflow run reference here is intentionally historical — the latest CI results must be read from the Actions tab on the fork, not from this file.

## 10. Ingestion grouping, edge-case parity, and transfer evidence
Problem: Grouping and categorization could become order-dependent, and SQL/DynamoDB compatibility checks historically diverged on scale sensitivity (BigDecimal equals vs compareTo) and strict category/merchant assertions.
Decision: Preserve source-message ownership as the primary durable identity. Use `compareTo() == 0` for all exact amount matches. Extend SQL compatibility checks to match DynamoDB. Select merchant deterministically from best evidence, classify `TRANSFER` only with explicit matching bank references (IMPS/NEFT) and dual-direction corroboration.
Evidence: Tests added cover transfer matching across different scales, conflicting category/merchant rejections, order-independent merchant selection, and malformed record tracking.
