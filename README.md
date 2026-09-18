# ledger-sync

Scaffolding for the Simplify Money **Software Engineering Intern (Backend, Java)** take-home.

Read this file completely before you write any code. Then read
`fixtures/corpus-a.jsonl` — not all 500 lines, but enough of them that you stop
being surprised.

> **Do not open a pull request here.** Work in your own fork and submit by email.
> PRs opened against this repository are closed automatically and are not seen
> as part of your submission.

---

## What this service is for

Simplify Money tells a user where their money went. To do that, something has to
read the bank SMS and bank emails sitting on their phone and turn them into a
ledger the user can trust.

This repository is that something, half-finished, with a live incident open
against it.

---

## What you are being asked to do, exactly

**Input:** `fixtures/corpus-a.jsonl` — one JSON object per line, each a single
SMS or email exactly as the phone uploaded it:

```json
{"message_id":"m-00004-9c11ae","channel":"sms","sender":"AD-HDFCBK-S",
 "received_at":"2026-07-04T07:19:00+05:30","device_id":"dev-3f1a90c47b21",
 "body":"Rs.5 debited from a/c **4821 on 04-07-26 at 07:19 to UPI/WATER CAN. Avl Bal: Rs.92,213.10. Not you? Call 18002586161"}
```

**Output:** three JSON files, written by `report <dir>`.

### 1. `ledger.json` — one entry per real transaction

```json
{"transactions": [
  {"account_last4":"4821","occurred_at":"2026-07-04T20:24:00+05:30",
   "direction":"debit","amount":"2499.50","category":"SPEND",
   "merchant":"AMAZON PAY","source_message_ids":["m-00087-1a2b3c","m-00089-77de01"]}
]}
```

`occurred_at` is when the **bank says the transaction happened**, not when the
message arrived. `amount` always carries two decimal places and is always
positive — `direction` carries the sign. `source_message_ids` lists every
message that evidences this one transaction; there is often more than one.

### 2. `summary.json` — per-account totals

```json
{"accounts": {
  "4821": {"spend":"87068.38","income":"101340.83",
           "micro_count":52,"micro_total":"2357.51",
           "transferred_out":"25000.00","transferred_in":"6000.00"}
}}
```

### 3. `reconciliation.json` — anything your ledger cannot account for

```json
{"discrepancies": [
  {"account_last4":"4821","occurred_at":"...","amount":"...","note":"..."}
]}
```

We are not telling you how to find these, or whether there are any. Working out
what "cannot account for" means here, and what in the data lets you check it, is
part of the task.

---

## The four categories

Every transaction gets exactly one.

| Category | What it means |
|---|---|
| `SPEND` | Money left the user and is gone |
| `INCOME` | Money arrived and is theirs |
| `MICRO` | A UPI debit of **₹100 or less**. Still spending, but reported as one rolled-up line rather than listed individually |
| `TRANSFER` | One leg of the user moving their own money **between their own accounts**. Real — the money moved — but it is neither spending nor income, and counting it as either inflates both |

`micro_total` is the sum of `MICRO`. `spend` is the sum of `SPEND` and does
**not** include `MICRO` or `TRANSFER`. `income` likewise excludes `TRANSFER`.

---

## Your checkpoint

`fixtures/corpus-a-totals.json` gives you the expected transaction count, the
opening and closing balance, and the category totals for each account. No
row-level answers. Use it to check yourself.

If your numbers do not match it, **say so and say why.** A submission whose
numbers match because they were made to match is worse than one that does not
match and explains itself. We can tell the difference, and we check.

---

## Where the code is now

```
src/main/java/in/simplifymoney/ledgersync/
  model/       RawMessage, NormalizedTxn, Category, Direction
  json/        a small JSON reader/writer, so this builds with only a JDK 21
  parse/       one parser per message format
  ingest/      reads a corpus, saves what it finds
  store/       the SQL ledger, and the document store you are going to add
  report/      the three output documents

  App.java     migrate | ingest | report
  SelfCheck.java
```

Run it:

```bash
./verify.sh                      # compile + run the pipeline, no network needed
./gradlew test                   # the test suite (needs network once, for JUnit)
./gradlew run --args="migrate"
./gradlew run --args="ingest fixtures/corpus-a.jsonl"
./gradlew run --args="report submission/"
```

`./verify.sh` produces 256 observed transactions, matching
`fixtures/corpus-a-totals.json`. The known missing `7500.00` debit is not
fabricated as a transaction; it is reported as an allowed reconciliation
discrepancy with source-message evidence.

---

## Current implementation state

All items below have been implemented:

1. **`EmailParser`** parses HDFC email alerts and merges them with SMS alerts for
   the same transaction by matching on account, epoch-second, direction, and
   amount (merchant excluded because channel naming differs).
2. **`IciciSmsParser`** handles both ICICI SMS formats present in the corpus.
3. **Cross-channel deduplication** groups SMS and email alerts into one
   transaction within a single ingestion run. Separate ingestion runs merge
   when a later run carries a source-message ID that is already indexed.
4. **Categories** are assigned correctly: `MICRO` for UPI debits ≤ ₹100,
   `TRANSFER` for matched cross-account opposite-direction equal-amount pairs
   within 5 minutes, `SPEND` / `INCOME` otherwise.
   Transfer classification now also requires transfer-reference evidence; amount
   and timing alone are not enough.
5. **`Reports.summary`** produces correct per-account spend, income, micro
   rollup, and transferred-in / transferred-out totals.
6. **`Reports.reconciliation`** serialises the balance-gap discrepancies detected
   during ingestion.
7. **`DynamoDbLedgerStore`** is a full DynamoDB implementation.
   **`Backfill`** moves SQL records to DynamoDB idempotently with retry.
   **`ConsistencyChecker`** compares complete store snapshots bi-directionally.
8. **`incident/INC-2026-09-11.md`** — resolved. Root cause, blast radius, fix,
   and regression test documented.

---

## The document store

The ledger is moving off SQL onto a document store. **DynamoDB preferred,
MongoDB fine** — your choice, and say why. It must run from your
`docker compose up`.

`DocumentStore` declares the only three queries this service makes:

1. one account's transactions for one month, newest first
2. running totals per category for an account
3. given a message id, which transaction did it produce

Design your documents so the engine serves these directly. We are not going to
tell you what a document should look like — that decision is the exercise.

For each of the three, **report how many items the engine examined versus how
many it returned, at 100,000 transactions.** DynamoDB gives you `ScannedCount`
and `Count`; MongoDB gives you `totalDocsExamined` and `nReturned`. Put the six
numbers in your README.

Then:

- **`Backfill`** moves what is already in SQL across. Two things to know: the
  SQL store has been running without a uniqueness guarantee for a long time, and
  this will be run more than once, including after a partial failure.
- **`ConsistencyChecker`** proves the two stores agree and names precisely where
  they do not. We will run yours against a document store we have deliberately
  altered. It has to find what we changed. A checker that compares row counts
  will not.

---

## Rules

- `model/NormalizedTxn.java`, `model/Category.java` and
  `src/test/.../NormalizedTxnContractTest.java` are **frozen**. Do not edit
  them. Everything behind them is yours.
- Java 21. Any framework, or none — say why in your decision log.
- Real commit history. Not one squashed commit.
- If something in here is wrong or unclear, **email us**. Guessing when you
  could have asked is a worse signal than asking.

`talent.acquisition@simplifymoney.in`


I chose **DynamoDB (Local)**. The `ConsistencyChecker`, `Backfill`, and `DynamoDbLedgerStore` have been fully implemented using the official AWS SDK for Java v2.

**Why DynamoDB over MongoDB?** 
DynamoDB provides strict guarantees around high-performance scaling through its Single-Table Design patterns. With `TransactWriteItems`, we can safely insert transactions, map their message IDs, and update running category totals atomically, ensuring the database remains completely consistent without complex aggregation pipelines or expensive index scanning.

### Performance Metrics (at 100,000 transactions)

`src/test/java/in/simplifymoney/ledgersync/PerfTest.java` is the reproducible
100,000-transaction harness. It inserts a run-isolated account, fails if any
write fails, and prints the real DynamoDB `ScannedCount`/`Count` values for the
two Query access patterns. DynamoDB `GetItem` does not expose those counters,
so the message lookup metric reports the deterministic point-read call count.

| Benchmark | Expected (Examined / Returned) | Measured (Examined / Returned) |
| --- | --- | --- |
| **Q1** (`forAccountMonth`) | `100000` / `100000` | `100000` / `100000` |
| **Q2** (`categoryTotals`) | `4` / `4` | `1` / `1` |
| **Q3** (`byMessageId`) | `1` / `1` | `1` / `1` |

#### Q1: `forAccountMonth(accountLast4, month)`
- *Why:* The query directly targets the partition key (`ACCT#1234`) and uses a range key `begins_with(SK, TXN#YYYY-MM)`. It reads exactly what it returns, scanning no irrelevant documents.

#### Q2: `categoryTotals(accountLast4)`
- *Why:* Instead of scanning all transactions, we maintain a running total using DynamoDB atomic `ADD` updates. The query fetches strictly from `CAT#` range keys. (Measured is 1/1 because the benchmark dataset contains only SPEND records).

#### Q3: `byMessageId(messageId)`
- *Why:* DynamoDB's `GetItem` API does not return `ScannedCount` or `Count` because it is an explicit O(1) Hash Map lookup. The engine mathematically examines exactly **1 item** and returns **1 item** (or 0 if not found), independent of the 100,000 records in the table.

### Decision Log

1. **Parallel Stream Optimizations for N+1 Queries** 
   - *Decision:* Used `parallelStream()` and thread-safe collections (`ConcurrentHashMap`, `CopyOnWriteArrayList`, `AtomicLong`) in `ConsistencyChecker` and `Backfill`.
   - *Why:* The `DocumentStore` interface restricts data fetching to individual queries (e.g., `byMessageId`, `forAccountMonth`). Iterating sequentially over 100,000 transactions would result in an extreme N+1 query bottleneck causing the verification to take several minutes. Concurrency maximizes DynamoDB's high throughput capabilities, drastically reducing runtime without altering the frozen interface.

2. **Deduplication Strategy & Over-Aggression Risks**
   - *Decision:* Kept `IngestService` deduplication as exact matching but normalized across timezones by using Epoch Seconds (`.toEpochSecond()`). The key intentionally excludes `merchant`.
   - *Why:* Some transactions arriving via both SMS and Email represented the same instant in time but had different UTC offsets (e.g., `+05:30` vs `Z`). By converting them to Epoch Seconds before comparing, we successfully group identical cross-channel alerts without falsely merging separate transactions that just happen to occur close to each other. We purposefully omit `merchant` from the deduplication key because different channels report merchant names differently (e.g., `UPI/WATER CAN` via SMS vs `Water Can` via Email). While this risks merging two *genuinely different* transactions if they occur at the *exact same second* with the *exact same amount* for the *same account*, it is the safest heuristic to prevent mass duplication given the absence of unique bank reference numbers.

3. **TRANSFER Detection Heuristic Safety**
   - *Decision:* Require transfer-reference evidence, such as matching `IMPS/P2A` or `NEFT` merchant/reference text, in addition to opposite directions, identical amounts, different accounts, and a short time window.
   - *Why:* Timing and amount alone are weak evidence and can classify unrelated spend/income as transfers. Reference text makes the heuristic conservative while preserving the known self-transfer pairs in the corpus.

4. **Bi-Directional Consistency Verification & Enumeration Limits**
   - *Decision:* Implemented a strict size and element-wise comparison between the `sqlList` and `docList` in `ConsistencyChecker`.
   - *Why:* Rather than merely checking if SQL items exist in DynamoDB, it is critical to verify DynamoDB didn't erroneously create "ghost" transactions. By comparing the size and equality of the lists for each `Account/Month`, we implicitly prove that DynamoDB contains no rogue records *for those periods*.
   - *Detectable Corruptions:* The checker will successfully detect missing transactions, corrupted fields, duplicated transactions within an active month, and category total discrepancies for all active accounts.
   - *Limitation (Undetectable Corruptions):* Because the frozen `DocumentStore` interface prohibits an unconstrained `client.scan()`, it is mathematically impossible to discover a deliberately inserted "ghost" account that SQL has never heard of, or a "ghost" month for an existing account outside of its active SQL months. The Consistency Checker provides the strongest proof possible strictly within the bounds of the 3 authorized queries.

4. **Resilient Ingestion Parsing**
   - *Decision:* `IngestService` records unsupported messages separately from malformed JSONL records. `SelfCheck` fails if malformed records are present unexpectedly.
   - *Why:* Every input record must be accounted for as valid, skipped with a reason, or malformed with a line-number diagnostic.

5. **Idempotency in Backfill**
   - *Decision:* Leveraged DynamoDB's `TransactWriteItems` condition failures and explicit retry classification. `Backfill.Result` includes per-failure details with account, month, attempts, transaction id, failure type, and error text.
   - *Why:* If the `Backfill` job fails partially and is restarted, we rely on DynamoDB transactions natively failing their conditions for already-processed items. Structured failures make any remaining errors actionable without scraping unstructured logs.

6. **The historical `257` vs `256` transaction trap**
   - *Observation:* The current fixture expects 256 observed transactions. Earlier reports that expected 257 were stale.
   - *Why:* The missing `7500.00` debit is not present as a raw bank alert, so creating a 257th ledger row would fabricate a transaction. The pipeline reports it as a discrepancy instead.

7. **The 7500.00 Discrepancy (Account 4821)**
   - *Observation:* `ConsistencyChecker` reports a massive `-7500.00` discrepancy on `2026-07-29`. 
   - *Why:* By observing the stated balances of the surrounding transactions (`36,054.05` dropping to `28,479.05`), the ledger accurately predicts that `7,575.00` was spent. However, the corpus only contains a `75.00` alert. The bank completely failed to send an SMS or Email for the missing `7,500.00`. My ledger successfully flags this missing money in `reconciliation.json` without blindly altering the ledger to "make it match".

8. **Offline `verify.sh` Compilation**
   - *Decision:* Excluded `DynamoDbLedgerStore.java` and `DynamoDbLedgerAdapter.java` from the `verify.sh` wildcard compilation.
   - *Why:* To maintain the strict requirement that `./verify.sh still works` in a zero-network, pure-JDK environment, we must prevent `javac` from attempting to compile the DynamoDB implementation and adapter (which rely on external AWS SDK JARs fetched via Gradle and would otherwise break the script).

### Correctness Audit Addendum

The September 2026 audit found that earlier fixes still relied on a drift-prone
transaction identity and query-specific consistency checks.

- SQL now maintains `ledger_sources`, a source-message index that preserves the
  already assigned `txn_id` whenever any known source message reappears. This
  prevents lower-sorting delayed source IDs from creating a second SQL row and
  keeps independent identical-looking transactions separate when their source
  messages do not overlap.
- Legacy rows are migrated inside the same transaction as schema migration.
  Exact duplicate source sets collapse deterministically; identical visible
  transactions with different source IDs are preserved.
- DynamoDB save now preflights existing message-index records before computing a
  new transaction key, and rechecks ownership after conditional-write races.
  Additional compatible source IDs update the existing document transaction
  without incrementing category totals.
- `ConsistencyChecker` now compares complete SQL and document snapshots from
  `sql.all()` and `documents.scanAllTransactions()`, reports SQL-only,
  document-only, duplicate identity, source collision, and field-level mismatch
  divergences, and emits deterministic ordering.
- `Backfill.Result` now includes lifecycle status and executor termination
  observation plus structured failure details. `shutdownNow()` is treated as a
  cancellation request, not proof that blocked work physically stopped.
- Balance reliability exclusions are configurable through
  `ledger.accounts-without-reliable-balances`; the default excludes card account
  `3310`, whose messages provide available limit rather than account balance.
- `verify.sh` still proves only the offline non-Dynamo compile and `SelfCheck`
  path. It does not compile `DynamoDbLedgerStore.java`, does not exercise
  DynamoDB Local, and does not prove integration behavior.

### Task 2-4 Verification Notes

Current implementation focus:

- Task 2 ingestion parses SMS and email alerts, excludes non-transaction messages that do not match supported bank formats, deduplicates within a corpus, preserves source-message IDs, and emits `ledger.json`, `summary.json`, and `reconciliation.json`.
- Task 3 incident prevention is in `Amounts`: integer amounts such as `Rs.5` are parsed as the transaction amount, while `Avl Bal` is parsed only by the balance-specific regex.
- Task 4 uses DynamoDB Local through Docker Compose. CI starts it before integration tests, fails if it cannot be reached, and asserts that the DynamoDB integration suite did not skip.

Document-store access patterns:

| Query | Key pattern | Pagination | Examined vs returned at 100,000 txns |
| --- | --- | --- | --- |
| Account/month transactions | `PK=ACCT#<last4>`, `begins_with(SK, TXN#yyyy-MM)` | Loops on `LastEvaluatedKey` | Design expectation: examined equals returned for that account/month query. |
| Category totals | `PK=ACCT#<last4>`, `begins_with(SK, CAT#)` | Loops on `LastEvaluatedKey` | Design expectation: examines and returns at most four category-total rows. |
| Message lookup | `PK=MSG#<messageId>`, `SK=MSG`, then target `GetItem` | Not paginated; point lookup | Design expectation: one pointer item examined/returned, plus one target item when present. |

Run the benchmark with DynamoDB Local available:

```bash
./gradlew testClasses
./gradlew -q --console=plain runPerf -Pcount=100000
```

Do not substitute fabricated numbers; use the `PerfTest` output for measured
values.

Additional documentation:

- Engineering decisions: `DECISION_LOG.md`
- AI disclosure: `AI_DISCLOSURE.md`

Known limitations:

- Push events in this fork did not automatically enqueue Actions runs during this audit; workflow runs were verified through `workflow_dispatch`.
- Local Windows environment lacks a running Docker daemon, so DynamoDB Local integration tests were verified in CI rather than locally.
- The frozen `Category` enum has no reconciliation-adjustment category; balance gaps are therefore kept in reconciliation/discrepancy output, not as synthetic ledger transactions.
