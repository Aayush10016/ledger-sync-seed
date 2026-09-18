# AI Disclosure

AI tools were used to inspect the repository, draft tests, reason about concurrency risks, and edit code. The final changes were reviewed against the source files, tests, local Gradle execution, and GitHub Actions output.

What AI helped with:
- Locating weak areas in DynamoDB atomicity, pagination, consistency checking, and CI.
- Drafting regression tests for identity drift, pagination, message ownership, and backfill timeout behavior.
- Updating documentation and summarizing verification evidence.

Independently reviewed:
- Frozen contracts remained unchanged.
- DynamoDB Local CI logs were inspected after each failure.
- Test failures were traced to exact failing assertions before patching.
- The final workflow result was verified with `gh run view`.

Concrete AI mistake and correction:
- Earlier work treated a green local suite as enough even though local Docker was unavailable, so DynamoDB integration behavior was skipped locally. CI later exposed real issues: DynamoDB readiness, missing Gradle wrapper, non-executable wrapper, and an end-to-end consistency assertion that only made sense after backfill.
- Corrected approach: make CI start DynamoDB Local with dummy AWS credentials, add the Gradle wrapper, require executable `gradlew`, backfill before full-store consistency assertions, and verify the actual GitHub Actions run.

Another correction:
- A previous checker compared transaction snapshots but did not validate category-total materialization or message-ID index ownership. The final checker compares both stores' transaction snapshots and also verifies category totals plus `byMessageId` ownership for every source ID.

Limitations of AI Assistance in this context:
- AI assistants lack domain-specific intuition about unstated business rules (e.g., whether to synthesize phantom ledger transactions to "make numbers match" or simply report the gap).
- The AI required explicit manual direction to implement the Epoch Second timezone-normalization fix, as it initially could not discern that cross-channel deduplication was failing due to UTC offset mismatches (e.g. `+05:30` vs `Z`) in otherwise identical alerts.
- Automated code generation relies on continuous human oversight to prevent infinite loops (like unclosed DynamoDB paginators) and to enforce exact interface constraints without rewriting frozen boilerplate.
