# Unfinished List

- **Cloud Deployment Verification**: The DynamoDB local instance has been thoroughly tested, but true production metrics (like AWS billing mode interactions) require a real cloud deployment to verify completely.
- **Advanced Concurrency Load Test**: While idempotency and multi-source merging have been tested with the provided corpus, an adversarial load test simulating concurrent ingestion of thousands of out-of-order messages is needed.
- **Personal Evidence/Draft Placeholders**: The `TASK_0_ONE_PAGER_DRAFT.md`, `TASK_1_TRACK_TEARDOWN.md`, and `UPDATED_CV_DRAFT.md` currently contain drafts/templates that require manual addition of the candidate's personal data.
- **Long-term Backfill Resilience**: The backfill tool has been designed to resume safely, but we need more empirical data on massive (10M+ rows) SQL migration behavior regarding memory pressure.
