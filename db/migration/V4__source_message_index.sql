CREATE TABLE IF NOT EXISTS ledger_sources (
    source_message_id VARCHAR(200) PRIMARY KEY,
    txn_id            VARCHAR(100) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ledger_sources_txn_id ON ledger_sources(txn_id);
