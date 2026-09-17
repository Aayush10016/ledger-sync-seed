package in.simplifymoney.ledgersync.store;

/**
 * Moves everything already in the SQL store into the document store.
 *
 * NOT IMPLEMENTED - this is yours.
 *
 * Two things to know before you start:
 *  - the SQL store is not clean. It has been running without a uniqueness
 *    guarantee for a long time
 *  - this will be run more than once, including after a partial failure
 */
public final class Backfill {

    private final SqlLedgerStore source;
    private final DocumentStore target;

    public Backfill(SqlLedgerStore source, DocumentStore target) {
        this.source = source;
        this.target = target;
    }

    public Result run() {
        System.out.println("Starting Backfill...");
        long read = 0;
        long written = 0;
        long skipped = 0;

        try {
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (in.simplifymoney.ledgersync.model.NormalizedTxn txn : source.all()) {
                read++;
                // Deduplicate items to handle dirty SQL store
                String deduplicationKey = txn.accountLast4() + "|" + txn.occurredAt() + "|" + txn.direction() + "|" + txn.amount();
                if (!seen.add(deduplicationKey)) {
                    skipped++;
                    continue;
                }
                
                target.save(txn);
                written++;
            }
            System.out.println("Backfill complete. Read: " + read + ", Written: " + written + ", Skipped: " + skipped);
            return new Result(read, written, skipped);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Backfill failed", e);
        }
    }

    public record Result(long read, long written, long skipped) {}
}
