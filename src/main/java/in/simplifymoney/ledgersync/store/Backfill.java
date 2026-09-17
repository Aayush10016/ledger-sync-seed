package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.NormalizedTxn;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
        System.out.println("Starting High-Performance Backfill...");
        AtomicLong read = new AtomicLong(0);
        AtomicLong written = new AtomicLong(0);
        AtomicLong skipped = new AtomicLong(0);
        AtomicLong failed = new AtomicLong(0);

        try {
            List<NormalizedTxn> allTxns = source.all();
            Set<String> seen = ConcurrentHashMap.newKeySet();

            // Parallelize network-bound insertion to maximize throughput
            allTxns.parallelStream().forEach(txn -> {
                long currentRead = read.incrementAndGet();
                if (currentRead % 100 == 0) {
                    System.out.println("Backfill Progress: " + currentRead + " / " + allTxns.size());
                }

                // Deduplicate items to handle dirty SQL store
                String deduplicationKey = txn.accountLast4() + "|" + txn.occurredAt().toEpochSecond() + "|" + txn.direction() + "|" + txn.amount();
                if (!seen.add(deduplicationKey)) {
                    skipped.incrementAndGet();
                    return;
                }

                try {
                    target.save(txn);
                    written.incrementAndGet();
                } catch (Exception e) {
                    System.err.println("Failed to insert transaction " + deduplicationKey + " - " + e.getMessage());
                    failed.incrementAndGet();
                }
            });

            System.out.println("Backfill complete. Read: " + read.get() + ", Written: " + written.get() 
                    + ", Skipped: " + skipped.get() + ", Failed: " + failed.get());
            return new Result(read.get(), written.get(), skipped.get());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Backfill failed critically", e);
        }
    }

    public record Result(long read, long written, long skipped) {}
}
