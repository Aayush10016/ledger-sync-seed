package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.util.TxnIdentity;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Moves everything already in the SQL store into the document store.
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
        boolean timedOut = false;

        try {
            List<NormalizedTxn> allTxns = source.all();
            java.util.Map<String, NormalizedTxn> deduplicatedTxns = new java.util.LinkedHashMap<>();

            // Deduplicate items to handle dirty SQL store using TxnIdentity instead of arbitrary visible fields
            for (NormalizedTxn txn : allTxns) {
                long currentRead = read.incrementAndGet();
                if (currentRead % 100 == 0) {
                    System.out.println("Backfill Read Progress: " + currentRead + " / " + allTxns.size());
                }

                String deduplicationKey = TxnIdentity.getId(txn);
                
                deduplicatedTxns.merge(deduplicationKey, txn, (existing, incoming) -> {
                    skipped.incrementAndGet();
                    java.util.Set<String> mergedIds = new java.util.HashSet<>(existing.sourceMessageIds());
                    mergedIds.addAll(incoming.sourceMessageIds());
                    java.util.List<String> sorted = new java.util.ArrayList<>(mergedIds);
                    java.util.Collections.sort(sorted);
                    return new NormalizedTxn(
                        existing.accountLast4(), existing.occurredAt(), existing.direction(),
                        existing.amount(), existing.category(), existing.merchant(),
                        sorted
                    );
                });
            }

            // Bounded concurrency
            ExecutorService executor = Executors.newFixedThreadPool(10);
            
            for (NormalizedTxn txn : deduplicatedTxns.values()) {
                executor.submit(() -> {
                    int attempts = 0;
                    boolean success = false;
                    Throwable lastException = null;
                    
                    while (attempts < 3 && !success) {
                        attempts++;
                        try {
                            target.save(txn);
                            written.incrementAndGet();
                            success = true;
                        } catch (Exception e) {
                            lastException = e;
                            boolean retryable = isRetryable(e);
                            if (!retryable) {
                                break;
                            }
                            try {
                                Thread.sleep(200L * attempts); // Simple backoff
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                    
                    if (!success) {
                        failed.incrementAndGet();
                        System.err.println("Failed to insert transaction " + TxnIdentity.getId(txn) + 
                            " after " + attempts + " attempts. Exception: " + lastException.getClass().getSimpleName() + 
                            " - " + lastException.getMessage() + ". Retryable: " + isRetryable(lastException));
                    }
                });
            }

            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.MINUTES)) {
                System.err.println("Backfill executor timed out.");
                timedOut = true;
            }

            System.out.println("Backfill complete. Read: " + read.get() + ", Written: " + written.get() 
                    + ", Skipped: " + skipped.get() + ", Failed: " + failed.get());
                    
            if (failed.get() > 0) {
                System.err.println("Note: " + failed.get() + " records failed. Checkpointing is not implemented. A re-run will process all items and skip already written ones idempotently.");
                throw new IllegalStateException("Backfill completed with " + failed.get() + " failures. Check logs for details.");
            }
            return new Result(read.get(), written.get(), skipped.get(), failed.get(), timedOut);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Result(read.get(), written.get(), skipped.get(), failed.get(), true);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Backfill failed critically", e);
        }
    }

    private boolean isRetryable(Throwable e) {
        if (e == null) return false;
        String name = e.getClass().getSimpleName();
        String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
        if (name.contains("ProvisionedThroughputExceededException") ||
            name.contains("InternalServerError") ||
            name.contains("ThrottlingException") ||
            name.contains("RequestLimitExceeded") ||
            msg.contains("rate limit") || msg.contains("throughput") ||
            msg.contains("timeout")) {
            return true;
        }
        return isRetryable(e.getCause());
    }

    public record Result(long read, long written, long skipped, long failed, boolean timedOut) {}
}
