package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.util.TxnIdentity;

import java.util.List;
import java.time.YearMonth;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Moves everything already in the SQL store into the document store.
 */
public final class Backfill {

    private final LedgerStore source;
    private final DocumentStore target;

    public Backfill(LedgerStore source, DocumentStore target) {
        this.source = source;
        this.target = target;
    }

    public Result run() {
        return run(30, TimeUnit.MINUTES);
    }

    public Result run(long timeout, TimeUnit unit) {
        System.out.println("Starting High-Performance Backfill...");
        AtomicLong read = new AtomicLong(0);
        AtomicLong written = new AtomicLong(0);
        AtomicLong sourceDeduplicated = new AtomicLong(0);
        AtomicLong targetSkipped = new AtomicLong(0);
        AtomicLong failed = new AtomicLong(0);
        Status status = Status.COMPLETED;
        boolean terminated = true;
        boolean timedOut = false;
        List<FailureDetail> failureDetails = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

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
                    sourceDeduplicated.incrementAndGet();
                    java.util.Set<String> mergedIds = new java.util.HashSet<>(existing.sourceMessageIds());
                    mergedIds.addAll(incoming.sourceMessageIds());
                    java.util.List<String> sorted = new java.util.ArrayList<>(mergedIds);
                    java.util.Collections.sort(sorted);
                    String bankRef = existing.bankReferenceId();
                    if (incoming.bankReferenceId() != null) {
                        if (bankRef == null) {
                            bankRef = incoming.bankReferenceId();
                        } else if (!bankRef.equals(incoming.bankReferenceId())) {
                            throw new IllegalStateException("Deduplication conflict: different bank references: " + bankRef + " vs " + incoming.bankReferenceId());
                        }
                    }
                    return new NormalizedTxn(
                        existing.accountLast4(), existing.occurredAt(), existing.direction(),
                        existing.amount(), existing.category(), existing.merchant(),
                        sorted, bankRef
                    );
                });
            }

            // Bounded concurrency
            ExecutorService executor = Executors.newFixedThreadPool(10);
            List<Future<?>> futures = new java.util.ArrayList<>();
            
            for (NormalizedTxn txn : deduplicatedTxns.values()) {
                String deduplicationKey = TxnIdentity.getId(txn);
                futures.add(executor.submit(() -> {
                    int attempts = 0;
                    boolean success = false;
                    Throwable lastException = null;
                    
                    while (attempts < 3 && !success) {
                        attempts++;
                        try {
                            // Safe Resume Support: Skip if it already exists
                            java.util.Optional<NormalizedTxn> existingTarget = target.byMessageId(txn.sourceMessageIds().get(0));
                            if (existingTarget.isPresent() && TxnIdentity.getId(existingTarget.get()).equals(deduplicationKey)) {
                                targetSkipped.incrementAndGet();
                                success = true; // Pretend it succeeded
                            } else {
                                target.save(txn);
                                written.incrementAndGet();
                                success = true;
                            }
                        } catch (Exception e) {
                            lastException = e;
                            if (classify(e) == FailureType.NON_RETRYABLE) {
                                break;
                            }
                            try {
                                Thread.sleep(200L * (1L << Math.max(0, attempts - 1)));
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                    
                    if (!success) {
                        failed.incrementAndGet();
                        String exName = lastException != null ? lastException.getClass().getSimpleName() : "Unknown";
                        String exMsg = lastException != null ? lastException.getMessage() : "No message";
                        FailureType failureType = classify(lastException);
                        failureDetails.add(new FailureDetail(
                                txn.accountLast4(),
                                YearMonth.from(txn.occurredAt()).toString(),
                                TxnIdentity.getId(txn),
                                attempts,
                                1,
                                1,
                                failureType,
                                exName,
                                exMsg,
                                false));
                        System.err.println("Failed to insert transaction " + TxnIdentity.getId(txn) + 
                            " after " + attempts + " attempts. Exception: " + exName + 
                            " - " + exMsg + ". FailureType: " + failureType);
                    }
                }));
            }

            executor.shutdown();
            if (!executor.awaitTermination(timeout, unit)) {
                System.err.println("Backfill executor timed out. Cancelling unfinished tasks...");
                status = Status.TIMED_OUT;
                timedOut = true;
                for (Future<?> future : futures) {
                    if (!future.isDone()) {
                        future.cancel(true);
                    }
                }
                executor.shutdownNow();
                terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
            }

            for (Future<?> future : futures) {
                if (future.isDone() && !future.isCancelled()) {
                    try {
                        future.get();
                    } catch (java.util.concurrent.ExecutionException e) {
                        failed.incrementAndGet();
                        status = Status.FAILED;
                        System.err.println("Backfill worker escaped with exception: " + e.getCause());
                    }
                }
            }

            System.out.println("Backfill complete. Read: " + read.get() + ", Written: " + written.get() 
                    + ", Source Deduplicated: " + sourceDeduplicated.get() + ", Target Skipped: " + targetSkipped.get() + ", Failed: " + failed.get());
                    
            if (failed.get() > 0) {
                status = Status.FAILED;
                System.err.println("Note: " + failed.get() + " records failed. Checkpointing is not implemented. A re-run will process all items and skip already written ones idempotently.");
            }
            if (status == Status.TIMED_OUT && !terminated) {
                System.err.println("Backfill timed out and executor did not terminate within the grace period.");
            }
            return new Result(read.get(), written.get(), sourceDeduplicated.get(), targetSkipped.get(), failed.get(),
                    timedOut, status, terminated, List.copyOf(failureDetails));
        } catch (InterruptedException e) {
            System.err.println("Backfill interrupted.");
            Thread.currentThread().interrupt();
            return new Result(read.get(), written.get(), sourceDeduplicated.get(), targetSkipped.get(), failed.get(),
                    true, Status.INTERRUPTED, false, List.copyOf(failureDetails));
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Backfill failed critically", e);
        }
    }

    private FailureType classify(Throwable e) {
        if (e == null) return FailureType.NON_RETRYABLE;
        
        if (e instanceof software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException ||
            e instanceof software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException ||
            e instanceof software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException) {
            return FailureType.RETRYABLE;
        }
        
        if (e instanceof software.amazon.awssdk.core.exception.ApiCallTimeoutException ||
            e instanceof software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException ||
            e instanceof software.amazon.awssdk.core.exception.RetryableException) {
            return FailureType.RETRYABLE;
        }

        if (e instanceof software.amazon.awssdk.awscore.exception.AwsServiceException) {
            software.amazon.awssdk.awscore.exception.AwsServiceException awsEx = 
                (software.amazon.awssdk.awscore.exception.AwsServiceException) e;
            if (awsEx.isThrottlingException() || awsEx.statusCode() >= 500) {
                return FailureType.RETRYABLE;
            }
        }

        if (e.getCause() != null) {
            return classify(e.getCause());
        }
        return FailureType.NON_RETRYABLE;
    }

    public enum Status {
        COMPLETED,
        FAILED,
        TIMED_OUT,
        INTERRUPTED
    }

    public enum FailureType {
        RETRYABLE,
        NON_RETRYABLE
    }

    public record Result(long read, long written, long sourceDeduplicated, long targetSkipped, long failed,
                         boolean timedOut, Status status, boolean executorTerminated,
                         List<FailureDetail> failureDetails) {}

    public record FailureDetail(String account, String month, String transactionId,
                                int attempts, long processedCount, long failedCount,
                                FailureType failureType, String errorType,
                                String errorMessage, boolean finalSuccess) {}
}
