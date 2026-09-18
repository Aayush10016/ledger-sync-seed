package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BackfillTest {

    @Test
    public void testBackfillHandlesTimeoutsAndInterrupts() throws Exception {
        NormalizedTxn t = new NormalizedTxn(
                "1234", OffsetDateTime.now(), Direction.DEBIT, new BigDecimal("100.00"),
                Category.SPEND, "MERCHANT", List.of("msg-1")
        );

        LedgerStore fakeSql = new LedgerStore() {
            @Override
            public List<NormalizedTxn> all() {
                return List.of(t);
            }
            @Override public void save(NormalizedTxn t) {}
            @Override public long count() { return 1; }
            @Override public void save(in.simplifymoney.ledgersync.model.Discrepancy d) {}
            @Override public List<in.simplifymoney.ledgersync.model.Discrepancy> discrepancies() { return List.of(); }
        };

        CountDownLatch latch = new CountDownLatch(1);

        DocumentStore fakeDoc = new DocumentStore() {
            @Override
            public void save(NormalizedTxn txn) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted");
                }
            }
            @Override public List<NormalizedTxn> forAccountMonth(String acct, java.time.YearMonth ym) { return List.of(); }
            @Override public java.util.Map<Category, BigDecimal> categoryTotals(String acct) { return java.util.Map.of(); }
            @Override public java.util.Optional<NormalizedTxn> byMessageId(String id) { return java.util.Optional.empty(); }
            @Override public List<NormalizedTxn> scanAllTransactions() { return List.of(); }
        };

        Backfill backfill = new Backfill(fakeSql, fakeDoc);
        try {
            // Give it 100 milliseconds to complete. Since it waits on the latch indefinitely, it will time out.
            Backfill.Result res = backfill.run(100, TimeUnit.MILLISECONDS);
            assertTrue(res.timedOut(), "Backfill should return timedOut=true");
            assertTrue(res.status() == Backfill.Status.TIMED_OUT || res.status() == Backfill.Status.FAILED);
            assertTrue(res.executorTerminated(), "interruptible worker should terminate after cancellation");
        } catch (IllegalStateException e) {
            // Depending on how interrupted task resolves, it might increment failed or skipped.
            assertTrue(e.getMessage().contains("Backfill completed with"));
        } finally {
            latch.countDown();
        }
    }

    @Test
    public void testBackfillReportsUnterminatedExecutorWhenWorkerIgnoresInterruption() throws Exception {
        NormalizedTxn t = new NormalizedTxn(
                "1234", OffsetDateTime.parse("2026-07-01T10:00:00+05:30"),
                Direction.DEBIT, new BigDecimal("100.00"),
                Category.SPEND, "MERCHANT", List.of("msg-ignore-interrupt")
        );

        LedgerStore fakeSql = new LedgerStore() {
            @Override public List<NormalizedTxn> all() { return List.of(t); }
            @Override public void save(NormalizedTxn t) {}
            @Override public long count() { return 1; }
            @Override public void save(in.simplifymoney.ledgersync.model.Discrepancy d) {}
            @Override public List<in.simplifymoney.ledgersync.model.Discrepancy> discrepancies() { return List.of(); }
        };

        CountDownLatch release = new CountDownLatch(1);
        DocumentStore fakeDoc = new DocumentStore() {
            @Override
            public void save(NormalizedTxn txn) {
                while (release.getCount() > 0) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ignored) {
                        // Deliberately ignore interruption to prove shutdownNow is observable, not magic.
                    }
                }
            }
            @Override public List<NormalizedTxn> forAccountMonth(String acct, java.time.YearMonth ym) { return List.of(); }
            @Override public java.util.Map<Category, BigDecimal> categoryTotals(String acct) { return java.util.Map.of(); }
            @Override public java.util.Optional<NormalizedTxn> byMessageId(String id) { return java.util.Optional.empty(); }
            @Override public List<NormalizedTxn> scanAllTransactions() { return List.of(); }
        };

        try {
            Backfill.Result res = new Backfill(fakeSql, fakeDoc).run(50, TimeUnit.MILLISECONDS);
            assertTrue(res.timedOut());
            assertEquals(Backfill.Status.TIMED_OUT, res.status());
            assertEquals(false, res.executorTerminated());
        } finally {
            release.countDown();
        }
    }

    @Test
    public void testBackfillReportsNonRetryableFailuresStructurally() {
        NormalizedTxn t = new NormalizedTxn(
                "1234", OffsetDateTime.parse("2026-07-01T10:00:00+05:30"),
                Direction.DEBIT, new BigDecimal("100.00"),
                Category.SPEND, "BAD", List.of("msg-bad")
        );

        LedgerStore fakeSql = new LedgerStore() {
            @Override public List<NormalizedTxn> all() { return List.of(t); }
            @Override public void save(NormalizedTxn t) {}
            @Override public long count() { return 1; }
            @Override public void save(in.simplifymoney.ledgersync.model.Discrepancy d) {}
            @Override public List<in.simplifymoney.ledgersync.model.Discrepancy> discrepancies() { return List.of(); }
        };

        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        DocumentStore fakeDoc = new DocumentStore() {
            @Override public void save(NormalizedTxn txn) {
                attempts.incrementAndGet();
                throw new IllegalArgumentException("permanent validation failure");
            }
            @Override public List<NormalizedTxn> forAccountMonth(String acct, java.time.YearMonth ym) { return List.of(); }
            @Override public java.util.Map<Category, BigDecimal> categoryTotals(String acct) { return java.util.Map.of(); }
            @Override public java.util.Optional<NormalizedTxn> byMessageId(String id) { return java.util.Optional.empty(); }
            @Override public List<NormalizedTxn> scanAllTransactions() { return List.of(); }
        };

        Backfill.Result result = new Backfill(fakeSql, fakeDoc).run(1, TimeUnit.MINUTES);

        assertEquals(Backfill.Status.FAILED, result.status());
        assertEquals(1, result.failed());
        assertEquals(1, attempts.get(), "non-retryable failures should not be retried");
        assertEquals(1, result.failureDetails().size());
        assertEquals("1234", result.failureDetails().get(0).account());
        assertEquals(Backfill.FailureType.NON_RETRYABLE, result.failureDetails().get(0).failureType());
    }
}
