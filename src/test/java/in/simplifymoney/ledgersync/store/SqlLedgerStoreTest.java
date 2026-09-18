package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlLedgerStoreTest {

    private SqlLedgerStore store;
    private Path dbFile;
    private Path migrationDir;

    @BeforeEach
    public void setup() throws Exception {
        Path tempDir = Files.createTempDirectory("dbtest");
        dbFile = tempDir.resolve("test_db");
        
        // H2 expects relative paths to be relative to CWD
        migrationDir = Path.of(System.getProperty("user.dir"), "db", "migration");
        
        store = new SqlLedgerStore(dbFile);
        store.migrate(migrationDir);
    }

    @AfterEach
    public void teardown() throws Exception {
        store.close();
    }

    @Test
    public void testConcurrentWritesAreSafe() throws Exception {
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);

        long initialCount = store.count();

        NormalizedTxn t = new NormalizedTxn(
                "1234", OffsetDateTime.parse("2026-07-01T10:00:00+05:30"), Direction.DEBIT, new BigDecimal("100.00"),
                Category.SPEND, "AMAZON", List.of("msg-1")
        );

        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                try {
                    latch.await();
                    store.save(t);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            }));
        }

        // Release all threads simultaneously
        latch.countDown();
        done.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        
        for (java.util.concurrent.Future<?> f : futures) {
            f.get(); // Re-throw any exceptions from the worker threads
        }

        // Only one record should exist (idempotent atomic merge)
        assertEquals(initialCount + 1, store.count());
    }

    @Test
    public void lowerSortingSourceIdDoesNotCreateSecondSqlTransaction() {
        long initialCount = store.count();
        OffsetDateTime occurredAt = OffsetDateTime.parse("2026-07-02T10:00:00+05:30");

        store.save(new NormalizedTxn(
                "1234", occurredAt, Direction.DEBIT, new BigDecimal("10.00"),
                Category.SPEND, "ANCHOR", List.of("msg-b")));

        try (SqlLedgerStore secondStore = new SqlLedgerStore(dbFile)) {
            secondStore.save(new NormalizedTxn(
                    "1234", occurredAt, Direction.DEBIT, new BigDecimal("10.00"),
                    Category.SPEND, "ANCHOR", List.of("msg-a", "msg-b")));
        }

        List<NormalizedTxn> anchorRows = store.all().stream()
                .filter(t -> t.merchant().equals("ANCHOR"))
                .toList();
        assertEquals(initialCount + 1, store.count());
        assertEquals(1, anchorRows.size());
        assertEquals(List.of("msg-a", "msg-b"), anchorRows.get(0).sourceMessageIds());
    }

    @Test
    public void independentIdenticalVisibleTransactionsRemainSeparate() {
        OffsetDateTime occurredAt = OffsetDateTime.parse("2026-07-03T10:00:00+05:30");

        store.save(new NormalizedTxn(
                "1234", occurredAt, Direction.DEBIT, new BigDecimal("11.00"),
                Category.SPEND, "TWIN", List.of("msg-twin-1")));
        store.save(new NormalizedTxn(
                "1234", occurredAt, Direction.DEBIT, new BigDecimal("11.00"),
                Category.SPEND, "TWIN", List.of("msg-twin-2")));

        long twinRows = store.all().stream()
                .filter(t -> t.merchant().equals("TWIN"))
                .count();
        assertEquals(2, twinRows);
    }

    @Test
    public void conflictingReuseOfSourceMessageRollsBack() {
        OffsetDateTime occurredAt = OffsetDateTime.parse("2026-07-04T10:00:00+05:30");
        store.save(new NormalizedTxn(
                "1234", occurredAt, Direction.DEBIT, new BigDecimal("12.00"),
                Category.SPEND, "CONFLICT", List.of("msg-conflict")));

        assertThrows(IllegalStateException.class, () -> store.save(new NormalizedTxn(
                "1234", occurredAt, Direction.DEBIT, new BigDecimal("13.00"),
                Category.SPEND, "CONFLICT", List.of("msg-conflict"))));

        List<NormalizedTxn> conflictRows = store.all().stream()
                .filter(t -> t.merchant().equals("CONFLICT"))
                .toList();
        assertEquals(1, conflictRows.size());
        assertEquals(new BigDecimal("12.00"), conflictRows.get(0).amount());
    }

    @Test
    public void concurrentIndependentWritesUseSeparateStoreInstances() throws Exception {
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        OffsetDateTime occurredAt = OffsetDateTime.parse("2026-07-05T10:00:00+05:30");

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            futures.add(executor.submit(() -> {
                ready.countDown();
                assertTrue(start.await(10, TimeUnit.SECONDS));
                try (SqlLedgerStore workerStore = new SqlLedgerStore(dbFile)) {
                    workerStore.save(new NormalizedTxn(
                            "1234", occurredAt.plusSeconds(index), Direction.DEBIT,
                            new BigDecimal("20.00").add(new BigDecimal(index + ".00")),
                            Category.SPEND, "CONCURRENT", List.of("msg-concurrent-" + index)));
                }
                return null;
            }));
        }

        assertTrue(ready.await(10, TimeUnit.SECONDS));
        start.countDown();
        for (java.util.concurrent.Future<?> future : futures) {
            future.get();
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        long concurrentRows = store.all().stream()
                .filter(t -> t.merchant().equals("CONCURRENT"))
                .count();
        assertEquals(threadCount, concurrentRows);
    }
    @Test
    public void conflictingCategoryForOwnedSourceIsRejected() {
        OffsetDateTime occurredAt = OffsetDateTime.parse("2026-07-06T10:00:00+05:30");
        store.save(new NormalizedTxn(
                "1234", occurredAt, Direction.DEBIT, new BigDecimal("10.00"),
                Category.SPEND, "CATMERCH", List.of("msg-cat")));

        // Same source message ID but different category — must be rejected
        assertThrows(IllegalStateException.class, () -> store.save(new NormalizedTxn(
                "1234", occurredAt, Direction.DEBIT, new BigDecimal("10.00"),
                Category.MICRO, "CATMERCH", List.of("msg-cat"))));

        // The original record must be unchanged
        List<NormalizedTxn> rows = store.all().stream()
                .filter(t -> t.merchant().equals("CATMERCH")).toList();
        assertEquals(1, rows.size());
        assertEquals(Category.SPEND, rows.get(0).category());
    }

    @Test
    public void conflictingMerchantForOwnedSourceIsRejected() {
        OffsetDateTime occurredAt = OffsetDateTime.parse("2026-07-07T10:00:00+05:30");
        store.save(new NormalizedTxn(
                "1234", occurredAt, Direction.DEBIT, new BigDecimal("10.00"),
                Category.SPEND, "ORIGINAL_MERCH", List.of("msg-merch")));

        // Same source message ID but different merchant — must be rejected
        assertThrows(IllegalStateException.class, () -> store.save(new NormalizedTxn(
                "1234", occurredAt, Direction.DEBIT, new BigDecimal("10.00"),
                Category.SPEND, "CHANGED_MERCH", List.of("msg-merch"))));

        List<NormalizedTxn> rows = store.all().stream()
                .filter(t -> t.merchant() != null && t.merchant().equals("ORIGINAL_MERCH")).toList();
        assertEquals(1, rows.size());
    }
}
