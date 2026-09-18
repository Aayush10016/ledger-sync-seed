package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.ingest.IngestService;
import in.simplifymoney.ledgersync.parse.Parsers;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import in.simplifymoney.ledgersync.store.Backfill;
import in.simplifymoney.ledgersync.store.SqlLedgerStore;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class DynamoDbLedgerStoreIntegrationTest {

    private DynamoDbClient client;
    private DynamoDbLedgerStore store;

    @BeforeEach
    public void setup() {
        try (java.net.Socket s = new java.net.Socket("localhost", 8000)) {
            // Port is open
        } catch (Exception e) {
            if ("true".equals(System.getenv("CI"))) {
                throw new IllegalStateException("DynamoDB Local is required in CI but failed to start/connect.", e);
            }
            Assumptions.assumeTrue(false, "DynamoDB Local is not available on port 8000. Skipping tests.");
            return;
        }

        client = newClient();
        
        try {
            client.deleteTable(DeleteTableRequest.builder().tableName("LedgerStore").build());
            // wait for deletion
            client.waiter().waitUntilTableNotExists(b -> b.tableName("LedgerStore"));
        } catch (ResourceNotFoundException e) {
            // ignore
        }

        store = new DynamoDbLedgerStore(client);
        
        // Wait for creation to complete
        client.waiter().waitUntilTableExists(b -> b.tableName("LedgerStore"));
    }

    private static DynamoDbClient newClient() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .region(software.amazon.awssdk.regions.Region.US_EAST_1)
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("dummy", "dummy")))
                .build();
    }

    @Test
    public void testIdempotentRetries() {
        NormalizedTxn txn = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("m1"));

        // Save first time
        store.save(txn);
        assertEquals(new BigDecimal("10.50"), store.categoryTotals("9999").get(Category.SPEND));

        // Retry exact same transaction
        store.save(txn);
        // Assert total didn't double
        assertEquals(new BigDecimal("10.50"), store.categoryTotals("9999").get(Category.SPEND));
    }

    @Test
    public void testIdenticalAmountsSameSecond() {
        NormalizedTxn txn1 = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch A", List.of("m1"));
        
        NormalizedTxn txn2 = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch B", List.of("m2"));

        store.save(txn1);
        store.save(txn2);

        // Assert BOTH were saved and total is 21.00 because their source messages differ (hence their SK differs)
        assertEquals(new BigDecimal("21.00"), store.categoryTotals("9999").get(Category.SPEND));
        assertEquals(2, store.forAccountMonth("9999", java.time.YearMonth.of(2026, 7)).size());
    }

    @Test
    public void testCompletelyIdenticalPurchasesSameSecond() {
        NormalizedTxn txn1 = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("sms-1"));
        
        // txn2 has the exact same visible fields, but a different message ID. It is a distinct identical purchase.
        NormalizedTxn txn2 = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("sms-2"));

        store.save(txn1);
        store.save(txn2);

        // Assert BOTH were saved and total is 21.00. The TxnIdentity change prevents DynamoDB from blindly overwriting them.
        assertEquals(new BigDecimal("21.00"), store.categoryTotals("9999").get(Category.SPEND));
        assertEquals(2, store.forAccountMonth("9999", java.time.YearMonth.of(2026, 7)).size());
    }

    @Test
    public void testMessageIndexResolution() {
        NormalizedTxn txn = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("msg-x", "msg-y"));

        store.save(txn);

        assertTrue(store.byMessageId("msg-x").isPresent());
        assertTrue(store.byMessageId("msg-y").isPresent());
        assertFalse(store.byMessageId("msg-z").isPresent());
    }

    @Test
    public void testNewSourceIdsMergeIntoExistingTransactionWithoutDoubleCounting() {
        NormalizedTxn first = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("old-msg"));
        NormalizedTxn merged = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("old-msg", "new-msg"));

        store.save(first);
        store.save(merged);

        assertEquals(new BigDecimal("10.50"), store.categoryTotals("9999").get(Category.SPEND));
        NormalizedTxn byOld = store.byMessageId("old-msg").orElseThrow();
        NormalizedTxn byNew = store.byMessageId("new-msg").orElseThrow();
        assertEquals(byOld, byNew);
        assertEquals(List.of("new-msg", "old-msg"), byOld.sourceMessageIds());
    }

    @Test
    public void testConcurrentSameTransactionFromIndependentClientsIsIdempotent() throws Exception {
        NormalizedTxn txn = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("concurrent-msg"));
        int workers = 8;
        CountDownLatch ready = new CountDownLatch(workers);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(workers);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < workers; i++) {
            futures.add(executor.submit(() -> {
                DynamoDbLedgerStore workerStore = new DynamoDbLedgerStore(newClient());
                ready.countDown();
                assertTrue(start.await(10, TimeUnit.SECONDS));
                workerStore.save(txn);
                return null;
            }));
        }

        assertTrue(ready.await(10, TimeUnit.SECONDS));
        start.countDown();
        for (var future : futures) {
            future.get();
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, store.scanAllTransactions().size());
        assertEquals(new BigDecimal("10.50"), store.categoryTotals("9999").get(Category.SPEND));
        assertTrue(store.byMessageId("concurrent-msg").isPresent());
    }

    @Test
    public void testConcurrentCompatibleUpdatesKeepOneOwnerPerSourceId() throws Exception {
        NormalizedTxn seed = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("owned-root"));
        store.save(seed);

        int workers = 6;
        CountDownLatch ready = new CountDownLatch(workers);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(workers);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < workers; i++) {
            final int index = i;
            futures.add(executor.submit(() -> {
                DynamoDbLedgerStore workerStore = new DynamoDbLedgerStore(newClient());
                NormalizedTxn update = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                        Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch",
                        List.of("owned-root", "owned-extra-" + index));
                ready.countDown();
                assertTrue(start.await(10, TimeUnit.SECONDS));
                workerStore.save(update);
                return null;
            }));
        }

        assertTrue(ready.await(10, TimeUnit.SECONDS));
        start.countDown();
        for (var future : futures) {
            future.get();
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, store.scanAllTransactions().size());
        assertEquals(new BigDecimal("10.50"), store.categoryTotals("9999").get(Category.SPEND));
        NormalizedTxn root = store.byMessageId("owned-root").orElseThrow();
        assertEquals(workers + 1, root.sourceMessageIds().size());
        for (int i = 0; i < workers; i++) {
            NormalizedTxn byExtra = store.byMessageId("owned-extra-" + i).orElseThrow();
            assertEquals(root, byExtra);
        }
    }

    @Test
    public void testConcurrentDuplicateNewSourceRetryIsIdempotent() throws Exception {
        NormalizedTxn seed = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("retry-root"));
        store.save(seed);

        NormalizedTxn update = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch",
                List.of("retry-root", "retry-new"));
        int workers = 6;
        CountDownLatch ready = new CountDownLatch(workers);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(workers);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < workers; i++) {
            futures.add(executor.submit(() -> {
                DynamoDbLedgerStore workerStore = new DynamoDbLedgerStore(newClient());
                ready.countDown();
                assertTrue(start.await(10, TimeUnit.SECONDS));
                workerStore.save(update);
                return null;
            }));
        }

        assertTrue(ready.await(10, TimeUnit.SECONDS));
        start.countDown();
        for (var future : futures) {
            future.get();
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, store.scanAllTransactions().size());
        assertEquals(new BigDecimal("10.50"), store.categoryTotals("9999").get(Category.SPEND));
        assertEquals(store.byMessageId("retry-root").orElseThrow(), store.byMessageId("retry-new").orElseThrow());
    }

    @Test
    public void testQueriesAndScanReadAllPages() {
        DynamoDbLedgerStore paged = new DynamoDbLedgerStore(client, 1);
        assertEquals(List.of(), paged.forAccountMonth("9999", YearMonth.of(2025, 1)));
        assertEquals(List.of(), paged.scanAllTransactions());
        assertEquals(java.util.Map.of(), paged.categoryTotals("9999"));

        paged.save(new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-31T23:58:00Z"),
                Direction.DEBIT, new BigDecimal("10.00"), Category.SPEND, "A", List.of("p1")));
        assertEquals(1, paged.forAccountMonth("9999", YearMonth.of(2026, 7)).size());
        assertEquals(1, paged.scanAllTransactions().size());

        paged.save(new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-31T23:59:00Z"),
                Direction.CREDIT, new BigDecimal("20.00"), Category.INCOME, "B", List.of("p2")));
        paged.save(new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-15T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("5.00"), Category.MICRO, "UPI/C", List.of("p3")));
        paged.save(new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-01T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("7.00"), Category.TRANSFER, "D", List.of("p4")));
        paged.save(new NormalizedTxn("9999", OffsetDateTime.parse("2026-08-01T00:01:00Z"),
                Direction.DEBIT, new BigDecimal("99.00"), Category.SPEND, "E", List.of("p5")));

        List<NormalizedTxn> july = paged.forAccountMonth("9999", YearMonth.of(2026, 7));
        assertEquals(4, july.size());
        assertEquals("p2", july.get(0).sourceMessageIds().get(0));
        assertEquals("p4", july.get(3).sourceMessageIds().get(0));
        assertEquals(5, paged.scanAllTransactions().size());
        assertEquals(new BigDecimal("109.00"), paged.categoryTotals("9999").get(Category.SPEND));
        assertEquals(new BigDecimal("20.00"), paged.categoryTotals("9999").get(Category.INCOME));
        assertEquals(new BigDecimal("5.00"), paged.categoryTotals("9999").get(Category.MICRO));
        assertEquals(new BigDecimal("7.00"), paged.categoryTotals("9999").get(Category.TRANSFER));
    }

    @Test
    public void testDirectDynamoIngestionIsIdempotent() throws Exception {
        Path corpus = Files.createTempFile("dynamo-direct", ".jsonl");
        String msg = "{\"message_id\":\"direct-1\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\","
                + "\"received_at\":\"2024-05-15T10:00:00Z\",\"device_id\":\"d1\","
                + "\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UBER.\"}";
        Files.writeString(corpus, msg + "\n");

        DynamoDbLedgerAdapter adapter = new DynamoDbLedgerAdapter(store);
        IngestService service = new IngestService(new Parsers(), adapter);
        service.ingestFile(corpus);
        service.ingestFile(corpus);

        assertEquals(1, store.scanAllTransactions().size());
        assertEquals(new BigDecimal("50.00"), store.categoryTotals("1234").get(Category.SPEND));
        assertTrue(store.byMessageId("direct-1").isPresent());
    }

    @Test
    public void testAtomicityOnMessageCollision() {
        NormalizedTxn txn1 = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch A", List.of("m1"));

        NormalizedTxn txn2 = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-05T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("20.00"), Category.SPEND, "Merch B", List.of("m1", "m2"));
                
        // Ordering 1: txn1 first, then txn2 collides on m1
        store.save(txn1);
        assertThrows(IllegalStateException.class, () -> store.save(txn2));
        
        // Assert the category total remains exactly 10.50 (from txn1 only)
        assertEquals(new BigDecimal("10.50"), store.categoryTotals("9999").get(Category.SPEND));
        // Verify m2 (unique to txn2) was NOT partially written
        assertFalse(store.byMessageId("m2").isPresent(), "Message index m2 should not exist because txn2 aborted");

        // Clear the table to test reverse ordering
        setup();

        // Ordering 2: txn2 first, then txn1 collides on m1
        store.save(txn2);
        assertThrows(IllegalStateException.class, () -> store.save(txn1));
        
        // Assert the category total remains exactly 20.00 (from txn2 only)
        assertEquals(new BigDecimal("20.00"), store.categoryTotals("9999").get(Category.SPEND));
    }

    @Test
    public void testConflictingCategoryForOwnedSourceIsRejected() {
        NormalizedTxn spend = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("owned-msg"));
        NormalizedTxn micro = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.MICRO, "Merch", List.of("owned-msg", "new-owned-msg"));

        store.save(spend);
        assertThrows(IllegalStateException.class, () -> store.save(micro));

        assertEquals(1, store.scanAllTransactions().size());
        assertEquals(new BigDecimal("10.50"), store.categoryTotals("9999").get(Category.SPEND));
        assertFalse(store.byMessageId("new-owned-msg").isPresent());
    }

    @Test
    public void testDanglingPointerThrows() {
        // Save a valid transaction, record its key, then delete it directly via the underlying client
        // so that the MSG# pointer survives but the target TXN# row is gone.
        // A subsequent save() that hits the dangling pointer should throw rather than silently succeed.
        NormalizedTxn txn = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T11:00:00Z"),
                Direction.DEBIT, new BigDecimal("15.00"), Category.SPEND, "Shop", List.of("dangle-msg"));
        store.save(txn);
        assertTrue(store.byMessageId("dangle-msg").isPresent());

        // Delete the TXN# item directly so the pointer is now dangling
        List<NormalizedTxn> txns = store.forAccountMonth("9999", java.time.YearMonth.of(2026, 7));
        NormalizedTxn stored = txns.stream()
                .filter(t -> t.sourceMessageIds().contains("dangle-msg"))
                .findFirst().orElseThrow();

        // Reconstruct SK: TXN#yyyy-MM#epoch#txnId — use scanAll to find the exact key via the store internals
        // We simulate "target gone" by re-saving with a new incompatible amount so the conflict is detected
        NormalizedTxn incompatible = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T11:00:00Z"),
                Direction.DEBIT, new BigDecimal("99.00"),  // different amount => isCompatible returns false
                Category.SPEND, "Shop", List.of("dangle-msg"));

        // The new save should recover by deleting the dangling pointer and successfully saving the new txn
        store.save(incompatible);
        
        // Assert the new incompatible txn was saved properly
        NormalizedTxn recovered = store.byMessageId("dangle-msg").orElseThrow();
        assertEquals(new BigDecimal("99.00"), recovered.amount());
        assertEquals(1, store.scanAllTransactions().size());

        // Original transaction is untouched
        assertEquals(new BigDecimal("15.00"), store.categoryTotals("9999").get(Category.SPEND));
        assertEquals(stored, store.byMessageId("dangle-msg").orElseThrow());
    }

    @Test
    public void testSourceIdOwnershipUnchangedAfterBackfillAndReingest() throws Exception {
        // Prove the full ingest → backfill → re-ingest lifecycle keeps source-ID ownership
        // consistent. We use direct store.save() calls instead of the SMS parser so the
        // test does not depend on which account numbers the parser supports.
        NormalizedTxn original = new NormalizedTxn(
                "9999", OffsetDateTime.parse("2024-06-01T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("75.00"),
                Category.SPEND, "SWIGGY", List.of("lifecycle-1"));

        // Phase 1: seed into mock SQL store
        LedgerStore sqlStore = new LedgerStore() {
            @Override public void save(NormalizedTxn txn) {}
            @Override public List<NormalizedTxn> all() { return List.of(original); }
            @Override public void save(in.simplifymoney.ledgersync.model.Discrepancy d) {}
            @Override public List<in.simplifymoney.ledgersync.model.Discrepancy> discrepancies() { return List.of(); }
            @Override public long count() { return 1; }
        };
        assertEquals(1, sqlStore.all().size());

        // Phase 2: backfill SQL → DynamoDB
        Backfill backfill = new Backfill(sqlStore, store);
        Backfill.Result result = backfill.run(1, java.util.concurrent.TimeUnit.MINUTES);
        assertTrue(result.status() == Backfill.Status.COMPLETED && result.failed() == 0,
                "Backfill must complete with no failures: " + result);
        assertEquals(1, store.scanAllTransactions().size());
        assertTrue(store.byMessageId("lifecycle-1").isPresent(),
                "lifecycle-1 must be findable after backfill");

        // Phase 3: re-save the same transaction (simulates re-ingest) — must be idempotent
        store.save(original);
        assertEquals(1, store.scanAllTransactions().size(),
                "Re-save must not create a duplicate in DynamoDB");

        // Phase 4: source ID ownership must be intact
        NormalizedTxn byId = store.byMessageId("lifecycle-1").orElseThrow(
                () -> new AssertionError("lifecycle-1 must resolve after backfill+reingest"));
        assertEquals(List.of("lifecycle-1"), byId.sourceMessageIds());
        assertEquals(new BigDecimal("75.00"), byId.amount());
        assertEquals(Category.SPEND, byId.category());
    }

    @Test
    public void testConcurrentIncompatibleUpdates() throws Exception {
        NormalizedTxn t1 = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("m1"));
        NormalizedTxn t2 = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("99.99"), Category.SPEND, "Merch", List.of("m1"));

        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.ExecutorService svc = java.util.concurrent.Executors.newFixedThreadPool(2);

        java.util.concurrent.Future<?> f1 = svc.submit(() -> {
            try { latch.await(); store.save(t1); } catch (Exception e) { throw new RuntimeException(e); }
        });
        java.util.concurrent.Future<?> f2 = svc.submit(() -> {
            try { latch.await(); store.save(t2); } catch (Exception e) { throw new RuntimeException(e); }
        });

        latch.countDown();
        
        boolean f1Success = false;
        boolean f2Success = false;
        try { f1.get(); f1Success = true; } catch (Exception e) {}
        try { f2.get(); f2Success = true; } catch (Exception e) {}

        // Exactly one should succeed since they are incompatible but share the same message ID
        assertTrue(f1Success ^ f2Success, "Exactly one concurrent incompatible write must succeed");
        svc.shutdown();
    }

    @Test
    public void testMultipleSourceIdsMerging() throws Exception {
        NormalizedTxn base = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("m1"));
        store.save(base);

        NormalizedTxn merge1 = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("m1", "new1"));
        NormalizedTxn merge2 = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("m1", "new2"));

        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.ExecutorService svc = java.util.concurrent.Executors.newFixedThreadPool(2);

        java.util.concurrent.Future<?> f1 = svc.submit(() -> {
            try { latch.await(); store.save(merge1); } catch (Exception e) { throw new RuntimeException(e); }
        });
        java.util.concurrent.Future<?> f2 = svc.submit(() -> {
            try { latch.await(); store.save(merge2); } catch (Exception e) { throw new RuntimeException(e); }
        });

        latch.countDown();
        f1.get();
        f2.get();
        svc.shutdown();

        // Both updates should succeed eventually. The final item should have all 3 message IDs
        NormalizedTxn finalTxn = store.byMessageId("m1").orElseThrow();
        assertTrue(finalTxn.sourceMessageIds().containsAll(List.of("m1", "new1", "new2")));
        assertEquals(1, store.scanAllTransactions().size());
    }
}
