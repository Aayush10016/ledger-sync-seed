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
}
