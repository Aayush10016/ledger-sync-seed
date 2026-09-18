package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

import java.math.BigDecimal;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DynamoDbLedgerStoreIntegrationTest {

    private DynamoDbLedgerStore store;

    @BeforeEach
    public void setup() {
        DynamoDbClient client = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .region(software.amazon.awssdk.regions.Region.US_EAST_1)
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("dummy", "dummy")))
                .build();
        
        try {
            client.deleteTable(DeleteTableRequest.builder().tableName("LedgerStore").build());
            // wait for deletion
            client.waiter().waitUntilTableNotExists(b -> b.tableName("LedgerStore"));
        } catch (ResourceNotFoundException e) {
            // ignore
        } catch (Exception e) {
            if ("true".equals(System.getenv("CI"))) {
                throw new IllegalStateException("DynamoDB Local is required in CI but failed to start/connect.", e);
            }
            Assumptions.assumeTrue(false, "DynamoDB Local is not available on port 8000. Skipping tests.");
        }

        store = new DynamoDbLedgerStore(client);
        
        // Wait for creation to complete
        client.waiter().waitUntilTableExists(b -> b.tableName("LedgerStore"));
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
}
