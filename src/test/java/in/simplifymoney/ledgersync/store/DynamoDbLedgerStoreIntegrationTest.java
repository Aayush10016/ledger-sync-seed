package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
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
                .build();
        
        try {
            client.deleteTable(DeleteTableRequest.builder().tableName("LedgerStore").build());
        } catch (ResourceNotFoundException e) {} // ignore

        store = new DynamoDbLedgerStore(client);
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
    public void testMessageIndexResolution() {
        NormalizedTxn txn = new NormalizedTxn("9999", OffsetDateTime.parse("2026-07-04T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("10.50"), Category.SPEND, "Merch", List.of("msg-x", "msg-y"));

        store.save(txn);

        assertTrue(store.byMessageId("msg-x").isPresent());
        assertTrue(store.byMessageId("msg-y").isPresent());
        assertFalse(store.byMessageId("msg-z").isPresent());
    }
}
