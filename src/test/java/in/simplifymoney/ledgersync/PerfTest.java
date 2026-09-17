package in.simplifymoney.ledgersync;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.store.DynamoDbLedgerStore;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.math.BigDecimal;
import java.net.URI;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PerfTest {

    public static void main(String[] args) throws Exception {
        DynamoDbClient client = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .build();
        
        DynamoDbLedgerStore store = new DynamoDbLedgerStore(client, "ledger_perf");
        store.init(); // Creates the table if not exists

        System.out.println("Inserting 100,000 records (this will take a moment)...");
        
        // We simulate inserting 100,000 transactions for the same account in the same month.
        // Actually, to make it realistic, we'll insert them concurrently.
        String account = "9999";
        OffsetDateTime baseTime = OffsetDateTime.parse("2026-01-01T10:00:00Z");
        
        java.util.stream.IntStream.range(0, 100_000).parallel().forEach(i -> {
            if (i % 10000 == 0) System.out.println("Inserted " + i);
            NormalizedTxn txn = new NormalizedTxn(
                    account,
                    baseTime.plusSeconds(i),
                    Direction.DEBIT,
                    new BigDecimal("10.00"),
                    Category.SPEND,
                    "TEST_MERCHANT",
                    List.of("msg-" + i)
            );
            try {
                store.save(txn);
            } catch (Exception e) {} // ignore local errors for perf test
        });
        
        System.out.println("Insertion complete. Running queries...");

        // Q1
        System.out.println("\n--- Q1: forAccountMonth ---");
        store.forAccountMonth(account, YearMonth.from(baseTime));

        // Q2
        System.out.println("\n--- Q2: categoryTotals ---");
        store.categoryTotals(account);

        // Q3
        System.out.println("\n--- Q3: byMessageId ---");
        store.byMessageId("msg-50000");
        
        System.out.println("Done.");
        System.exit(0);
    }
}
