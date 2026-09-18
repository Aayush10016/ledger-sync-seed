package in.simplifymoney.ledgersync;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.store.DynamoDbLedgerStore;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.math.BigDecimal;
import java.net.URI;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PerfTest {

    private static final String TABLE = "LedgerStore";

    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 100_000;
        DynamoDbClient client = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummy", "dummy")))
                .build();
        
        DynamoDbLedgerStore store = new DynamoDbLedgerStore(client);
        client.waiter().waitUntilTableExists(b -> b.tableName(TABLE));

        System.out.println("Inserting " + count + " records (this will take a moment)...");
        
        String account = String.format("%04d", Math.floorMod(System.currentTimeMillis(), 10_000));
        String runId = "perf-" + System.currentTimeMillis();
        OffsetDateTime baseTime = OffsetDateTime.parse("2026-01-01T10:00:00Z");

        AtomicInteger failures = new AtomicInteger();
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();

        java.util.stream.IntStream.range(0, count).parallel().forEach(i -> {
            if (i % 10000 == 0) System.out.println("Attempted " + i);
            NormalizedTxn txn = new NormalizedTxn(
                    account,
                    baseTime.plusSeconds(i),
                    Direction.DEBIT,
                    new BigDecimal("10.00"),
                    Category.SPEND,
                    "TEST_MERCHANT",
                    List.of(runId + "-msg-" + i)
            );
            try {
                store.save(txn);
            } catch (Exception e) {
                failures.incrementAndGet();
                firstFailure.compareAndSet(null, e);
            }
        });

        if (failures.get() > 0) {
            throw new IllegalStateException("Perf insert failed for " + failures.get()
                    + " records; first failure follows.", firstFailure.get());
        }
        
        System.out.println("Insertion complete. Running queries...");

        System.out.println("\n--- Q1: forAccountMonth ---");
        QueryMetrics month = queryMetrics(client, "ACCT#" + account,
                "TXN#" + YearMonth.from(baseTime), false);
        System.out.println(month);

        System.out.println("\n--- Q2: categoryTotals ---");
        QueryMetrics totals = queryMetrics(client, "ACCT#" + account, "CAT#", true);
        System.out.println(totals);

        System.out.println("\n--- Q3: byMessageId ---");
        PointLookupMetrics byMessage = messageLookupMetrics(client, runId + "-msg-" + (count / 2));
        System.out.println(byMessage);
        
        System.out.println("Done.");
        System.exit(0);
    }

    private static QueryMetrics queryMetrics(DynamoDbClient client, String pk, String skPrefix, boolean forward) {
        QueryRequest req = QueryRequest.builder()
                .tableName(TABLE)
                .keyConditionExpression("PK = :pk AND begins_with(SK, :sk)")
                .expressionAttributeValues(Map.of(
                        ":pk", AttributeValue.builder().s(pk).build(),
                        ":sk", AttributeValue.builder().s(skPrefix).build()))
                .scanIndexForward(forward)
                .build();

        int pages = 0;
        int returned = 0;
        int examined = 0;
        QueryResponse res;
        do {
            res = client.query(req);
            pages++;
            returned += res.count();
            examined += res.scannedCount();
            req = req.toBuilder().exclusiveStartKey(res.lastEvaluatedKey()).build();
        } while (res.lastEvaluatedKey() != null && !res.lastEvaluatedKey().isEmpty());

        return new QueryMetrics(pages, examined, returned);
    }

    private static PointLookupMetrics messageLookupMetrics(DynamoDbClient client, String messageId) {
        int getItemCalls = 1;
        int returnedItems = 0;
        GetItemResponse pointer = client.getItem(GetItemRequest.builder()
                .tableName(TABLE)
                .key(Map.of(
                        "PK", AttributeValue.builder().s("MSG#" + messageId).build(),
                        "SK", AttributeValue.builder().s("MSG").build()))
                .build());
        if (!pointer.hasItem()) {
            return new PointLookupMetrics(getItemCalls, returnedItems, false);
        }
        returnedItems++;

        getItemCalls++;
        GetItemResponse target = client.getItem(GetItemRequest.builder()
                .tableName(TABLE)
                .key(Map.of(
                        "PK", pointer.item().get("targetPk"),
                        "SK", pointer.item().get("targetSk")))
                .build());
        if (target.hasItem()) {
            returnedItems++;
        }
        return new PointLookupMetrics(getItemCalls, returnedItems, target.hasItem());
    }

    private record QueryMetrics(int pages, int examined, int returned) {}

    private record PointLookupMetrics(int getItemCalls, int returnedItems, boolean transactionFound) {}
}
