package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EndToEndBackfillIntegrationTest {

    private SqlLedgerStore sqlStore;
    private DynamoDbLedgerStore dynamoStore;
    private DynamoDbClient client;
    private Path dbFile;

    private static final String TABLE_NAME = "LedgerStore";

    @BeforeEach
    public void setup() throws Exception {
        try (java.net.Socket s = new java.net.Socket("localhost", 8000)) {
            // Port is open
        } catch (Exception e) {
            if ("true".equals(System.getenv("CI"))) {
                throw new IllegalStateException("DynamoDB Local is required in CI but failed to start/connect.", e);
            }
            org.junit.jupiter.api.Assumptions.assumeTrue(false, "DynamoDB Local is not available on port 8000. Skipping tests.");
            return;
        }

        // 1. Setup SQL Store
        Path tempDir = Files.createTempDirectory("dbtest");
        dbFile = tempDir.resolve("test_db");
        Path migrationDir = Path.of(System.getProperty("user.dir"), "db", "migration");
        sqlStore = new SqlLedgerStore(dbFile);
        sqlStore.migrate(migrationDir);

        // 2. Setup DynamoDB Store
        client = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .region(Region.AP_SOUTH_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummy", "dummy")))
                .build();

        try {
            client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
        } catch (ResourceNotFoundException ignored) {}

        client.createTable(CreateTableRequest.builder()
                .tableName(TABLE_NAME)
                .keySchema(
                        KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build()
                )
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("PK").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("SK").attributeType(ScalarAttributeType.S).build()
                )
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());

        dynamoStore = new DynamoDbLedgerStore(client);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (client != null) {
            try {
                client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
            } catch (ResourceNotFoundException ignored) {}
            client.close();
        }
        if (sqlStore != null) {
            sqlStore.close();
        }
        if (dbFile != null) {
            Files.deleteIfExists(dbFile);
        }
    }

    @Test
    public void testFullSqlToDynamoDbPipeline() throws Exception {
        // Insert diverse transactions into SQL
        NormalizedTxn t1 = new NormalizedTxn(
                "1234", OffsetDateTime.parse("2026-07-01T10:00:00+05:30"), Direction.DEBIT,
                new BigDecimal("100.00"), Category.SPEND, "MERCH1", List.of("msg1"), "REF1"
        );
        NormalizedTxn t2 = new NormalizedTxn(
                "1234", OffsetDateTime.parse("2026-07-01T10:00:00+05:30"), Direction.DEBIT,
                new BigDecimal("100.00"), Category.SPEND, "MERCH1", List.of("msg2"), "REF1" // Duplicate in SQL
        );
        NormalizedTxn t3 = new NormalizedTxn(
                "5678", OffsetDateTime.parse("2026-07-02T12:00:00+05:30"), Direction.CREDIT,
                new BigDecimal("5000.00"), Category.INCOME, "SALARY", List.of("msg3"), "REF2"
        );

        sqlStore.save(t1);
        sqlStore.save(t2);
        sqlStore.save(t3);

        // Run backfill
        Backfill backfill = new Backfill(sqlStore, dynamoStore);
        Backfill.Result result = backfill.run(1, TimeUnit.MINUTES);

        assertEquals(3, result.read(), "Should read 3 from SQL");
        assertEquals(2, result.written(), "Should write 2 unique to Dynamo");
        assertEquals(1, result.sourceDeduplicated(), "Should deduplicate 1");
        assertEquals(0, result.targetSkipped(), "Should skip 0 from target");
        assertEquals(0, result.failed());

        // Validate via ConsistencyChecker
        ConsistencyChecker checker = new ConsistencyChecker(sqlStore, dynamoStore);
        List<ConsistencyChecker.Divergence> divergences = checker.check();
        
        // Divergences will include 1 SQL_DUPLICATE_IDENTITY, because SQL has duplicates but DynamoDB shouldn't!
        // Wait, does ConsistencyChecker expect SQL duplicates to exist in DynamoDB?
        // Let's print out what we get and assert what is expected.
        long duplicateDivergences = divergences.stream()
            .filter(d -> d.what().startsWith("SQL_DUPLICATE_IDENTITY"))
            .count();
            
        assertEquals(1, duplicateDivergences, "Expect exactly 1 SQL_DUPLICATE_IDENTITY divergence");
        assertEquals(1, divergences.size(), "Should have exactly 1 divergence total (the duplicate in SQL)");
    }
}
