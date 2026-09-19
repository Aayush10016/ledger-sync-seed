package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import in.simplifymoney.ledgersync.ingest.IngestService;
import in.simplifymoney.ledgersync.parse.Parsers;
import org.junit.jupiter.api.Assumptions;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LifecycleIntegrationTest {

    private DynamoDbClient client;
    private DynamoDbLedgerStore store;

    private static DynamoDbClient newClient() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .region(Region.AP_SOUTH_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummy", "dummy")))
                .build();
    }

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
            client.waiter().waitUntilTableNotExists(b -> b.tableName("LedgerStore"));
        } catch (ResourceNotFoundException e) {
            // ignore
        }

        store = new DynamoDbLedgerStore(client);
        client.waiter().waitUntilTableExists(b -> b.tableName("LedgerStore"));
    }

    @Test
    public void testFullLifecycleWithRealSql() throws Exception {
        NormalizedTxn original = new NormalizedTxn(
                "9999", OffsetDateTime.parse("2024-06-01T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("75.00"),
                Category.SPEND, "SWIGGY", List.of("lifecycle-1"));

        // Phase 1: Real SQL Store in temporary directory
        Path tempDir = Files.createTempDirectory("lifecycle-sql-test");
        SqlLedgerStore sqlStore = new SqlLedgerStore(tempDir.resolve("db"));
        sqlStore.migrate(schemaOnlyMigrations());
        sqlStore.save(original);
        
        // Assert SQL store actually has it
        assertEquals(1, sqlStore.all().size());

        // Phase 2: Backfill SQL -> DynamoDB
        Backfill backfill = new Backfill(sqlStore, store);
        Backfill.Result result = backfill.run(1, java.util.concurrent.TimeUnit.MINUTES);
        assertTrue(result.status() == Backfill.Status.COMPLETED && result.failed() == 0,
                "Backfill must complete with no failures");
        assertEquals(1, store.scanAllTransactions().size());
        assertTrue(store.byMessageId("lifecycle-1").isPresent());

        // Phase 3: Re-ingest the exact same transaction to DynamoDB (idempotency check)
        store.save(original);
        assertEquals(1, store.scanAllTransactions().size(), "Must not duplicate in DynamoDB");

        // Phase 4: Verify source ID mapping remains intact
        NormalizedTxn byId = store.byMessageId("lifecycle-1").orElseThrow();
        assertEquals(List.of("lifecycle-1"), byId.sourceMessageIds());
        assertEquals(new BigDecimal("75.00"), byId.amount());
        
        sqlStore.close();
    }

    @Test
    public void testCorpusBackfillEquivalence() throws Exception {
        Path tempDir = Files.createTempDirectory("lifecycle-corpus-test");
        SqlLedgerStore sqlStore = new SqlLedgerStore(tempDir.resolve("db"));
        sqlStore.migrate(Path.of(System.getProperty("user.dir"), "db", "migration"));

        in.simplifymoney.ledgersync.ingest.IngestService ingest = 
            new in.simplifymoney.ledgersync.ingest.IngestService(new Parsers(), sqlStore);
        ingest.ingestFile(Path.of("fixtures/corpus-a.jsonl"));

        List<NormalizedTxn> sqlTxns = sqlStore.all();
        assertTrue(sqlTxns.size() > 0, "SQL store must have ingested transactions");

        Backfill backfill = new Backfill(sqlStore, store);
        Backfill.Result result = backfill.run(1, java.util.concurrent.TimeUnit.MINUTES);
        assertEquals(Backfill.Status.COMPLETED, result.status());
        assertEquals(0, result.failed());

        List<NormalizedTxn> dynamoTxns = store.scanAllTransactions();
        assertEquals(sqlTxns.size(), dynamoTxns.size(), 
            "Exactly equal ledger counts between SQL canon and DynamoDB store");

        java.util.Map<Category, BigDecimal> sqlCats = 
            in.simplifymoney.ledgersync.report.Reports.byCategory(sqlTxns);
        java.util.Map<Category, BigDecimal> dynamoCats = 
            in.simplifymoney.ledgersync.report.Reports.byCategory(dynamoTxns);

        assertEquals(sqlCats, dynamoCats, "Category totals must match exactly between SQL and DynamoDB");
        
        java.util.Set<String> seenMessageIds = new java.util.HashSet<>();
        for (NormalizedTxn txn : dynamoTxns) {
            for (String msgId : txn.sourceMessageIds()) {
                assertTrue(seenMessageIds.add(msgId), "Source message ID " + msgId + " mapped to multiple transactions!");
            }
        }
    }

    private static Path schemaOnlyMigrations() throws Exception {
        Path source = Path.of(System.getProperty("user.dir"), "db", "migration");
        Path target = Files.createTempDirectory("schema-only-migrations");
        try (var files = Files.list(source)) {
            for (Path file : files.filter(path -> path.getFileName().toString().endsWith(".sql")).toList()) {
                if ("V2__seed.sql".equals(file.getFileName().toString())) {
                    continue;
                }
                Files.copy(file, target.resolve(file.getFileName()), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
        }
        return target;
    }
}
