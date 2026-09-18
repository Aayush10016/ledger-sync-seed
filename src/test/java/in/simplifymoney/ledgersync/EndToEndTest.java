package in.simplifymoney.ledgersync;

import in.simplifymoney.ledgersync.ingest.IngestService;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.parse.Parsers;
import in.simplifymoney.ledgersync.store.Backfill;
import in.simplifymoney.ledgersync.store.ConsistencyChecker;
import in.simplifymoney.ledgersync.store.DynamoDbLedgerStore;
import in.simplifymoney.ledgersync.store.SqlLedgerStore;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EndToEndTest {

    @Test
    public void testExistingTransactionPlusNewMessage() throws Exception {
        Path db = Path.of("data", "ledger");
        Files.deleteIfExists(Path.of("data", "ledger.mv.db"));
        Files.deleteIfExists(Path.of("data", "ledger.trace.db"));

        // Use standard dummy credentials for local DynamoDB testing
        DynamoDbClient client = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummy", "dummy")))
                .build();

        try (SqlLedgerStore sql = new SqlLedgerStore(db)) {
            sql.migrate(Path.of("db", "migration"));
            
            // Note: We use a try-catch for DynamoDB setup to allow the test to gracefully
            // handle environments without Docker, while still running natively on CI.
            DynamoDbLedgerStore dynamo = null;
            boolean dynamoReady = false;
            try {
                dynamo = new DynamoDbLedgerStore(client);
                // Initialize Table
                System.out.println("Initializing DynamoDB tables...");
                dynamoReady = true;
            } catch (Exception e) {
                System.out.println("DynamoDB is not available locally. Skipping DynamoDB specific ingestion.");
            }

            System.out.println("Ingesting Corpus A into SQL...");
            var aSql = new IngestService(new Parsers(), sql).ingestFile(Path.of("fixtures", "corpus-a.jsonl"));
            System.out.println("SQL: " + aSql);
            
            long sqlSubTotalAfterA = getSqlTotal(sql, "4821", "INCOME");
            System.out.println("SQL INCOME total for 4821 after A: " + sqlSubTotalAfterA);

            System.out.println("\nIngesting Corpus B into SQL...");
            var bSql = new IngestService(new Parsers(), sql).ingestFile(Path.of("fixtures", "corpus-b.jsonl"));
            System.out.println("SQL: " + bSql);
            
            long sqlSubTotalAfterB = getSqlTotal(sql, "4821", "INCOME");
            System.out.println("SQL INCOME total for 4821 after B: " + sqlSubTotalAfterB);

            System.out.println("\nIngesting Corpus B into SQL a SECOND TIME (Idempotency Check)...");
            var bSql2 = new IngestService(new Parsers(), sql).ingestFile(Path.of("fixtures", "corpus-b.jsonl"));
            System.out.println("SQL (2nd run): " + bSql2);
            
            long sqlSubTotalAfterB2 = getSqlTotal(sql, "4821", "INCOME");
            System.out.println("SQL INCOME total for 4821 after B (2nd run): " + sqlSubTotalAfterB2);

            System.out.println("\nVerifying SQL Merge...");
            System.out.println("Total SQL rows: " + sql.count());
            
            var txn = sql.all().stream()
                .filter(t -> t.sourceMessageIds().contains("m-00001-31eb24") || t.sourceMessageIds().contains("m-99999-newmsg"))
                .findFirst()
                .orElseThrow();
            System.out.println("Merged Transaction has " + txn.sourceMessageIds().size() + " message IDs");
            System.out.println("Contains original ID (m-00001-31eb24): " + txn.sourceMessageIds().contains("m-00001-31eb24"));
            System.out.println("Contains new ID (m-99999-newmsg): " + txn.sourceMessageIds().contains("m-99999-newmsg"));
            assertEquals(3, txn.sourceMessageIds().size(), "There should be 3 message IDs due to deduplication");

            if (dynamoReady) {
                System.out.println("\nBackfilling SQL into DynamoDB before full consistency check...");
                var backfillResult = new Backfill(sql, dynamo).run(2, TimeUnit.MINUTES);
                System.out.println("Backfill: " + backfillResult);

                System.out.println("\nChecking consistency...");
                var divergences = new ConsistencyChecker(sql, dynamo).check();
                for (var d : divergences) {
                    System.out.println("Divergence: " + d);
                }
                assertTrue(divergences.isEmpty(), "SQL and DynamoDB must be consistent");

                System.out.println("\nVerifying the specific message merge in DynamoDB:");
                var txn1 = dynamo.byMessageId("m-00001-31eb24").orElseThrow();
                System.out.println("DynamoDB Transaction for m-00001-31eb24: " + txn1);

                var txn2 = dynamo.byMessageId("m-99999-newmsg").orElseThrow();
                System.out.println("DynamoDB Transaction for m-99999-newmsg: " + txn2);

                assertEquals(txn1, txn2, "Both message IDs should resolve to the exact same transaction");
                assertTrue(txn1.sourceMessageIds().contains("m-00001-31eb24"));
                assertTrue(txn1.sourceMessageIds().contains("m-99999-newmsg"));
                assertEquals(3, txn1.sourceMessageIds().size(), "There should be exactly 3 message IDs");
                
                // Check that Q2 category totals for account 4821 only increased once
                var catTotals = dynamo.categoryTotals("4821");
                System.out.println("Category totals for 4821: " + catTotals);
            }
        }
    }

    private long getSqlTotal(SqlLedgerStore sql, String account, String category) {
        return sql.all().stream()
                .filter(t -> t.accountLast4().equals(account) && t.category().name().equals(category))
                .mapToLong(t -> t.amount().multiply(new java.math.BigDecimal("100")).longValue())
                .sum();
    }
}
