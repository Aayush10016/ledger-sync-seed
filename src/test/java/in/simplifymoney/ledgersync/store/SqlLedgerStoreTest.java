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
                "1234", OffsetDateTime.now(), Direction.DEBIT, new BigDecimal("100.00"),
                Category.SPEND, "AMAZON", List.of("msg-1")
        );

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    latch.await();
                    store.save(t);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    done.countDown();
                }
            });
        }

        // Release all threads simultaneously
        latch.countDown();
        done.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Only one record should exist (idempotent atomic merge)
        assertEquals(initialCount + 1, store.count());
    }
}
