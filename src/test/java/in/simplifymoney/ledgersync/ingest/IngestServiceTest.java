package in.simplifymoney.ledgersync.ingest;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.parse.Parsers;
import in.simplifymoney.ledgersync.store.InMemoryLedgerStore;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IngestServiceTest {

    @Test
    public void testDistinctMerchantsAreNotMerged() throws IOException {
        Path tempDir = Files.createTempDirectory("corpus");
        Path corpus = tempDir.resolve("test.jsonl");

        // Two transactions, identical amount and time, different merchants
        String msg1 = "{\"message_id\":\"msg-1\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UBER.\"}";
        String msg2 = "{\"message_id\":\"msg-2\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to WATER CAN.\"}";
        
        Files.writeString(corpus, msg1 + "\n" + msg2 + "\n");

        InMemoryLedgerStore store = new InMemoryLedgerStore();
        IngestService service = new IngestService(new Parsers(), store);
        
        service.ingestFile(corpus);
        
        // Ensure both are saved, not merged
        assertEquals(2, store.count());
    }
}
