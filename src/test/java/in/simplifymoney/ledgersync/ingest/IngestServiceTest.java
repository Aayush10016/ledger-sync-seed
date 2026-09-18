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

    @Test
    public void testDuplicateSmsIsIdempotent() throws IOException {
        Path tempDir = Files.createTempDirectory("corpus");
        Path corpus = tempDir.resolve("test.jsonl");

        String msg1 = "{\"message_id\":\"msg-1\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UBER.\"}";
        String msg2 = "{\"message_id\":\"msg-2\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:10Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UBER.\"}";
        
        Files.writeString(corpus, msg1 + "\n" + msg2 + "\n");

        InMemoryLedgerStore store = new InMemoryLedgerStore();
        IngestService service = new IngestService(new Parsers(), store);
        
        service.ingestFile(corpus);
        
        // Ensure they are merged into one transaction
        assertEquals(1, store.count());
        assertEquals(2, store.all().get(0).sourceMessageIds().size());
    }

    @Test
    public void testSmsAndEmailForSameTransactionMergeDespiteMerchantDifferences() throws IOException {
        Path tempDir = Files.createTempDirectory("corpus");
        Path corpus = tempDir.resolve("test.jsonl");

        String sms = "{\"message_id\":\"sms-1\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00+05:30\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UPI/UBER INDIA.\"}";
        String email = "{\"message_id\":\"email-1\",\"channel\":\"email\",\"sender\":\"alerts@hdfcbank.net\",\"received_at\":\"2024-05-15T10:00:05+05:30\",\"device_id\":\"d1\",\"body\":\"Date: Wed, 15 May 2024 10:00:00 +0530\\nSubject: Transaction alert\\n\\nYour account ending 1234 has been debited with INR 50.00.\\nMerchant / Remarks: Uber Trip\"}";

        Files.writeString(corpus, sms + "\n" + email + "\n");

        InMemoryLedgerStore store = new InMemoryLedgerStore();
        IngestService service = new IngestService(new Parsers(), store);

        service.ingestFile(corpus);

        assertEquals(1, store.count());
        assertEquals(java.util.List.of("sms-1", "email-1"), store.all().get(0).sourceMessageIds());
    }


    @Test
    public void testDifferentEmailBodiesNotMerged() throws IOException {
        Path tempDir = Files.createTempDirectory("corpus");
        Path corpus = tempDir.resolve("test.jsonl");

        String msg1 = "{\"message_id\":\"em-1\",\"channel\":\"email\",\"sender\":\"alerts@hdfcbank.net\",\"received_at\":\"2024-05-15T10:00:00Z\",\"device_id\":\"d1\",\"body\":\"Date: Wed, 15 May 2024 10:00:00 +0530\\nSubject: Transaction alert\\n\\nYour account ending 1234 has been debited with INR 50.00 on 15-05-24 at 10:00. Info: AMAZON PAY.\"}";
        String msg2 = "{\"message_id\":\"em-2\",\"channel\":\"email\",\"sender\":\"alerts@hdfcbank.net\",\"received_at\":\"2024-05-15T10:00:05Z\",\"device_id\":\"d1\",\"body\":\"Date: Wed, 15 May 2024 10:00:00 +0530\\nSubject: Transaction alert\\n\\nYour account ending 1234 has been debited with INR 50.00 on 15-05-24 at 10:00. Info: FLIPKART.\"}";
        
        Files.writeString(corpus, msg1 + "\n" + msg2 + "\n");

        InMemoryLedgerStore store = new InMemoryLedgerStore();
        IngestService service = new IngestService(new Parsers(), store);
        
        service.ingestFile(corpus);
        
        // Ensure they are saved as distinct transactions because bodies differ
        assertEquals(2, store.count());
    }

    @Test
    public void testReingestionIdempotency() throws IOException {
        Path tempDir = Files.createTempDirectory("corpus");
        Path corpus = tempDir.resolve("test.jsonl");

        String msg1 = "{\"message_id\":\"msg-1\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UBER.\"}";
        Files.writeString(corpus, msg1 + "\n");

        InMemoryLedgerStore store = new InMemoryLedgerStore();
        IngestService service = new IngestService(new Parsers(), store);
        
        // Run once
        service.ingestFile(corpus);
        assertEquals(1, store.count());
        
        // Run again
        service.ingestFile(corpus);
        // Count should still be 1, because TxnIdentity logic deduplicates existing vs incoming
        assertEquals(1, store.count());
    }

    @Test
    public void testReconciliationRecordIsDeterministic() throws IOException {
        // Need to run a corpus that requires a reconciliation discrepancy.
        // We will fake a small dataset that has a balance discrepancy.
        Path tempDir = Files.createTempDirectory("corpus");
        Path corpus = tempDir.resolve("test.jsonl");

        // Two messages that cause a gap of 100.00
        // msg1: balance 1000
        String msg1 = "{\"message_id\":\"msg-1\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UBER. Avl Bal: Rs.1000.00\"}";
        // msg2: balance 850 (amount is 50, so previous balance was 900. Gap = 1000 - 900 = 100)
        String msg2 = "{\"message_id\":\"msg-2\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:05:00Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:05 to ZOMATO. Avl Bal: Rs.850.00\"}";
        
        Files.writeString(corpus, msg1 + "\n" + msg2 + "\n");

        InMemoryLedgerStore store = new InMemoryLedgerStore();
        IngestService service = new IngestService(new Parsers(), store);
        
        service.ingestFile(corpus);
        
        // Should have only the two observed transactions. The balance gap is
        // represented as reconciliation data, not a synthetic ledger row.
        assertEquals(2, store.count());
        assertEquals(1, store.discrepancies().size());
        
        // Run again
        service.ingestFile(corpus);
        
        // Count should STILL be 2 and reconciliation should remain idempotent.
        assertEquals(2, store.count());
        assertEquals(1, store.discrepancies().size());
    }
}
