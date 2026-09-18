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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    public void transferDetectionPairsMatchingAmountsAcrossAccounts() {
        // NormalizedTxn enforces scale=2, so amounts always have exactly 2 decimal places.
        // This test verifies that categorizeTransfers correctly identifies a cross-account
        // debit+credit pair of the same amount within the 5-minute window as TRANSFER.
        NormalizedTxn debit = new NormalizedTxn(
                "4821",
                OffsetDateTime.parse("2026-07-10T10:00:00+05:30"),
                in.simplifymoney.ledgersync.model.Direction.DEBIT,
                new java.math.BigDecimal("1000.00"),
                Category.SPEND, "IMPS/P2A/PARAG KAPOOR", java.util.List.of("tf-1"));
        NormalizedTxn credit = new NormalizedTxn(
                "3310",
                OffsetDateTime.parse("2026-07-10T10:02:00+05:30"),
                in.simplifymoney.ledgersync.model.Direction.CREDIT,
                new java.math.BigDecimal("1000.00"),
                Category.INCOME, "IMPS/P2A/PARAG KAPOOR", java.util.List.of("tf-2"));

        java.util.List<NormalizedTxn> txns = new java.util.ArrayList<>(java.util.List.of(debit, credit));
        IngestService.categorizeTransfers(txns);

        assertEquals(Category.TRANSFER, txns.get(0).category(),
                "Debit should become TRANSFER when paired with matching credit within 5 minutes");
        assertEquals(Category.TRANSFER, txns.get(1).category(),
                "Credit should become TRANSFER when paired with matching debit within 5 minutes");
    }

    @Test
    public void unrelatedTransactionsOutsideTimeWindowAreNotTransfers() {
        NormalizedTxn debit = new NormalizedTxn(
                "4821",
                OffsetDateTime.parse("2026-07-10T10:00:00+05:30"),
                in.simplifymoney.ledgersync.model.Direction.DEBIT,
                new java.math.BigDecimal("500.00"),
                Category.SPEND, "STORE", java.util.List.of("unrelated-1"));
        // Same amount, opposite direction, different account, but 10 minutes later (> 5 min window)
        NormalizedTxn credit = new NormalizedTxn(
                "3310",
                OffsetDateTime.parse("2026-07-10T10:10:00+05:30"),
                in.simplifymoney.ledgersync.model.Direction.CREDIT,
                new java.math.BigDecimal("500.00"),
                Category.INCOME, "IMPS/P2A/PARAG KAPOOR", java.util.List.of("unrelated-2"));

        java.util.List<NormalizedTxn> txns = new java.util.ArrayList<>(java.util.List.of(debit, credit));
        IngestService.categorizeTransfers(txns);

        assertNotEquals(Category.TRANSFER, txns.get(0).category(),
                "Debit should NOT become TRANSFER when time gap > 5 minutes");
        assertNotEquals(Category.TRANSFER, txns.get(1).category(),
                "Credit should NOT become TRANSFER when time gap > 5 minutes");
    }

    @Test
    public void weakSameAmountOppositeDirectionEvidenceIsNotTransfer() {
        NormalizedTxn debit = new NormalizedTxn(
                "4821",
                OffsetDateTime.parse("2026-07-10T10:00:00+05:30"),
                Direction.DEBIT,
                new java.math.BigDecimal("500.00"),
                Category.SPEND, "STORE", java.util.List.of("weak-1"));
        NormalizedTxn credit = new NormalizedTxn(
                "9075",
                OffsetDateTime.parse("2026-07-10T10:01:00+05:30"),
                Direction.CREDIT,
                new java.math.BigDecimal("500.00"),
                Category.INCOME, "CASHBACK", java.util.List.of("weak-2"));

        java.util.List<NormalizedTxn> txns = new java.util.ArrayList<>(java.util.List.of(debit, credit));
        IngestService.categorizeTransfers(txns);

        assertNotEquals(Category.TRANSFER, txns.get(0).category());
        assertNotEquals(Category.TRANSFER, txns.get(1).category());
    }

    @Test
    public void testSameTransactionWithTimezoneFormattingDifferencesMerges() throws IOException {
        Path tempDir = Files.createTempDirectory("corpus");
        Path corpus = tempDir.resolve("test.jsonl");

        // IST is UTC+5:30. 10:00:00+05:30 is 04:30:00Z
        String sms = "{\"message_id\":\"msg-tz-1\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00+05:30\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UBER.\"}";
        String email = "{\"message_id\":\"msg-tz-2\",\"channel\":\"email\",\"sender\":\"alerts@hdfcbank.net\",\"received_at\":\"2024-05-15T04:30:00Z\",\"device_id\":\"d1\",\"body\":\"Date: Wed, 15 May 2024 10:00:00 +0530\\nSubject: Transaction alert\\n\\nYour account ending 1234 has been debited with INR 50.00.\\nMerchant / Remarks: Uber Trip\"}";
        
        Files.writeString(corpus, sms + "\n" + email + "\n");

        InMemoryLedgerStore store = new InMemoryLedgerStore();
        IngestService service = new IngestService(new Parsers(), store);
        service.ingestFile(corpus);
        
        assertEquals(1, store.count());
    }

    @Test
    public void testTwoLegitimateTransactionsWithSimilarAttributesRemainSeparate() throws IOException {
        Path tempDir = Files.createTempDirectory("corpus");
        Path corpus = tempDir.resolve("test.jsonl");

        // Same amount, same account, same merchant, but 1 second apart (different transaction)
        String msg1 = "{\"message_id\":\"msg-sim-1\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UBER.\"}";
        String msg2 = "{\"message_id\":\"msg-sim-2\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:05Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:01 to UBER.\"}";
        
        Files.writeString(corpus, msg1 + "\n" + msg2 + "\n");

        InMemoryLedgerStore store = new InMemoryLedgerStore();
        IngestService service = new IngestService(new Parsers(), store);
        service.ingestFile(corpus);
        
        assertEquals(2, store.count());
    }

    @Test
    public void sameSecondSameMerchantDifferentMessagesRemainSeparateWhenIdentityIsUncertain() throws IOException {
        Path tempDir = Files.createTempDirectory("corpus");
        Path corpus = tempDir.resolve("same-second.jsonl");

        String msg1 = "{\"message_id\":\"msg-same-1\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UBER. Ref A1\"}";
        String msg2 = "{\"message_id\":\"msg-same-2\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UBER. Ref B2\"}";

        Files.writeString(corpus, msg1 + "\n" + msg2 + "\n");

        InMemoryLedgerStore store = new InMemoryLedgerStore();
        new IngestService(new Parsers(), store).ingestFile(corpus);

        assertEquals(2, store.count());
        assertEquals(java.util.Set.of("msg-same-1", "msg-same-2"),
                store.all().stream()
                        .flatMap(t -> t.sourceMessageIds().stream())
                        .collect(java.util.stream.Collectors.toSet()));
    }

    @Test
    public void selectedMerchantAndCategoryDoNotDependOnInputOrder() throws IOException {
        Path tempDir = Files.createTempDirectory("corpus");
        String sms = "{\"message_id\":\"sms-1\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00+05:30\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UPI/WATER CAN.\"}";
        String email = "{\"message_id\":\"email-1\",\"channel\":\"email\",\"sender\":\"alerts@hdfcbank.net\",\"received_at\":\"2024-05-15T10:00:05+05:30\",\"device_id\":\"d1\",\"body\":\"Date: Wed, 15 May 2024 10:00:00 +0530\\nSubject: Transaction alert\\n\\nYour account ending 1234 has been debited with INR 50.00.\\nMerchant / Remarks: Water Can\"}";

        Path smsFirst = tempDir.resolve("sms-first.jsonl");
        Path emailFirst = tempDir.resolve("email-first.jsonl");
        Files.writeString(smsFirst, sms + "\n" + email + "\n");
        Files.writeString(emailFirst, email + "\n" + sms + "\n");

        InMemoryLedgerStore smsFirstStore = new InMemoryLedgerStore();
        new IngestService(new Parsers(), smsFirstStore).ingestFile(smsFirst);
        InMemoryLedgerStore emailFirstStore = new InMemoryLedgerStore();
        new IngestService(new Parsers(), emailFirstStore).ingestFile(emailFirst);

        assertEquals(1, smsFirstStore.count());
        assertEquals(1, emailFirstStore.count());
        assertEquals("Water Can", smsFirstStore.all().get(0).merchant());
        assertEquals("Water Can", emailFirstStore.all().get(0).merchant());
        assertEquals(Category.MICRO, smsFirstStore.all().get(0).category());
        assertEquals(Category.MICRO, emailFirstStore.all().get(0).category());
    }

    @Test
    public void malformedRecordsAreAccountedForSeparately() throws IOException {
        Path tempDir = Files.createTempDirectory("corpus");
        Path corpus = tempDir.resolve("malformed.jsonl");

        String valid = "{\"message_id\":\"valid-1\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 debited from a/c **1234 on 15-05-24 at 10:00 to UBER.\"}";
        Files.writeString(corpus, valid + "\n{\"message_id\":\"bad\"\n");

        InMemoryLedgerStore store = new InMemoryLedgerStore();
        IngestService.Stats stats = new IngestService(new Parsers(), store).ingestFile(corpus);

        assertEquals(2, stats.messagesRead());
        assertEquals(1, stats.malformedRecords());
        assertEquals(2, stats.malformedDetails().get(0).lineNumber());
        assertEquals(1, store.count());
    }

    @Test
    public void unreliableBalanceAccountsAreConfigurable() throws IOException {
        Path tempDir = Files.createTempDirectory("corpus");
        Path corpus = tempDir.resolve("card.jsonl");

        String first = "{\"message_id\":\"card-1\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:00:00Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 spent on HDFC Bank Card x3310 at STORE on 15-05-24 10:00. Avl Limit: Rs.1000.00.\"}";
        String second = "{\"message_id\":\"card-2\",\"channel\":\"sms\",\"sender\":\"AD-HDFCBK-S\",\"received_at\":\"2024-05-15T10:10:00Z\",\"device_id\":\"d1\",\"body\":\"Rs.50.00 spent on HDFC Bank Card x3310 at STORE on 15-05-24 10:10. Avl Limit: Rs.800.00.\"}";
        Files.writeString(corpus, first + "\n" + second + "\n");

        InMemoryLedgerStore defaultStore = new InMemoryLedgerStore();
        new IngestService(new Parsers(), defaultStore).ingestFile(corpus);
        assertEquals(0, defaultStore.discrepancies().size());

        InMemoryLedgerStore configuredStore = new InMemoryLedgerStore();
        new IngestService(new Parsers(), configuredStore, java.util.Set.of()).ingestFile(corpus);
        assertEquals(1, configuredStore.discrepancies().size());
    }

}
