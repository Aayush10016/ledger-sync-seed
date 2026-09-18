package in.simplifymoney.ledgersync.ingest;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TransferHeuristicTest {

    @Test
    public void testAmbiguousTransfersResolveSafely() {
        // Scenario: Multiple identical transfers occur within the 5-minute window.
        // We must ensure the heuristic pairs them exactly 1:1 and doesn't re-use a counterpart.
        
        OffsetDateTime t1 = OffsetDateTime.parse("2026-07-04T10:00:00Z");
        
        NormalizedTxn debit1 = new NormalizedTxn("1111", t1, Direction.DEBIT, new BigDecimal("500.00"), Category.SPEND, "IMPS/P2A/PARAG KAPOOR", List.of("m1"));
        NormalizedTxn credit1 = new NormalizedTxn("2222", t1.plusSeconds(30), Direction.CREDIT, new BigDecimal("500.00"), Category.INCOME, "IMPS/P2A/PARAG KAPOOR", List.of("m2"));
        
        NormalizedTxn debit2 = new NormalizedTxn("1111", t1.plusSeconds(60), Direction.DEBIT, new BigDecimal("500.00"), Category.SPEND, "IMPS/P2A/PARAG KAPOOR", List.of("m3"));
        NormalizedTxn credit2 = new NormalizedTxn("2222", t1.plusSeconds(90), Direction.CREDIT, new BigDecimal("500.00"), Category.INCOME, "IMPS/P2A/PARAG KAPOOR", List.of("m4"));

        // Add a third debit with NO matching credit
        NormalizedTxn debit3 = new NormalizedTxn("1111", t1.plusSeconds(120), Direction.DEBIT, new BigDecimal("500.00"), Category.SPEND, "IMPS/P2A/PARAG KAPOOR", List.of("m5"));

        List<NormalizedTxn> txns = new java.util.ArrayList<>(List.of(debit1, credit1, debit2, credit2, debit3));
        
        IngestService.categorizeTransfers(txns);

        assertEquals(Category.TRANSFER, txns.get(0).category()); // debit1
        assertEquals(Category.TRANSFER, txns.get(1).category()); // credit1
        assertEquals(Category.TRANSFER, txns.get(2).category()); // debit2
        assertEquals(Category.TRANSFER, txns.get(3).category()); // credit2
        assertEquals(Category.SPEND, txns.get(4).category());    // debit3 remains SPEND
    }

    @Test
    public void testSequentialPurchasesNotMerged() {
        OffsetDateTime t1 = OffsetDateTime.parse("2026-07-04T10:00:00Z");
        
        NormalizedTxn debit1 = new NormalizedTxn("1111", t1, Direction.DEBIT, new BigDecimal("500.00"), Category.SPEND, "CAFE COFFEE DAY", List.of("m1"));
        NormalizedTxn credit1 = new NormalizedTxn("2222", t1.plusSeconds(30), Direction.CREDIT, new BigDecimal("500.00"), Category.INCOME, "CAFE COFFEE DAY", List.of("m2"));
        
        List<NormalizedTxn> txns = new java.util.ArrayList<>(List.of(debit1, credit1));
        
        IngestService.categorizeTransfers(txns);

        assertEquals(Category.SPEND, txns.get(0).category()); // debit1 remains SPEND
        assertEquals(Category.INCOME, txns.get(1).category()); // credit1 remains INCOME
    }
}
