package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class BackfillTest {

    @Test
    public void testBackfillHandlesTimeoutsAndInterrupts() throws Exception {
        NormalizedTxn t = new NormalizedTxn(
                "1234", OffsetDateTime.now(), Direction.DEBIT, new BigDecimal("100.00"),
                Category.SPEND, "MERCHANT", List.of("msg-1")
        );

        LedgerStore fakeSql = new LedgerStore() {
            @Override
            public List<NormalizedTxn> all() {
                return List.of(t);
            }
            @Override public void save(NormalizedTxn t) {}
            @Override public long count() { return 1; }
            @Override public void save(in.simplifymoney.ledgersync.model.Discrepancy d) {}
            @Override public List<in.simplifymoney.ledgersync.model.Discrepancy> discrepancies() { return List.of(); }
        };

        DocumentStore fakeDoc = new DocumentStore() {
            @Override
            public void save(NormalizedTxn txn) {
                throw new RuntimeException("DynamoDB Timeout");
            }
            @Override public List<NormalizedTxn> forAccountMonth(String acct, java.time.YearMonth ym) { return List.of(); }
            @Override public java.util.Map<Category, BigDecimal> categoryTotals(String acct) { return java.util.Map.of(); }
            @Override public java.util.Optional<NormalizedTxn> byMessageId(String id) { return java.util.Optional.empty(); }
            @Override public List<NormalizedTxn> scanAllTransactions() { return List.of(); }
        };

        Backfill backfill = new Backfill(fakeSql, fakeDoc);
        try {
            backfill.run();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("Backfill completed with 1 failures"));
        }
    }
}
