package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsistencyCheckerTest {

    @Test
    public void detectsSqlOnlyDocumentOnlyAndFieldMismatchFromCompleteSnapshots() throws Exception {
        Path dbFile = Files.createTempDirectory("checker").resolve("db");
        try (SqlLedgerStore sql = new SqlLedgerStore(dbFile)) {
            sql.migrate(Path.of(System.getProperty("user.dir"), "db", "migration"));
            NormalizedTxn matchingSql = txn("msg-match", "MATCH", "30.00");
            NormalizedTxn sqlOnly = txn("msg-sql-only", "SQL_ONLY", "31.00");
            sql.save(matchingSql);
            sql.save(sqlOnly);

            FakeDocumentStore docs = new FakeDocumentStore(List.of(
                    new NormalizedTxn(
                            matchingSql.accountLast4(), matchingSql.occurredAt(), matchingSql.direction(),
                            new BigDecimal("30.00"), matchingSql.category(), "MISMATCHED",
                            matchingSql.sourceMessageIds()),
                    txn("msg-doc-only", "DOC_ONLY", "32.00")
            ));

            List<ConsistencyChecker.Divergence> divergences = new ConsistencyChecker(sql, docs).check();

            assertTrue(divergences.stream().anyMatch(d -> d.what().contains("SQL_ONLY")));
            assertTrue(divergences.stream().anyMatch(d -> d.what().contains("DOCUMENT_ONLY")));
            assertTrue(divergences.stream().anyMatch(d -> d.what().contains("field=merchant")));
        }
    }

    @Test
    public void detectsDuplicateDocumentIdentity() throws Exception {
        Path dbFile = Files.createTempDirectory("checker").resolve("db");
        try (SqlLedgerStore sql = new SqlLedgerStore(dbFile)) {
            sql.migrate(Path.of(System.getProperty("user.dir"), "db", "migration"));
            NormalizedTxn t = txn("msg-duplicate", "DUP", "40.00");
            sql.save(t);

            FakeDocumentStore docs = new FakeDocumentStore(List.of(t, t));
            List<ConsistencyChecker.Divergence> divergences = new ConsistencyChecker(sql, docs).check();

            assertTrue(divergences.stream().anyMatch(d -> d.what().contains("Document_DUPLICATE_IDENTITY")));
        }
    }

    private static NormalizedTxn txn(String messageId, String merchant, String amount) {
        return new NormalizedTxn(
                "1234",
                OffsetDateTime.parse("2026-07-01T10:00:00+05:30"),
                Direction.DEBIT,
                new BigDecimal(amount),
                Category.SPEND,
                merchant,
                List.of(messageId));
    }

    private static final class FakeDocumentStore implements DocumentStore {
        private final List<NormalizedTxn> txns;

        private FakeDocumentStore(List<NormalizedTxn> txns) {
            this.txns = new ArrayList<>(txns);
        }

        @Override public List<NormalizedTxn> forAccountMonth(String accountLast4, YearMonth month) { return List.of(); }
        @Override public Map<Category, BigDecimal> categoryTotals(String accountLast4) { return Map.of(); }
        @Override public Optional<NormalizedTxn> byMessageId(String messageId) { return Optional.empty(); }
        @Override public void save(NormalizedTxn txn) { txns.add(txn); }
        @Override public List<NormalizedTxn> scanAllTransactions() { return List.copyOf(txns); }
    }
}
