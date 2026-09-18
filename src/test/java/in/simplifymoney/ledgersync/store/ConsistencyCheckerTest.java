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

    @Test
    public void detectsCategoryTotalAndMessageIndexMismatch() throws Exception {
        Path dbFile = Files.createTempDirectory("checker").resolve("db");
        try (SqlLedgerStore sql = new SqlLedgerStore(dbFile)) {
            sql.migrate(Path.of(System.getProperty("user.dir"), "db", "migration"));
            NormalizedTxn t = txn("msg-index", "INDEX", "40.00");
            sql.save(t);

            FakeDocumentStore docs = new FakeDocumentStore(List.of(t));
            docs.overrideTotal("1234", Category.SPEND, new BigDecimal("41.00"));
            docs.overrideMessage("msg-index", txn("msg-other", "OTHER", "40.00"));

            List<ConsistencyChecker.Divergence> divergences = new ConsistencyChecker(sql, docs).check();

            assertTrue(divergences.stream().anyMatch(d -> d.what().contains("CATEGORY_TOTAL_MISMATCH")));
            assertTrue(divergences.stream().anyMatch(d -> d.what().contains("MESSAGE_INDEX_MISMATCH")));
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
        private final Map<String, Map<Category, BigDecimal>> totalOverrides = new java.util.HashMap<>();
        private final Map<String, NormalizedTxn> messageOverrides = new java.util.HashMap<>();

        private FakeDocumentStore(List<NormalizedTxn> txns) {
            this.txns = new ArrayList<>(txns);
        }

        void overrideTotal(String account, Category category, BigDecimal value) {
            totalOverrides.computeIfAbsent(account, ignored -> new java.util.HashMap<>()).put(category, value);
        }

        void overrideMessage(String sourceId, NormalizedTxn txn) {
            messageOverrides.put(sourceId, txn);
        }

        @Override public List<NormalizedTxn> forAccountMonth(String accountLast4, YearMonth month) { return List.of(); }
        @Override public Map<Category, BigDecimal> categoryTotals(String accountLast4) {
            if (totalOverrides.containsKey(accountLast4)) {
                return totalOverrides.get(accountLast4);
            }
            Map<Category, BigDecimal> totals = new java.util.EnumMap<>(Category.class);
            for (NormalizedTxn txn : txns) {
                if (txn.accountLast4().equals(accountLast4)) {
                    totals.merge(txn.category(), txn.amount(), BigDecimal::add);
                }
            }
            return totals;
        }
        @Override public Optional<NormalizedTxn> byMessageId(String messageId) {
            if (messageOverrides.containsKey(messageId)) {
                return Optional.of(messageOverrides.get(messageId));
            }
            return txns.stream()
                    .filter(txn -> txn.sourceMessageIds().contains(messageId))
                    .findFirst();
        }
        @Override public void save(NormalizedTxn txn) { txns.add(txn); }
        @Override public List<NormalizedTxn> scanAllTransactions() { return List.copyOf(txns); }
    }
}
