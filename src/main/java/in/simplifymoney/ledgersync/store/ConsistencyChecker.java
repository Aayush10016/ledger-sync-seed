package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.util.TxnIdentity;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Proves the two stores agree, and says precisely where they do not.
 */
public final class ConsistencyChecker {

    private final SqlLedgerStore sql;
    private final DocumentStore documents;

    public ConsistencyChecker(SqlLedgerStore sql, DocumentStore documents) {
        this.sql = sql;
        this.documents = documents;
    }

    public List<Divergence> check() {
        System.out.println("Starting complete bidirectional consistency check...");
        List<Divergence> out = new ArrayList<>();

        List<NormalizedTxn> sqlTxns = sql.all();
        List<NormalizedTxn> docTxns = documents.scanAllTransactions();
        Snapshot sqlSnapshot = Snapshot.from(sqlTxns, "SQL");
        Snapshot docSnapshot = Snapshot.from(docTxns, "Document");

        out.addAll(sqlSnapshot.divergences);
        out.addAll(docSnapshot.divergences);

        TreeSet<String> identities = new TreeSet<>();
        identities.addAll(sqlSnapshot.byIdentity.keySet());
        identities.addAll(docSnapshot.byIdentity.keySet());

        for (String identity : identities) {
            List<NormalizedTxn> sqlRows = sqlSnapshot.byIdentity.get(identity);
            List<NormalizedTxn> docRows = docSnapshot.byIdentity.get(identity);
            if (sqlRows == null) {
                out.add(new Divergence("DOCUMENT_ONLY identity=" + identity, "null", docRows.get(0).toString()));
                continue;
            }
            if (docRows == null) {
                out.add(new Divergence("SQL_ONLY identity=" + identity, sqlRows.get(0).toString(), "null"));
                continue;
            }
            if (sqlRows.size() == 1 && docRows.size() == 1) {
                compareFields(identity, sqlRows.get(0), docRows.get(0), out);
            }
        }

        compareCategoryTotals(sqlTxns, docTxns, out);
        compareMessageIndexes(sqlSnapshot, docSnapshot, out);

        out.sort(java.util.Comparator
                .comparing(Divergence::what)
                .thenComparing(Divergence::inSql)
                .thenComparing(Divergence::inDocuments));

        System.out.println("==========================================");
        System.out.println("Consistency Check Summary:");
        System.out.println("- Checks performed: complete SQL snapshot, complete document snapshot, identity map comparison, source-message collision detection, field comparison");
        if (out.isEmpty()) {
            System.out.println("- Completeness Status: COMPLETE");
        } else {
            System.out.println("- Completeness Status: FAILED. Found " + out.size() + " divergences.");
        }
        System.out.println("- SQL records inspected: " + sqlTxns.size());
        System.out.println("- Document records scanned: " + docTxns.size());
        System.out.println("- Divergences found: " + out.size());
        System.out.println("==========================================");

        return out;
    }

    private void compareCategoryTotals(List<NormalizedTxn> sqlTxns, List<NormalizedTxn> docTxns, List<Divergence> out) {
        TreeSet<String> accounts = new TreeSet<>();
        sqlTxns.forEach(t -> accounts.add(t.accountLast4()));
        docTxns.forEach(t -> accounts.add(t.accountLast4()));

        Map<String, Map<Category, BigDecimal>> expected = totalsByAccount(sqlTxns);
        for (String account : accounts) {
            Map<Category, BigDecimal> docTotals = documents.categoryTotals(account);
            for (Category category : Category.values()) {
                BigDecimal sqlValue = expected
                        .getOrDefault(account, Map.of())
                        .getOrDefault(category, BigDecimal.ZERO.setScale(2));
                BigDecimal docValue = docTotals.getOrDefault(category, BigDecimal.ZERO.setScale(2));
                if (sqlValue.compareTo(docValue) != 0) {
                    out.add(new Divergence("CATEGORY_TOTAL_MISMATCH account=" + account + " category=" + category,
                            sqlValue.toPlainString(), docValue.toPlainString()));
                }
            }
        }
    }

    private static Map<String, Map<Category, BigDecimal>> totalsByAccount(List<NormalizedTxn> txns) {
        Map<String, Map<Category, BigDecimal>> totals = new TreeMap<>();
        for (NormalizedTxn txn : txns) {
            totals.computeIfAbsent(txn.accountLast4(), ignored -> new TreeMap<>())
                    .merge(txn.category(), txn.amount(), BigDecimal::add);
        }
        return totals;
    }

    private void compareMessageIndexes(Snapshot sqlSnapshot, Snapshot docSnapshot, List<Divergence> out) {
        TreeSet<String> sourceIds = new TreeSet<>();
        sourceIds.addAll(sqlSnapshot.sourceToIdentity.keySet());
        sourceIds.addAll(docSnapshot.sourceToIdentity.keySet());

        for (String sourceId : sourceIds) {
            String expectedIdentity = sqlSnapshot.sourceToIdentity.get(sourceId);
            java.util.Optional<NormalizedTxn> byMessage = documents.byMessageId(sourceId);
            if (expectedIdentity == null) {
                if (byMessage.isPresent()) {
                    out.add(new Divergence("MESSAGE_INDEX_EXTRA source=" + sourceId,
                            "null", TxnIdentity.getId(byMessage.get())));
                }
                continue;
            }
            if (byMessage.isEmpty()) {
                out.add(new Divergence("MESSAGE_INDEX_MISSING source=" + sourceId,
                        expectedIdentity, "null"));
                continue;
            }
            String actualIdentity = TxnIdentity.getId(byMessage.get());
            if (!expectedIdentity.equals(actualIdentity)) {
                out.add(new Divergence("MESSAGE_INDEX_MISMATCH source=" + sourceId,
                        expectedIdentity, actualIdentity));
            }
        }
    }

    private static void compareFields(String identity, NormalizedTxn sql, NormalizedTxn doc, List<Divergence> out) {
        compare(identity, "accountLast4", sql.accountLast4(), doc.accountLast4(), out);
        compare(identity, "occurredAt", sql.occurredAt().toString(), doc.occurredAt().toString(), out);
        compare(identity, "direction", sql.direction().name(), doc.direction().name(), out);
        if (sql.amount().compareTo(doc.amount()) != 0) {
            out.add(new Divergence("FIELD_MISMATCH identity=" + identity + " field=amount",
                    sql.amount().toPlainString(), doc.amount().toPlainString()));
        }
        compare(identity, "category", sql.category().name(), doc.category().name(), out);
        compare(identity, "merchant", sql.merchant(), doc.merchant(), out);
        compare(identity, "sourceMessageIds", String.join(",", sql.sourceMessageIds()),
                String.join(",", doc.sourceMessageIds()), out);
    }

    private static void compare(String identity, String field, String sql, String doc, List<Divergence> out) {
        if (!java.util.Objects.equals(sql, doc)) {
            out.add(new Divergence("FIELD_MISMATCH identity=" + identity + " field=" + field, sql, doc));
        }
    }

    private static final class Snapshot {
        private final Map<String, List<NormalizedTxn>> byIdentity;
        private final Map<String, String> sourceToIdentity;
        private final List<Divergence> divergences;

        private Snapshot(Map<String, List<NormalizedTxn>> byIdentity,
                         Map<String, String> sourceToIdentity,
                         List<Divergence> divergences) {
            this.byIdentity = byIdentity;
            this.sourceToIdentity = sourceToIdentity;
            this.divergences = divergences;
        }

        private static Snapshot from(List<NormalizedTxn> txns, String side) {
            Map<String, List<NormalizedTxn>> byIdentity = new TreeMap<>();
            Map<String, String> sourceToIdentity = new LinkedHashMap<>();
            List<Divergence> divergences = new ArrayList<>();

            for (NormalizedTxn txn : txns) {
                String identity = TxnIdentity.getId(txn);
                byIdentity.computeIfAbsent(identity, ignored -> new ArrayList<>()).add(txn);

                for (String sourceId : txn.sourceMessageIds()) {
                    String previous = sourceToIdentity.putIfAbsent(sourceId, identity);
                    if (previous != null && !previous.equals(identity)) {
                        divergences.add(new Divergence(side + "_SOURCE_COLLISION source=" + sourceId,
                                previous, identity));
                    }
                }
            }

            for (Map.Entry<String, List<NormalizedTxn>> entry : byIdentity.entrySet()) {
                if (entry.getValue().size() > 1) {
                    divergences.add(new Divergence(side + "_DUPLICATE_IDENTITY identity=" + entry.getKey(),
                            String.valueOf(entry.getValue().size()), entry.getValue().toString()));
                }
            }

            return new Snapshot(byIdentity, sourceToIdentity, divergences);
        }
    }

    /** One place the two stores disagree. */
    public record Divergence(String what, String inSql, String inDocuments) {}
}
