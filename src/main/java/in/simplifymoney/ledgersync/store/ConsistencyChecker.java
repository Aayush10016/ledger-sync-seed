package in.simplifymoney.ledgersync.store;

import java.util.List;

/**
 * Proves the two stores agree, and says precisely where they do not.
 *
 * NOT IMPLEMENTED - this is yours.
 *
 * We will run your checker against a document store we have deliberately
 * altered. It has to find what we changed and name it. A checker that only
 * compares row counts will not.
 */
public final class ConsistencyChecker {

    private final SqlLedgerStore sql;
    private final DocumentStore documents;

    public ConsistencyChecker(SqlLedgerStore sql, DocumentStore documents) {
        this.sql = sql;
        this.documents = documents;
    }

    public List<Divergence> check() {
        System.out.println("Starting Consistency Check...");
        List<Divergence> out = new java.util.ArrayList<>();
        
        java.util.Map<String, java.util.Set<java.time.YearMonth>> accountMonths = new java.util.HashMap<>();
        java.util.Set<String> allAccounts = new java.util.HashSet<>();
        java.util.Set<String> allMsgIds = new java.util.HashSet<>();
        
        java.util.Map<String, java.util.Map<in.simplifymoney.ledgersync.model.Category, java.math.BigDecimal>> sqlTotals = new java.util.HashMap<>();

        // 1. Gather all data from SQL
        java.util.List<in.simplifymoney.ledgersync.model.NormalizedTxn> allSqlTxns = sql.all();
        // SQL store is dirty, we need to deduplicate it first exactly as backfill does
        java.util.Set<String> seen = new java.util.HashSet<>();
        java.util.List<in.simplifymoney.ledgersync.model.NormalizedTxn> cleanSqlTxns = new java.util.ArrayList<>();
        for (in.simplifymoney.ledgersync.model.NormalizedTxn txn : allSqlTxns) {
            String deduplicationKey = txn.accountLast4() + "|" + txn.occurredAt() + "|" + txn.direction() + "|" + txn.amount();
            if (seen.add(deduplicationKey)) {
                cleanSqlTxns.add(txn);
            }
        }

        for (in.simplifymoney.ledgersync.model.NormalizedTxn txn : cleanSqlTxns) {
            String acct = txn.accountLast4();
            allAccounts.add(acct);
            accountMonths.computeIfAbsent(acct, k -> new java.util.HashSet<>())
                         .add(java.time.YearMonth.from(txn.occurredAt()));
            
            allMsgIds.addAll(txn.sourceMessageIds());
            
            java.util.Map<in.simplifymoney.ledgersync.model.Category, java.math.BigDecimal> acctTotals = 
                sqlTotals.computeIfAbsent(acct, k -> new java.util.HashMap<>());
            acctTotals.put(txn.category(), acctTotals.getOrDefault(txn.category(), java.math.BigDecimal.ZERO).add(txn.amount()));
        }

        // Q1: forAccountMonth
        for (java.util.Map.Entry<String, java.util.Set<java.time.YearMonth>> entry : accountMonths.entrySet()) {
            String acct = entry.getKey();
            for (java.time.YearMonth ym : entry.getValue()) {
                java.util.List<in.simplifymoney.ledgersync.model.NormalizedTxn> sqlList = cleanSqlTxns.stream()
                        .filter(t -> t.accountLast4().equals(acct) && java.time.YearMonth.from(t.occurredAt()).equals(ym))
                        .sorted(java.util.Comparator.comparing(in.simplifymoney.ledgersync.model.NormalizedTxn::occurredAt).reversed())
                        .toList();
                
                java.util.List<in.simplifymoney.ledgersync.model.NormalizedTxn> docList = documents.forAccountMonth(acct, ym);
                
                if (sqlList.size() != docList.size()) {
                    out.add(new Divergence("forAccountMonth size for " + acct + " " + ym, String.valueOf(sqlList.size()), String.valueOf(docList.size())));
                }
                
                for (int i = 0; i < Math.min(sqlList.size(), docList.size()); i++) {
                    if (!sqlList.get(i).equals(docList.get(i))) {
                        out.add(new Divergence("forAccountMonth element " + i + " for " + acct + " " + ym, sqlList.get(i).toString(), docList.get(i).toString()));
                    }
                }
            }
        }

        // Q2: categoryTotals
        for (String acct : allAccounts) {
            java.util.Map<in.simplifymoney.ledgersync.model.Category, java.math.BigDecimal> sTot = sqlTotals.get(acct);
            java.util.Map<in.simplifymoney.ledgersync.model.Category, java.math.BigDecimal> dTot = documents.categoryTotals(acct);
            for (in.simplifymoney.ledgersync.model.Category cat : in.simplifymoney.ledgersync.model.Category.values()) {
                java.math.BigDecimal s = sTot.getOrDefault(cat, java.math.BigDecimal.ZERO);
                java.math.BigDecimal d = dTot.getOrDefault(cat, java.math.BigDecimal.ZERO);
                // In DynamoDB we stored it exactly, but let's safely compare
                if (s.compareTo(d) != 0) {
                    out.add(new Divergence("categoryTotals " + acct + " " + cat, s.toPlainString(), d.toPlainString()));
                }
            }
        }

        // Q3: byMessageId
        for (String msgId : allMsgIds) {
            in.simplifymoney.ledgersync.model.NormalizedTxn sTxn = cleanSqlTxns.stream()
                    .filter(t -> t.sourceMessageIds().contains(msgId))
                    .findFirst().orElse(null);
            in.simplifymoney.ledgersync.model.NormalizedTxn dTxn = documents.byMessageId(msgId).orElse(null);
            
            if (sTxn != null && dTxn == null) {
                out.add(new Divergence("byMessageId " + msgId, sTxn.toString(), "null"));
            } else if (sTxn == null && dTxn != null) {
                out.add(new Divergence("byMessageId " + msgId, "null", dTxn.toString()));
            } else if (sTxn != null && dTxn != null && !sTxn.equals(dTxn)) {
                out.add(new Divergence("byMessageId " + msgId, sTxn.toString(), dTxn.toString()));
            }
        }

        System.out.println("Consistency Check complete. Found " + out.size() + " divergences.");
        return out;
    }

    /** One place the two stores disagree. */
    public record Divergence(String what, String inSql, String inDocuments) {}
}
