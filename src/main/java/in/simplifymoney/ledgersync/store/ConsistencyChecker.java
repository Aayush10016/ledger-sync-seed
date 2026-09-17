package in.simplifymoney.ledgersync.store;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

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
        System.out.println("Starting High-Performance Consistency Check...");
        List<Divergence> out = new CopyOnWriteArrayList<>();
        
        java.util.Map<String, java.util.Set<java.time.YearMonth>> accountMonths = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.Set<String> allAccounts = java.util.concurrent.ConcurrentHashMap.newKeySet();
        java.util.Set<String> allMsgIds = java.util.concurrent.ConcurrentHashMap.newKeySet();
        
        java.util.Map<String, java.util.Map<in.simplifymoney.ledgersync.model.Category, java.math.BigDecimal>> sqlTotals = new java.util.concurrent.ConcurrentHashMap<>();

        // 1. Gather all data from SQL
        java.util.List<in.simplifymoney.ledgersync.model.NormalizedTxn> allSqlTxns = sql.all();
        // SQL store is dirty, we need to deduplicate it first exactly as backfill does
        java.util.Set<String> seen = java.util.concurrent.ConcurrentHashMap.newKeySet();
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
            accountMonths.computeIfAbsent(acct, k -> java.util.concurrent.ConcurrentHashMap.newKeySet())
                         .add(java.time.YearMonth.from(txn.occurredAt()));
            
            allMsgIds.addAll(txn.sourceMessageIds());
            
            java.util.Map<in.simplifymoney.ledgersync.model.Category, java.math.BigDecimal> acctTotals = 
                sqlTotals.computeIfAbsent(acct, k -> new java.util.concurrent.ConcurrentHashMap<>());
            acctTotals.merge(txn.category(), txn.amount(), java.math.BigDecimal::add);
        }

        // Parallelize Q1: forAccountMonth to bypass N+1 network bottleneck
        System.out.println("Checking Q1 (forAccountMonth) concurrently...");
        accountMonths.entrySet().parallelStream().forEach(entry -> {
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
        });

        // Parallelize Q2: categoryTotals
        System.out.println("Checking Q2 (categoryTotals) concurrently...");
        allAccounts.parallelStream().forEach(acct -> {
            java.util.Map<in.simplifymoney.ledgersync.model.Category, java.math.BigDecimal> sTot = sqlTotals.get(acct);
            java.util.Map<in.simplifymoney.ledgersync.model.Category, java.math.BigDecimal> dTot = documents.categoryTotals(acct);
            for (in.simplifymoney.ledgersync.model.Category cat : in.simplifymoney.ledgersync.model.Category.values()) {
                java.math.BigDecimal s = sTot.getOrDefault(cat, java.math.BigDecimal.ZERO);
                java.math.BigDecimal d = dTot.getOrDefault(cat, java.math.BigDecimal.ZERO);
                if (s.compareTo(d) != 0) {
                    out.add(new Divergence("categoryTotals " + acct + " " + cat, s.toPlainString(), d.toPlainString()));
                }
            }
        });

        // Parallelize Q3: byMessageId
        System.out.println("Checking Q3 (byMessageId) concurrently...");
        allMsgIds.parallelStream().forEach(msgId -> {
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
        });

        System.out.println("Consistency Check complete. Found " + out.size() + " divergences.");
        return new java.util.ArrayList<>(out);
    }

    /** One place the two stores disagree. */
    public record Divergence(String what, String inSql, String inDocuments) {}
}

