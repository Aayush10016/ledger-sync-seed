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
        java.util.Map<String, in.simplifymoney.ledgersync.model.NormalizedTxn> deduplicatedTxns = new java.util.LinkedHashMap<>();
        for (in.simplifymoney.ledgersync.model.NormalizedTxn txn : allSqlTxns) {
            String deduplicationKey = txn.accountLast4() + "|" + txn.occurredAt().toEpochSecond() + "|" + txn.direction() + "|" + txn.amount();
            deduplicatedTxns.merge(deduplicationKey, txn, (existing, incoming) -> {
                java.util.Set<String> mergedIds = new java.util.HashSet<>(existing.sourceMessageIds());
                mergedIds.addAll(incoming.sourceMessageIds());
                return new in.simplifymoney.ledgersync.model.NormalizedTxn(
                    existing.accountLast4(), existing.occurredAt(), existing.direction(),
                    existing.amount(), existing.category(), existing.merchant(),
                    mergedIds.stream().sorted().toList()
                );
            });
        }
        java.util.List<in.simplifymoney.ledgersync.model.NormalizedTxn> cleanSqlTxns = new java.util.ArrayList<>(deduplicatedTxns.values());

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

        // Build efficient lookups to avoid O(M * T) nested loops
        java.util.Map<String, java.util.List<in.simplifymoney.ledgersync.model.NormalizedTxn>> acctMonthToTxns = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.Map<String, in.simplifymoney.ledgersync.model.NormalizedTxn> msgIdToTxn = new java.util.concurrent.ConcurrentHashMap<>();

        for (in.simplifymoney.ledgersync.model.NormalizedTxn txn : cleanSqlTxns) {
            String acct = txn.accountLast4();
            java.time.YearMonth ym = java.time.YearMonth.from(txn.occurredAt());
            String key = acct + "|" + ym.toString();
            acctMonthToTxns.computeIfAbsent(key, k -> new java.util.ArrayList<>()).add(txn);
            
            for (String msgId : txn.sourceMessageIds()) {
                in.simplifymoney.ledgersync.model.NormalizedTxn existing = msgIdToTxn.putIfAbsent(msgId, txn);
                if (existing != null && !existing.equals(txn)) {
                    out.add(new Divergence("Duplicate message ID in SQL: " + msgId, existing.toString(), txn.toString()));
                }
            }
        }

        for (java.util.List<in.simplifymoney.ledgersync.model.NormalizedTxn> list : acctMonthToTxns.values()) {
            list.sort(java.util.Comparator.comparing(in.simplifymoney.ledgersync.model.NormalizedTxn::occurredAt).reversed());
        }

        // Parallelize Q1: forAccountMonth to bypass N+1 network bottleneck
        System.out.println("Checking Q1 (forAccountMonth) concurrently...");
        accountMonths.entrySet().parallelStream().forEach(entry -> {
            String acct = entry.getKey();
            for (java.time.YearMonth ym : entry.getValue()) {
                String key = acct + "|" + ym.toString();
                java.util.List<in.simplifymoney.ledgersync.model.NormalizedTxn> sqlList = acctMonthToTxns.getOrDefault(key, java.util.Collections.emptyList());
                
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
        // Note on DocumentStore enumeration limitation:
        // Because the DocumentStore interface is frozen and only provides queries by `accountLast4`, 
        // we can only verify consistency for accounts derived from the SQL store (`allAccounts`).
        // To natively discover document-side "ghost" records for entirely new accounts or months, 
        // we would require an enumeration method like `client.scan()` which is explicitly forbidden
        // by the assignment's rule: "DocumentStore declares the only three queries this service makes".
        System.out.println("Checking Q2 (categoryTotals) concurrently...");
        allAccounts.parallelStream().forEach(acct -> {
            java.util.Map<in.simplifymoney.ledgersync.model.Category, java.math.BigDecimal> sTot = sqlTotals.getOrDefault(acct, new java.util.concurrent.ConcurrentHashMap<>());
            java.util.Map<in.simplifymoney.ledgersync.model.Category, java.math.BigDecimal> dTot = documents.categoryTotals(acct);
            
            // If both are empty, skip
            if (sTot.isEmpty() && dTot.isEmpty()) return;

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
            in.simplifymoney.ledgersync.model.NormalizedTxn sTxn = msgIdToTxn.get(msgId);
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

