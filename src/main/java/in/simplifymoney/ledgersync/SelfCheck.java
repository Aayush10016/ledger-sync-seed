package in.simplifymoney.ledgersync;

import in.simplifymoney.ledgersync.ingest.IngestService;
import in.simplifymoney.ledgersync.json.Json;
import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.parse.Parsers;
import in.simplifymoney.ledgersync.store.InMemoryLedgerStore;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Runs the whole pipeline in memory against fixtures/corpus-a.jsonl and prints
 * what it produced next to what fixtures/corpus-a-totals.json says it should
 * have produced.
 *
 * No database, no network, no test framework. `./gradlew selfCheck`.
 */
public final class SelfCheck {

    public static void main(String[] args) throws Exception {
        Path corpus = Path.of(args.length > 0 ? args[0] : "fixtures/corpus-a.jsonl");
        Path totals = Path.of(args.length > 1 ? args[1] : "fixtures/corpus-a-totals.json");
        
        boolean failed = false;

        InMemoryLedgerStore store = new InMemoryLedgerStore();
        IngestService ingest = new IngestService(new Parsers(), store);
        IngestService.Stats stats = ingest.ingestFile(corpus);

        System.out.println("INGEST");
        System.out.printf("  messages read       %d%n", stats.messagesRead());
        System.out.printf("  transactions written %d%n", stats.transactionsWritten());
        System.out.printf("  messages skipped    %d%n", stats.messagesSkipped());
        System.out.printf("  failed writes       %d%n", stats.failedWrites());

        List<NormalizedTxn> ledger = store.all();
        Map<Category, BigDecimal> cats = in.simplifymoney.ledgersync.report.Reports
                .byCategory(ledger);
        System.out.println("\nBY CATEGORY");
        cats.forEach((c, v) -> System.out.printf("  %-9s %12s%n", c, v.toPlainString()));

        Map<String, Object> want = Json.parseObject(Files.readString(totals));
        @SuppressWarnings("unchecked")
        Map<String, Object> accounts = (Map<String, Object>) want.get("accounts");

        System.out.println("\nAGAINST fixtures/corpus-a-totals.json");
        
        int expectedTxns = ((java.math.BigDecimal) want.get("transactions_expected")).intValue();
        System.out.printf("  transactions   expected %d, produced %d%n",
                expectedTxns, ledger.size());
        if (expectedTxns != ledger.size()) {
            System.err.println("  FAIL: Total transaction count mismatch!");
            failed = true;
        }

        for (Map.Entry<String, Object> e : accounts.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> a = (Map<String, Object>) e.getValue();
            BigDecimal opening = new BigDecimal((String) a.get("opening_balance"));
            BigDecimal closing = new BigDecimal((String) a.get("closing_balance"));

            BigDecimal running = opening;
            long n = 0;
            for (NormalizedTxn t : ledger) {
                if (!t.accountLast4().equals(e.getKey())) continue;
                n++;
                running = switch (t.direction()) {
                    case DEBIT -> running.subtract(t.amount());
                    case CREDIT -> running.add(t.amount());
                };
            }
            int expectedAcctTxns = ((java.math.BigDecimal) a.get("transactions_expected")).intValue();
            System.out.printf("  **%s  txns %d (expected %d)%n",
                    e.getKey(), n, expectedAcctTxns);
            if (n != expectedAcctTxns) {
                System.err.printf("  FAIL: Account %s transaction count mismatch!%n", e.getKey());
                failed = true;
            }
            
            System.out.printf("           balance from ledger %s, bank says %s, difference %s%n",
                    running.toPlainString(), closing.toPlainString(),
                    running.subtract(closing).toPlainString());
        }

        System.out.println("\nDISCREPANCIES");
        List<in.simplifymoney.ledgersync.model.Discrepancy> discrepancies = store.discrepancies();
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> allowedDiscs = (List<Map<String, Object>>) want.get("allowed_discrepancies");
        if (allowedDiscs == null) allowedDiscs = List.of();
        
        for (in.simplifymoney.ledgersync.model.Discrepancy d : discrepancies) {
            System.out.println("  " + d);
            boolean allowed = false;
            for (Map<String, Object> ad : allowedDiscs) {
                if (d.accountLast4().equals(ad.get("account")) &&
                    d.amount().toPlainString().equals(ad.get("amount"))) {
                    allowed = true;
                    System.out.println("    (Expected: " + ad.get("reason") + ")");
                    break;
                }
            }
            if (!allowed) {
                System.err.println("  FAIL: Unexplained discrepancy found!");
                failed = true;
            }
        }
        
        if (failed) {
            System.err.println("\nVerification FAILED. See above for details.");
            System.exit(1);
        } else {
            System.out.println("\nVerification PASSED.");
        }
    }
}
