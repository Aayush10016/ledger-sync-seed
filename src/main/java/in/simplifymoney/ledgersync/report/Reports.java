package in.simplifymoney.ledgersync.report;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * The two reports the assignment asks for.
 *
 * summary() below is a first cut: it adds up what is in the ledger. It does not
 * know that a transfer is not spending, and it does not roll micro spends up.
 *
 * reconciliation() has not been written at all.
 */
public final class Reports {

    private Reports() {}

    private static final BigDecimal ZERO = BigDecimal.ZERO.setScale(2);

    public static Map<String, Object> summary(List<NormalizedTxn> ledger) {
        Map<String, Object> accounts = new LinkedHashMap<>();
        for (String acct : new TreeSet<>(ledger.stream()
                .map(NormalizedTxn::accountLast4).toList())) {

            BigDecimal spend = ZERO;
            BigDecimal income = ZERO;
            int micro_count = 0;
            BigDecimal micro_total = ZERO;
            BigDecimal transferred_out = ZERO;
            BigDecimal transferred_in = ZERO;

            for (NormalizedTxn t : ledger) {
                if (!t.accountLast4().equals(acct)) continue;
                
                if (t.category() == Category.SPEND) spend = spend.add(t.amount());
                else if (t.category() == Category.INCOME) income = income.add(t.amount());
                else if (t.category() == Category.MICRO) {
                    micro_count++;
                    micro_total = micro_total.add(t.amount());
                } else if (t.category() == Category.TRANSFER) {
                    if (t.direction() == Direction.DEBIT) transferred_out = transferred_out.add(t.amount());
                    else transferred_in = transferred_in.add(t.amount());
                }
            }

            Map<String, Object> a = new LinkedHashMap<>();
            a.put("spend", spend.toPlainString());
            a.put("income", income.toPlainString());
            a.put("micro_count", micro_count);
            a.put("micro_total", micro_total.toPlainString());
            a.put("transferred_out", transferred_out.toPlainString());
            a.put("transferred_in", transferred_in.toPlainString());
            accounts.put(acct, a);
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("accounts", accounts);
        return doc;
    }

    public static Map<String, Object> ledgerDocument(List<NormalizedTxn> ledger) {
        List<Object> rows = ledger.stream().map(t -> {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("account_last4", t.accountLast4());
            r.put("occurred_at", t.occurredAt().toString());
            r.put("direction", t.direction().name().toLowerCase());
            r.put("amount", t.amount().toPlainString());
            r.put("category", t.category().name());
            r.put("merchant", t.merchant());
            r.put("source_message_ids", t.sourceMessageIds());
            return (Object) r;
        }).toList();
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("transactions", rows);
        return doc;
    }

    public static Map<String, Object> reconciliation(List<in.simplifymoney.ledgersync.model.Discrepancy> discrepancies) {
        List<Map<String, String>> list = new ArrayList<>();
        for (in.simplifymoney.ledgersync.model.Discrepancy d : discrepancies) {
            Map<String, String> map = new LinkedHashMap<>();
            map.put("account_last4", d.accountLast4());
            map.put("occurred_at", d.occurredAt().toString());
            map.put("amount", d.amount().toPlainString());
            map.put("note", d.note());
            list.add(map);
        }
        return Map.of("discrepancies", list);
    }

    public static Map<Category, BigDecimal> byCategory(List<NormalizedTxn> ledger) {
        Map<Category, BigDecimal> out = new LinkedHashMap<>();
        for (Category c : Category.values()) out.put(c, ZERO);
        for (NormalizedTxn t : ledger) {
            out.put(t.category(), out.get(t.category()).add(t.amount()));
        }
        return out;
    }
}
