package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.NormalizedTxn;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Used by SelfCheck and by tests. Keeps everything it is given. */
public final class InMemoryLedgerStore implements LedgerStore {

    private final List<NormalizedTxn> rows = new ArrayList<>();
    private final List<in.simplifymoney.ledgersync.model.Discrepancy> disc = new ArrayList<>();

    @Override public void save(NormalizedTxn txn) {
        NormalizedTxn existing = null;
        for (String msgId : txn.sourceMessageIds()) {
            for (NormalizedTxn r : rows) {
                if (r.sourceMessageIds().contains(msgId)) {
                    existing = r;
                    break;
                }
            }
            if (existing != null) break;
        }
        if (existing != null) {
            java.util.Set<String> mergedIds = new java.util.TreeSet<>(existing.sourceMessageIds());
            mergedIds.addAll(txn.sourceMessageIds());
            NormalizedTxn updated = new NormalizedTxn(
                    existing.accountLast4(), existing.occurredAt(), existing.direction(), 
                    existing.amount(), existing.category(), existing.merchant(),
                    new java.util.ArrayList<>(mergedIds)
            );
            rows.remove(existing);
            rows.add(updated);
        } else {
            rows.add(txn);
        }
    }

    @Override public List<NormalizedTxn> all() { return Collections.unmodifiableList(rows); }

    public List<NormalizedTxn> scanAllTransactions() { return all(); }

    @Override public void save(in.simplifymoney.ledgersync.model.Discrepancy d) { disc.add(d); }

    @Override public List<in.simplifymoney.ledgersync.model.Discrepancy> discrepancies() { return Collections.unmodifiableList(disc); }

    @Override public long count() { return rows.size(); }
}
