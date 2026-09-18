package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Discrepancy;
import in.simplifymoney.ledgersync.model.NormalizedTxn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Lets the ingestion pipeline write directly to DynamoDB while still using the
 * LedgerStore contract expected by IngestService.
 */
public final class DynamoDbLedgerAdapter implements LedgerStore {

    private final DynamoDbLedgerStore store;
    private final List<Discrepancy> discrepancies = new ArrayList<>();

    public DynamoDbLedgerAdapter(DynamoDbLedgerStore store) {
        this.store = store;
    }

    @Override
    public void save(NormalizedTxn txn) {
        store.save(txn);
    }

    @Override
    public List<NormalizedTxn> all() {
        return store.scanAllTransactions();
    }

    @Override
    public void save(Discrepancy d) {
        discrepancies.add(d);
    }

    @Override
    public List<Discrepancy> discrepancies() {
        return Collections.unmodifiableList(discrepancies);
    }

    @Override
    public long count() {
        return all().size();
    }
}
