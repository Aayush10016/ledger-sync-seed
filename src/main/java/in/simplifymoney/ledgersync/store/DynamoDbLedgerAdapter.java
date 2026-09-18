package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Discrepancy;
import in.simplifymoney.ledgersync.model.NormalizedTxn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Lets the ingestion pipeline write directly to DynamoDB while still using the
 * LedgerStore contract expected by IngestService.
 *
 * <p>Discrepancies are held in an in-memory list for the lifetime of this
 * adapter instance. They are not persisted to DynamoDB because:
 * <ul>
 *   <li>The DynamoDB table schema has no discrepancy partition — discrepancies
 *       are a SQL-side concern surfaced via {@code reconciliation.json}.</li>
 *   <li>Discrepancies are re-derived from balance gaps on every ingest run, so
 *       durable storage would require a deduplication key that the current
 *       assignment scope does not define for DynamoDB.</li>
 * </ul>
 * If durable DynamoDB discrepancy storage is required in a future iteration,
 * add a {@code DISC#} sort-key partition and update this adapter accordingly.
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
