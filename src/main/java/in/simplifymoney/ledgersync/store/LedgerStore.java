package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.NormalizedTxn;
import java.util.List;

/**
 * Where transactions live.
 *
 * Note what this interface does NOT promise: that saving the same transaction
 * twice results in one row.
 */
public interface LedgerStore {

    void save(NormalizedTxn txn);

    List<NormalizedTxn> all();

    void save(in.simplifymoney.ledgersync.model.Discrepancy d);

    List<in.simplifymoney.ledgersync.model.Discrepancy> discrepancies();

    long count();
}
