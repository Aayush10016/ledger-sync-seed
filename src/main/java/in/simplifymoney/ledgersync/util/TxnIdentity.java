package in.simplifymoney.ledgersync.util;

import in.simplifymoney.ledgersync.model.NormalizedTxn;

public final class TxnIdentity {

    private TxnIdentity() {}

    /**
     * Determines a strict, deterministic identity for a transaction.
     * The unique identity of a transaction is defined by its lowest sorted message ID.
     * Distinct transactions with identical visible fields but different message IDs 
     * are treated as distinct entities.
     */
    public static String getId(NormalizedTxn txn) {
        String m = txn.merchant() == null ? "" : txn.merchant().trim().toLowerCase();
        return txn.accountLast4() + "|" + txn.occurredAt().toEpochSecond() + "|" + txn.direction().name() + "|" + txn.amount().toPlainString() + "|" + m;
    }
}
