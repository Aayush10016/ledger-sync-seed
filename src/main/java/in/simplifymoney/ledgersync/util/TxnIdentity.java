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
        String base = txn.accountLast4() + "|" + txn.occurredAt().toEpochSecond() + "|" + txn.direction().name() + "|" + txn.amount().toPlainString() + "|" + m;
        
        if (txn.sourceMessageIds() != null && !txn.sourceMessageIds().isEmpty()) {
            java.util.List<String> sorted = new java.util.ArrayList<>(txn.sourceMessageIds());
            java.util.Collections.sort(sorted);
            base += "|" + sorted.get(0);
        }
        
        return java.util.Base64.getEncoder().encodeToString(base.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
