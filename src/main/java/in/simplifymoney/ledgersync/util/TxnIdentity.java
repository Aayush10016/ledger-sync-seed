package in.simplifymoney.ledgersync.util;

import in.simplifymoney.ledgersync.model.NormalizedTxn;

public final class TxnIdentity {

    private TxnIdentity() {}

    /**
     * Determines a strict, deterministic identity for a transaction.
     * 
     * Identity Anchor Strategy:
     * When multiple source messages (e.g. an SMS and an Email) represent the same 
     * logical transaction, the transaction's canonical identity is anchored to the 
     * lexicographically lowest message ID among its sources, combined with its visible fields.
     * This provides a stable, repeatable deterministic identity, while explicitly preventing 
     * two independent identical transactions (e.g. two $5 coffees at the same minute) from merging.
     *
     * KNOWN DOMAIN LIMITATION (Identity Drift):
     * Because NormalizedTxn is a frozen domain contract, we cannot assign a persistent `txn_id` 
     * field at creation time. If a transaction later receives an out-of-order delayed source 
     * message whose ID sorts lexicographically earlier than all existing IDs, the transaction's
     * canonical identity WILL DRIFT. This will result in an orphaned legacy record in document 
     * stores that don't support atomic renames. This edge-case is an accepted limitation of purely 
     * stateless identity derivation.
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
