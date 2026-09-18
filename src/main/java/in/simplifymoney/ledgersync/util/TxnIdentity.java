package in.simplifymoney.ledgersync.util;

import in.simplifymoney.ledgersync.model.NormalizedTxn;

public final class TxnIdentity {

    private TxnIdentity() {}

    /**
     * Deterministic identity for a newly observed transaction.
     *
     * Persistent stores must first look up existing transactions by source-message
     * ID and retain the already assigned txn_id when any source overlaps. This
     * method is only the initial assignment path for a source set that has never
     * been persisted before.
     */
    public static String getId(NormalizedTxn txn) {
        if (txn.bankReferenceId() != null) {
            return "ref-" + sha256Hex(txn.bankReferenceId());
        }
        if (txn.sourceMessageIds() == null || txn.sourceMessageIds().isEmpty()) {
            throw new IllegalArgumentException("Transactions must have at least one source message ID to guarantee uniqueness.");
        }
        String base = "sources|";
        java.util.List<String> sorted = new java.util.ArrayList<>(txn.sourceMessageIds());
        java.util.Collections.sort(sorted);
        base += String.join(",", sorted);

        return "txn-" + sha256Hex(base);
    }

    public static String sha256Hex(String input) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
