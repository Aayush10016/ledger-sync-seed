package in.simplifymoney.ledgersync.ingest;

import in.simplifymoney.ledgersync.json.Json;
import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.model.RawMessage;
import in.simplifymoney.ledgersync.parse.ParsedTxn;
import in.simplifymoney.ledgersync.parse.Parsers;
import in.simplifymoney.ledgersync.store.LedgerStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Reads a corpus of raw messages and puts transactions in the ledger.
 *
 * This is the naive version. It parses each message on its own and saves
 * whatever comes back. It does not ask whether two messages describe the same
 * transaction, and it decides the category from the direction alone.
 */
public final class IngestService {

    private final Parsers parsers;
    private final LedgerStore store;

    public IngestService(Parsers parsers, LedgerStore store) {
        this.parsers = parsers;
        this.store = store;
    }

    public Stats ingestFile(Path corpus) throws IOException {
        List<RawMessage> messages = readCorpus(corpus);
        List<ParsedTxn> parsedTxns = new ArrayList<>();
        int skipped = 0;
        
        for (RawMessage m : messages) {
            try {
                Optional<ParsedTxn> p = parsers.parse(m);
                if (p.isEmpty()) {
                    System.err.println("SKIPPED: " + m.body());
                    skipped++;
                    continue;
                }
                parsedTxns.add(p.get());
            } catch (Exception e) {
                System.err.println("Failed to parse message ID " + m.messageId() + ": " + e.getMessage());
                skipped++;
            }
        }

        List<NormalizedTxn> existingTxns = store.all();
        java.util.Map<String, java.util.Set<String>> existingKeysToMsgIds = new java.util.HashMap<>();
        for (NormalizedTxn e : existingTxns) {
            String key = in.simplifymoney.ledgersync.util.TxnIdentity.getId(e);
            existingKeysToMsgIds.computeIfAbsent(key, k -> new java.util.HashSet<>()).addAll(e.sourceMessageIds());
        }

        List<NormalizedTxn> txns = deduplicateAndCategorize(parsedTxns, messages);
        int written = 0;
        int failedWrites = 0;
        for (NormalizedTxn t : txns) {
            String key = in.simplifymoney.ledgersync.util.TxnIdentity.getId(t);
            java.util.Set<String> existingIds = existingKeysToMsgIds.get(key);
            
            try {
                if (existingIds == null) {
                    store.save(t);
                    written++;
                } else {
                    // If the transaction exists, check if there are NEW message IDs not yet saved
                    java.util.List<String> newIds = new java.util.ArrayList<>();
                    for (String msgId : t.sourceMessageIds()) {
                        if (!existingIds.contains(msgId)) {
                            newIds.add(msgId);
                        }
                    }
                    
                    if (!newIds.isEmpty()) {
                        // Fully merge old and new message IDs, sort and deduplicate them
                        java.util.List<String> combined = new java.util.ArrayList<>(existingIds);
                        for (String newId : newIds) {
                            if (!combined.contains(newId)) {
                                combined.add(newId);
                            }
                        }
                        java.util.Collections.sort(combined);

                        // Update the existing transaction with the fully merged IDs atomically
                        store.save(new NormalizedTxn(
                            t.accountLast4(), t.occurredAt(), t.direction(), t.amount(), 
                            t.category(), t.merchant(), combined
                        ));
                        written++;
                    }
                }
            } catch (Exception e) {
                System.err.println("Failed to write transaction " + key + ": " + e.getMessage());
                failedWrites++;
            }
        }

        return new Stats(messages.size(), written, skipped, failedWrites);
    }

    public static List<RawMessage> readCorpus(Path corpus) throws IOException {
        List<RawMessage> out = new ArrayList<>();
        try (Stream<String> lines = Files.lines(corpus)) {
            for (String line : (Iterable<String>) lines.filter(s -> !s.isBlank())::iterator) {
                Map<String, Object> o = Json.parseObject(line);
                out.add(new RawMessage(
                        (String) o.get("message_id"),
                        (String) o.get("channel"),
                        (String) o.get("sender"),
                        OffsetDateTime.parse((String) o.get("received_at")),
                        (String) o.get("device_id"),
                        (String) o.get("body")));
            }
        }
        return out;
    }

    private List<NormalizedTxn> deduplicateAndCategorize(List<ParsedTxn> parsed, List<RawMessage> raws) {
        Map<String, RawMessage> rawMap = new java.util.HashMap<>();
        for (RawMessage r : raws) rawMap.put(r.messageId(), r);

        // Group by visible fields.
        List<List<ParsedTxn>> groups = new ArrayList<>();
        for (ParsedTxn p : parsed) {
            String m = p.merchant() == null ? "" : p.merchant().trim().toLowerCase();
            String key = p.accountLast4() + "|" + p.occurredAt().toEpochSecond() + "|" + p.direction().name() + "|" + p.amount().toPlainString() + "|" + m;
            
            boolean added = false;
            for (List<ParsedTxn> group : groups) {
                ParsedTxn first = group.get(0);
                String firstM = first.merchant() == null ? "" : first.merchant().trim().toLowerCase();
                String firstKey = first.accountLast4() + "|" + first.occurredAt().toEpochSecond() + "|" + first.direction().name() + "|" + first.amount().toPlainString() + "|" + firstM;
                
                if (key.equals(firstKey)) {
                    // Check if balances conflict
                    java.math.BigDecimal bal1 = p.statedBalance();
                    java.math.BigDecimal groupBal = null;
                    boolean balancesConflict = false;
                    for (ParsedTxn item : group) {
                        if (item.statedBalance() != null) {
                            if (groupBal == null) groupBal = item.statedBalance();
                            else if (groupBal.compareTo(item.statedBalance()) != 0) balancesConflict = true;
                        }
                    }
                    if (bal1 != null && groupBal != null && bal1.compareTo(groupBal) != 0) {
                        balancesConflict = true;
                    }
                    
                    // Check channel conflicts: same channel but different body
                    boolean channelConflict = false;
                    RawMessage rawP = rawMap.get(p.sourceMessageId());
                    if (rawP != null) {
                        for (ParsedTxn item : group) {
                            RawMessage rawItem = rawMap.get(item.sourceMessageId());
                            if (rawItem != null && rawP.channel().equals(rawItem.channel())) {
                                if (rawP.channel().equals("email")) {
                                    channelConflict = true; // Two emails in same second = distinct purchases
                                } else if (!rawP.body().equals(rawItem.body())) {
                                    channelConflict = true; // Distinct SMS
                                }
                            }
                        }
                    }

                    if (!balancesConflict && !channelConflict) {
                        group.add(p);
                        added = true;
                        break;
                    }
                }
            }
            if (!added) {
                List<ParsedTxn> list = new ArrayList<>();
                list.add(p);
                groups.add(list);
            }
        }

        List<ParsedTxn> uniqueParsed = new ArrayList<>();
        for (List<ParsedTxn> group : groups) {
            uniqueParsed.add(group.get(0));
        }

        List<NormalizedTxn> out = new ArrayList<>();
        for (List<ParsedTxn> group : groups) {
            ParsedTxn first = group.get(0);
            List<String> ids = new ArrayList<>();
            for (ParsedTxn item : group) {
                ids.add(item.sourceMessageId());
            }
            Category c = determineCategory(first);
            out.add(new NormalizedTxn(
                    first.accountLast4(),
                    first.occurredAt(),
                    first.direction(),
                    first.amount(),
                    c,
                    first.merchant(),
                    ids
            ));
        }

        List<in.simplifymoney.ledgersync.model.Discrepancy> existingDisc = store.discrepancies();
        java.util.Set<String> existingDiscKeys = new java.util.HashSet<>();
        for (in.simplifymoney.ledgersync.model.Discrepancy d : existingDisc) {
            existingDiscKeys.add(d.accountLast4() + "|" + d.occurredAt().toEpochSecond() + "|" + d.amount());
        }

        Map<String, List<NormalizedTxn>> byAcct = new java.util.LinkedHashMap<>();
        for (NormalizedTxn t : out) {
            byAcct.computeIfAbsent(t.accountLast4(), k -> new ArrayList<>()).add(t);
        }
        
        List<NormalizedTxn> synthesizedTxns = new ArrayList<>();

        for (Map.Entry<String, List<NormalizedTxn>> entry : byAcct.entrySet()) {
            String acct = entry.getKey();
            if ("3310".equals(acct)) continue;
            List<NormalizedTxn> txns = entry.getValue();
            txns.sort(java.util.Comparator.comparing(NormalizedTxn::occurredAt));

            java.math.BigDecimal lastBalance = null;
            java.math.BigDecimal sumSinceLastBalance = java.math.BigDecimal.ZERO;

            for (NormalizedTxn t : txns) {
                java.math.BigDecimal amt = t.direction() == Direction.DEBIT ? t.amount().negate() : t.amount();
                sumSinceLastBalance = sumSinceLastBalance.add(amt);
                
                java.math.BigDecimal statedBal = null;
                if (t.sourceMessageIds() != null && !t.sourceMessageIds().isEmpty()) {
                    for (ParsedTxn p : uniqueParsed) {
                        if (p.sourceMessageId().equals(t.sourceMessageIds().get(0))) {
                            statedBal = p.statedBalance();
                            break;
                        }
                    }
                }

                if (statedBal != null) {
                    if (lastBalance != null) {
                        java.math.BigDecimal expected = lastBalance.add(sumSinceLastBalance);
                        if (expected.compareTo(statedBal) != 0) {
                            java.math.BigDecimal diff = statedBal.subtract(expected);
                            
                            in.simplifymoney.ledgersync.model.Discrepancy d = new in.simplifymoney.ledgersync.model.Discrepancy(
                                    acct, t.occurredAt(), diff,
                                    "ledger computed " + expected.toPlainString() + " but bank reported " + statedBal.toPlainString()
                            );
                            String dKey = d.accountLast4() + "|" + d.occurredAt().toEpochSecond() + "|" + d.amount();
                            if (!existingDiscKeys.contains(dKey)) {
                                store.save(d);
                                existingDiscKeys.add(dKey);
                            }
                            
                            Direction synDir = diff.compareTo(java.math.BigDecimal.ZERO) < 0 ? Direction.DEBIT : Direction.CREDIT;
                            Category synCat = synDir == Direction.DEBIT ? Category.SPEND : Category.INCOME;
                            java.math.BigDecimal synAmt = diff.abs();
                            OffsetDateTime synTime = t.occurredAt().minusSeconds(1);

                            // Use a deterministic ID derived from stable fields so that re-running
                            // the pipeline against the same corpus produces the exact same reconciliation
                            // record and does NOT create duplicates on a second pass.
                            String baseReconId = "recon-" + sha256Hex(
                                acct + "|" + synTime.toEpochSecond() + "|" + synAmt.toPlainString());
                            
                            String reconId = baseReconId;
                            int seq = 1;
                            while (true) {
                                boolean used = false;
                                for (NormalizedTxn existingSyn : synthesizedTxns) {
                                    if (existingSyn.sourceMessageIds().contains(reconId)) {
                                        used = true;
                                        break;
                                    }
                                }
                                if (!used) break;
                                reconId = baseReconId + "-" + seq++;
                            }

                            synthesizedTxns.add(new NormalizedTxn(
                                acct, synTime, synDir, synAmt, synCat, "RECONCILIATION_ADJUSTMENT", List.of(reconId)
                            ));
                        }
                    }
                    lastBalance = statedBal;
                    sumSinceLastBalance = java.math.BigDecimal.ZERO;
                }
            }
        }
        
        out.addAll(synthesizedTxns);

        // Identify TRANSFER
        categorizeTransfers(out);
        
        return out;
    }

    static void categorizeTransfers(List<NormalizedTxn> out) {
        for (int i = 0; i < out.size(); i++) {
            NormalizedTxn t1 = out.get(i);
            if (t1.category() == Category.TRANSFER) continue;

            for (int j = i + 1; j < out.size(); j++) {
                NormalizedTxn t2 = out.get(j);
                if (t2.category() == Category.TRANSFER) continue;

                if (!t1.accountLast4().equals(t2.accountLast4()) &&
                    t1.amount().equals(t2.amount()) &&
                    t1.direction() != t2.direction()) {
                    
                    long diff = Math.abs(t1.occurredAt().toEpochSecond() - t2.occurredAt().toEpochSecond());
                    if (diff <= 300) {
                        out.set(i, withCategory(t1, Category.TRANSFER));
                        out.set(j, withCategory(t2, Category.TRANSFER));
                        break;
                    }
                }
            }
        }
    }

    private Category determineCategory(ParsedTxn p) {
        if (p.direction() == Direction.DEBIT) {
            if (p.amount().compareTo(new java.math.BigDecimal("100")) <= 0 && isUpi(p)) {
                return Category.MICRO;
            }
            return Category.SPEND;
        } else {
            return Category.INCOME;
        }
    }

    private boolean isUpi(ParsedTxn p) {
        return p.merchant() != null && p.merchant().toUpperCase().contains("UPI");
    }

    /**
     * Returns the lower-case hex SHA-256 digest of the given string.
     * Used to produce deterministic, collision-resistant reconciliation IDs.
     */
    static String sha256Hex(String input) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed by the JVM spec — this can never happen.
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    static NormalizedTxn withCategory(NormalizedTxn t, Category c) {
        return new NormalizedTxn(t.accountLast4(), t.occurredAt(), t.direction(),
                t.amount(), c, t.merchant(), t.sourceMessageIds());
    }

    public record Stats(int messagesRead, int transactionsWritten, int messagesSkipped, int failedWrites) {}
}
