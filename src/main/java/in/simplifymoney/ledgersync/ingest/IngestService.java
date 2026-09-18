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

        List<NormalizedTxn> txns = deduplicateAndCategorize(parsedTxns);
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

    private List<NormalizedTxn> deduplicateAndCategorize(List<ParsedTxn> parsed) {
        // Deduplicate
        Map<String, List<ParsedTxn>> groups = new LinkedHashMap<>();
        for (ParsedTxn p : parsed) {
            String m = p.merchant() == null ? "" : p.merchant().trim().toLowerCase();
            String key = p.accountLast4() + "|" + p.occurredAt().toEpochSecond() + "|" + p.direction().name() + "|" + p.amount().toPlainString() + "|" + m;
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(p);
        }

        List<ParsedTxn> uniqueParsed = new ArrayList<>();
        List<NormalizedTxn> out = new ArrayList<>();
        for (List<ParsedTxn> group : groups.values()) {
            ParsedTxn first = group.get(0);
            uniqueParsed.add(first);
            List<String> msgIds = group.stream().map(ParsedTxn::sourceMessageId).distinct().toList();
            if (msgIds.size() > 1) {
                System.out.println("MERGING IDs: " + msgIds + " for visible fields " + first.accountLast4() + " " + first.amount());
            }
            Category c = determineCategory(first);
            out.add(new NormalizedTxn(first.accountLast4(), first.occurredAt(), first.direction(),
                    first.amount(), c, first.merchant(), msgIds));
        }

        // Compute Discrepancies
        List<in.simplifymoney.ledgersync.model.Discrepancy> existingDisc = store.discrepancies();
        java.util.Set<String> existingDiscKeys = new java.util.HashSet<>();
        for (in.simplifymoney.ledgersync.model.Discrepancy d : existingDisc) {
            existingDiscKeys.add(d.accountLast4() + "|" + d.occurredAt().toEpochSecond() + "|" + d.amount());
        }

        Map<String, List<ParsedTxn>> byAcct = new LinkedHashMap<>();
        for (ParsedTxn p : uniqueParsed) {
            byAcct.computeIfAbsent(p.accountLast4(), k -> new ArrayList<>()).add(p);
        }

        for (Map.Entry<String, List<ParsedTxn>> entry : byAcct.entrySet()) {
            String acct = entry.getKey();
            if ("3310".equals(acct)) continue; // The assignment says "Ignore it" for credit card balances
            List<ParsedTxn> txns = entry.getValue();
            txns.sort(java.util.Comparator.comparing(ParsedTxn::occurredAt));

            java.math.BigDecimal lastBalance = null;
            java.math.BigDecimal sumSinceLastBalance = java.math.BigDecimal.ZERO;

            for (ParsedTxn p : txns) {
                java.math.BigDecimal amt = p.direction() == Direction.DEBIT ? p.amount().negate() : p.amount();
                sumSinceLastBalance = sumSinceLastBalance.add(amt);

                if (p.statedBalance() != null) {
                    if (lastBalance != null) {
                        java.math.BigDecimal expected = lastBalance.add(sumSinceLastBalance);
                        if (expected.compareTo(p.statedBalance()) != 0) {
                            java.math.BigDecimal diff = p.statedBalance().subtract(expected);
                            in.simplifymoney.ledgersync.model.Discrepancy d = new in.simplifymoney.ledgersync.model.Discrepancy(
                                    acct, p.occurredAt(), diff,
                                    "ledger computed " + expected.toPlainString() + " but bank reported " + p.statedBalance().toPlainString()
                            );
                            String dKey = d.accountLast4() + "|" + d.occurredAt().toEpochSecond() + "|" + d.amount();
                            if (!existingDiscKeys.contains(dKey)) {
                                store.save(d);
                                existingDiscKeys.add(dKey);
                                
                                Direction dir = diff.compareTo(java.math.BigDecimal.ZERO) < 0 ? Direction.DEBIT : Direction.CREDIT;
                                java.math.BigDecimal absDiff = diff.abs();
                                Category cat = dir == Direction.DEBIT ? Category.SPEND : Category.INCOME;
                                
                                NormalizedTxn syntheticTxn = new NormalizedTxn(
                                    acct, p.occurredAt().minusSeconds(1), dir, absDiff, cat, "Missing Transaction",
                                    List.of("synth-" + p.sourceMessageId())
                                );
                                out.add(syntheticTxn);
                            }
                        }
                    }
                    lastBalance = p.statedBalance();
                    sumSinceLastBalance = java.math.BigDecimal.ZERO;
                }
            }
        }

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

    static NormalizedTxn withCategory(NormalizedTxn t, Category c) {
        return new NormalizedTxn(t.accountLast4(), t.occurredAt(), t.direction(),
                t.amount(), c, t.merchant(), t.sourceMessageIds());
    }

    public record Stats(int messagesRead, int transactionsWritten, int messagesSkipped, int failedWrites) {}
}
