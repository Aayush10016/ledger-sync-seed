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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Reads a corpus of raw messages and puts transactions in the ledger.
 *
 * Multiple alerts for the same underlying bank event are merged before save.
 * Balance gaps are recorded as discrepancies; they are not inserted as ledger
 * transactions.
 */
public final class IngestService {

    private static final String BALANCE_EXCLUSION_PROPERTY =
            "ledger.accounts-without-reliable-balances";
    private static final String DEFAULT_BALANCE_EXCLUSIONS = "3310";

    private final Parsers parsers;
    private final LedgerStore store;
    private final java.util.Set<String> accountsWithoutReliableBalances;

    public IngestService(Parsers parsers, LedgerStore store) {
        this(parsers, store, configuredAccountsWithoutReliableBalances());
    }

    public IngestService(Parsers parsers, LedgerStore store,
                         java.util.Set<String> accountsWithoutReliableBalances) {
        this.parsers = parsers;
        this.store = store;
        this.accountsWithoutReliableBalances = java.util.Set.copyOf(accountsWithoutReliableBalances);
    }

    public Stats ingestFile(Path corpus) throws IOException {
        CorpusReadResult readResult = readCorpusDetailed(corpus);
        List<RawMessage> messages = readResult.validRecords();
        List<ParsedTxn> parsedTxns = new ArrayList<>();
        List<SkipDetail> skipDetails = new ArrayList<>();
        
        for (RawMessage m : messages) {
            try {
                Optional<ParsedTxn> p = parsers.parse(m);
                if (p.isEmpty()) {
                    skipDetails.add(new SkipDetail(m.messageId(), "unsupported format"));
                    continue;
                }
                parsedTxns.add(p.get());
            } catch (Exception e) {
                skipDetails.add(new SkipDetail(m.messageId(), e.getMessage()));
            }
        }
        
        if (!skipDetails.isEmpty()) {
            System.err.println("Skipped " + skipDetails.size() + " messages:");
            Map<String, Integer> reasons = new java.util.HashMap<>();
            for (SkipDetail d : skipDetails) {
                reasons.merge(d.reason(), 1, Integer::sum);
            }
            reasons.forEach((r, count) -> System.err.println("- " + r + ": " + count));
        }

        List<NormalizedTxn> txns = deduplicateAndCategorize(parsedTxns, messages);
        int written = 0;
        int failedWrites = 0;
        for (NormalizedTxn t : txns) {
            try {
                store.save(t);
                written++;
            } catch (Exception e) {
                System.err.println("Failed to write transaction " + t + ": " + e.getMessage());
                failedWrites++;
            }
        }

        return new Stats(readResult.recordsRead(), written, skipDetails.size(), failedWrites,
                skipDetails, readResult.malformedRecords(), readResult.malformedDetails());
    }

    public static List<RawMessage> readCorpus(Path corpus) throws IOException {
        return readCorpusDetailed(corpus).validRecords();
    }

    public static CorpusReadResult readCorpusDetailed(Path corpus) throws IOException {
        List<RawMessage> out = new ArrayList<>();
        List<MalformedRecord> malformed = new ArrayList<>();
        int recordsRead = 0;
        try (Stream<String> lines = Files.lines(corpus)) {
            int lineNum = 0;
            for (String line : (Iterable<String>) lines.filter(s -> !s.isBlank())::iterator) {
                lineNum++;
                recordsRead++;
                try {
                    Map<String, Object> o = Json.parseObject(line);
                    
                    String messageId = (String) o.get("message_id");
                    String channel = (String) o.get("channel");
                    String receivedAtStr = (String) o.get("received_at");
                    OffsetDateTime receivedAt = receivedAtStr == null ? null : OffsetDateTime.parse(receivedAtStr);
                    
                    out.add(new RawMessage(
                            messageId,
                            channel,
                            (String) o.get("sender"),
                            receivedAt,
                            (String) o.get("device_id"),
                            (String) o.get("body")));
                } catch (Exception e) {
                    malformed.add(new MalformedRecord(lineNum, e.getMessage(), line));
                }
            }
        }
        if (!malformed.isEmpty()) {
            System.err.println("Malformed corpus records: " + malformed.size());
            for (MalformedRecord record : malformed) {
                System.err.println("- line " + record.lineNumber() + ": " + record.reason());
            }
        }
        return new CorpusReadResult(recordsRead, out, malformed);
    }

    private List<NormalizedTxn> deduplicateAndCategorize(List<ParsedTxn> parsed, List<RawMessage> raws) {
        Map<String, RawMessage> rawMap = new java.util.HashMap<>();
        for (RawMessage r : raws) rawMap.put(r.messageId(), r);

        // Group by immutable transaction facts. Merchant is intentionally not
        // part of this key because SMS and email alerts often name the same
        // counterparty differently.
        List<List<ParsedTxn>> groups = new ArrayList<>();
        for (ParsedTxn p : parsed) {
            String key = p.accountLast4() + "|" + p.occurredAt().toEpochSecond() + "|" + p.direction().name() + "|" + p.amount().toPlainString();
            
            boolean added = false;
            for (List<ParsedTxn> group : groups) {
                ParsedTxn first = group.get(0);
                String firstKey = first.accountLast4() + "|" + first.occurredAt().toEpochSecond() + "|" + first.direction().name() + "|" + first.amount().toPlainString();
                
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
                    
                    // Strong evidence check: Bank references
                    boolean refConflict = false;
                    boolean refMatch = false;
                    String ref1 = p.bankReferenceId();
                    if (ref1 != null) {
                        for (ParsedTxn item : group) {
                            if (item.bankReferenceId() != null) {
                                RawMessage rawP = rawMap.get(p.sourceMessageId());
                                RawMessage rawItem = rawMap.get(item.sourceMessageId());
                                boolean isCrossChannel = rawP != null && rawItem != null && !rawP.channel().equals(rawItem.channel());
                                
                                if (ref1.equals(item.bankReferenceId())) {
                                    refMatch = true;
                                } else if (!isCrossChannel) {
                                    // Only conflict if same channel. Cross-channel often has different ref formats.
                                    refConflict = true;
                                }
                            }
                        }
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

                    boolean crossChannel = rawP != null && group.stream().anyMatch(
                            item -> rawMap.containsKey(item.sourceMessageId()) &&
                                    !rawP.channel().equals(rawMap.get(item.sourceMessageId()).channel()));

                    // Keep ambiguous records separate: if they lack strong cross-channel evidence or have any conflicts
                    if (refConflict || balancesConflict || channelConflict || (crossChannel && !refMatch && !canGroup(rawP, group, rawMap))) {
                        continue; // try next group or add as new
                    }

                    group.add(p);
                    added = true;
                    break;
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
            String bestMerchant = chooseBestMerchant(group, rawMap);
            Category c = determineCategory(group, bestMerchant);
            
            String bankRef = null;
            for (ParsedTxn item : group) {
                if (item.bankReferenceId() != null) {
                    bankRef = item.bankReferenceId();
                    break;
                }
            }
            
            out.add(new NormalizedTxn(
                    first.accountLast4(),
                    first.occurredAt(),
                    first.direction(),
                    first.amount(),
                    c,
                    bestMerchant,
                    ids,
                    bankRef
            ));
        }

        List<in.simplifymoney.ledgersync.model.Discrepancy> existingDisc = store.discrepancies();
        java.util.Set<String> existingDiscKeys = new java.util.HashSet<>();
        for (in.simplifymoney.ledgersync.model.Discrepancy d : existingDisc) {
            existingDiscKeys.add(d.accountLast4() + "|" + d.occurredAt() + "|"
                    + d.amount() + "|" + d.note());
        }

        Map<String, List<NormalizedTxn>> byAcct = new java.util.LinkedHashMap<>();
        for (NormalizedTxn t : out) {
            byAcct.computeIfAbsent(t.accountLast4(), k -> new ArrayList<>()).add(t);
        }
        
        for (Map.Entry<String, List<NormalizedTxn>> entry : byAcct.entrySet()) {
            String acct = entry.getKey();
            if (accountsWithoutReliableBalances.contains(acct)) continue;
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
                            String sourceEvidence = String.join(",", t.sourceMessageIds());
                            in.simplifymoney.ledgersync.model.Discrepancy d = new in.simplifymoney.ledgersync.model.Discrepancy(
                                    acct, t.occurredAt(), diff,
                                    "type=balance-gap; sources=" + sourceEvidence
                                            + "; ledger computed " + expected.toPlainString()
                                            + " but bank reported " + statedBal.toPlainString()
                            );
                            String dKey = d.accountLast4() + "|" + d.occurredAt() + "|"
                                    + d.amount() + "|" + d.note();
                            if (!existingDiscKeys.contains(dKey)) {
                                store.save(d);
                                existingDiscKeys.add(dKey);
                            }
                            
                        }
                    }
                    lastBalance = statedBal;
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
                    t1.amount().compareTo(t2.amount()) == 0 &&
                    t1.direction() != t2.direction() &&
                    hasTransferEvidence(t1, t2)) {
                    
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

    private boolean canGroup(RawMessage rawP, List<ParsedTxn> group, Map<String, RawMessage> rawMap) {
        if (rawP == null) return false;
        for (ParsedTxn item : group) {
            RawMessage rawItem = rawMap.get(item.sourceMessageId());
            if (rawItem == null) continue;
            if (rawP.body().equals(rawItem.body())) {
                return true;
            }
            boolean crossChannel = !rawP.channel().equals(rawItem.channel());
            if (crossChannel && ("sms".equals(rawP.channel()) || "sms".equals(rawItem.channel()))
                    && ("email".equals(rawP.channel()) || "email".equals(rawItem.channel()))) {
                return true;
            }
        }
        return false;
    }

    private String chooseBestMerchant(List<ParsedTxn> group, Map<String, RawMessage> rawMap) {
        String best = null;
        boolean hasEmail = false;
        for (ParsedTxn p : group) {
            RawMessage raw = rawMap.get(p.sourceMessageId());
            if (p.merchant() == null) continue;
            if (best == null) {
                best = p.merchant();
                hasEmail = (raw != null && "email".equals(raw.channel()));
                continue;
            }
            boolean candidateIsEmail = (raw != null && "email".equals(raw.channel()));
            // 1. Prefer email over SMS
            if (candidateIsEmail && !hasEmail) {
                best = p.merchant();
                hasEmail = true;
            } else if (candidateIsEmail == hasEmail) {
                // Same channel priority — prefer longer description
                if (p.merchant().length() > best.length()) {
                    best = p.merchant();
                } else if (p.merchant().length() == best.length()) {
                    // Tie-break: lexicographically smaller for determinism
                    if (p.merchant().compareTo(best) < 0) {
                        best = p.merchant();
                    }
                }
            }
        }
        return best;
    }

    private Category determineCategory(List<ParsedTxn> group, String selectedMerchant) {
        ParsedTxn first = group.get(0);
        if (first.direction() == Direction.DEBIT) {
            boolean upiEvidence = isUpi(selectedMerchant);
            for (ParsedTxn p : group) {
                upiEvidence = upiEvidence || isUpi(p.merchant());
            }
            if (first.amount().compareTo(new java.math.BigDecimal("100")) <= 0 && upiEvidence) {
                return Category.MICRO;
            }
            return Category.SPEND;
        } else {
            return Category.INCOME;
        }
    }

    private boolean isUpi(String merchant) {
        return merchant != null && merchant.toUpperCase().contains("UPI");
    }

    private static boolean hasTransferEvidence(NormalizedTxn left, NormalizedTxn right) {
        String leftMerchant = normalizeTransferMerchant(left.merchant());
        String rightMerchant = normalizeTransferMerchant(right.merchant());
        return !leftMerchant.isBlank()
                && leftMerchant.equals(rightMerchant)
                && (leftMerchant.contains("IMPS") || leftMerchant.contains("NEFT")
                    || leftMerchant.contains("UPI") || leftMerchant.contains("TRANSFER"));
    }

    private static String normalizeTransferMerchant(String merchant) {
        if (merchant == null) return "";
        String normalized = merchant.toUpperCase(java.util.Locale.ROOT)
                .replace("TO:", "")
                .replace("FROM:", "")
                .replaceAll("\\s+", " ")
                .trim();
        if (normalized.startsWith("SENT ")) normalized = normalized.substring(5).trim();
        if (normalized.startsWith("RECEIVED ")) normalized = normalized.substring(9).trim();
        return normalized;
    }

    private static java.util.Set<String> configuredAccountsWithoutReliableBalances() {
        String raw = System.getProperty(BALANCE_EXCLUSION_PROPERTY, DEFAULT_BALANCE_EXCLUSIONS);
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        for (String item : raw.split(",")) {
            String account = item.trim();
            if (!account.isBlank()) {
                out.add(account);
            }
        }
        return out;
    }

    /**
     * Returns the lower-case hex SHA-256 digest of the given string.
     * Kept package-visible for any deterministic ID tests that need it.
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
                t.amount(), c, t.merchant(), t.sourceMessageIds(), t.bankReferenceId());
    }

    public record Stats(int messagesRead, int transactionsWritten, int messagesSkipped,
                        int failedWrites, List<SkipDetail> skipDetails,
                        int malformedRecords, List<MalformedRecord> malformedDetails) {}
    
    public record SkipDetail(String messageId, String reason) {}

    public record MalformedRecord(int lineNumber, String reason, String rawRecord) {}

    public record CorpusReadResult(int recordsRead, List<RawMessage> validRecords,
                                   List<MalformedRecord> malformedDetails) {
        public int malformedRecords() {
            return malformedDetails.size();
        }
    }
}
