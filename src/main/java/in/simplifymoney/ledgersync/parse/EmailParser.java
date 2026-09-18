package in.simplifymoney.ledgersync.parse;

import in.simplifymoney.ledgersync.model.RawMessage;
import java.util.Optional;

/**
 * Bank transaction alert emails.
 *
 * Not written yet. The corpus contains them and they are currently all dropped.
 */
public final class EmailParser implements MessageParser {

    @Override
    public boolean supports(RawMessage m) {
        return "email".equals(m.channel());
    }

    private static final java.util.regex.Pattern ACCT = java.util.regex.Pattern.compile("Your account ending (\\d{4}) has been (debited|credited)");
    private static final java.util.regex.Pattern MERCHANT = java.util.regex.Pattern.compile("Merchant / Remarks: (.+)");
    private static final java.util.regex.Pattern DATE = java.util.regex.Pattern.compile("Date: [A-Za-z]{3}, (\\d{2} [A-Za-z]{3} \\d{4} \\d{2}:\\d{2}:\\d{2} \\+\\d{4})");
    private static final java.util.regex.Pattern REF = java.util.regex.Pattern.compile("Transaction reference: (\\S+)");

    @Override
    public Optional<ParsedTxn> parse(RawMessage m) {
        String body = m.body();
        java.util.regex.Matcher a = ACCT.matcher(body);
        if (!a.find()) return Optional.empty();
        
        String acct = a.group(1);
        in.simplifymoney.ledgersync.model.Direction dir = "debited".equals(a.group(2)) 
                ? in.simplifymoney.ledgersync.model.Direction.DEBIT : in.simplifymoney.ledgersync.model.Direction.CREDIT;
                
        java.util.regex.Matcher merch = MERCHANT.matcher(body);
        String merchant = merch.find() ? merch.group(1).trim() : "UNKNOWN";
        
        java.math.BigDecimal amount = Amounts.first(body);
        if (amount == null) return Optional.empty();
        
        java.util.regex.Matcher d = DATE.matcher(body);
        if (!d.find()) return Optional.empty();
        
        java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter.ofPattern("dd MMM yyyy HH:mm:ss Z");
        java.time.OffsetDateTime dt = java.time.OffsetDateTime.parse(d.group(1), 
                java.time.format.DateTimeFormatter.ofPattern("dd MMM yyyy HH:mm:ss Z"));
                
        java.util.regex.Matcher r = REF.matcher(body);
        String bankRef = r.find() ? Parsers.normalizeBankReference(r.group(1)) : null;
                
        return Optional.of(new ParsedTxn(acct, dt, dir, amount, merchant, null, m.messageId(), bankRef));
    }
}
