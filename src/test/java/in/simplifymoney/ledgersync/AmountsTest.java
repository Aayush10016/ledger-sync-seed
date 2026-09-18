package in.simplifymoney.ledgersync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import in.simplifymoney.ledgersync.ingest.IngestService;
import in.simplifymoney.ledgersync.model.RawMessage;
import in.simplifymoney.ledgersync.parse.Amounts;
import in.simplifymoney.ledgersync.parse.Parsers;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Amount extraction.
 *
 * This suite is green. It has been green since it was written.
 */
class AmountsTest {

    private static final Pattern LEGACY_DECIMAL_ONLY_AMOUNT =
            Pattern.compile("(?:Rs\\.?|INR)\\s*([0-9,]+\\.[0-9]{2})");
    private static final Pattern FIXED_AMOUNT =
            Pattern.compile("(?:Rs\\.?|INR)\\s*([0-9,]+(?:\\.[0-9]{2})?)");
    private static final Pattern BALANCE_OR_LIMIT = Pattern.compile(
            "(?:Avl\\s*Bal|Available\\s*Balance|BalAvl|Avl\\s*Limit)\\s*:?\\s*"
                    + "(?:Rs\\.?|INR)\\s*([0-9,]+\\.[0-9]{2})",
            Pattern.CASE_INSENSITIVE);

    @Test
    void readsRupeesWithADot() {
        assertEquals(new BigDecimal("2499.50"),
                Amounts.first("Rs.2,499.50 debited from a/c **4821 on 04-07-26 at "
                        + "20:24 to AMAZON PAY. Avl Bal: Rs.89,032.61."));
    }

    @Test
    void readsInrPrefix() {
        assertEquals(new BigDecimal("333.33"),
                Amounts.first("Dear Customer, Acct XX9075 is debited with INR 333.33 "
                        + "on 04/07/2026 07:54. Info: SWIGGY. Avl Bal Rs.49,857.25"));
    }

    @Test
    void readsThousandsSeparators() {
        assertEquals(new BigDecimal("45000.00"),
                Amounts.first("Rs.45,000.00 credited to a/c **4821 on 01-07-26 at "
                        + "09:02 by SALARY CREDIT. Avl Bal: Rs.93,211.40"));
    }

    @Test
    void readsTheStatedBalance() {
        assertEquals(new BigDecimal("89032.61"),
                Amounts.statedBalance("Rs.2,499.50 debited from a/c **4821 on "
                        + "04-07-26 at 20:24 to AMAZON PAY. Avl Bal: Rs.89,032.61."));
    }

    @Test
    void ignoresAMessageWithNoAmountAtAll() {
        assertEquals(null, Amounts.first("Your Swiggy order is on the way!"));
    }

    @Test
    void readsIntegerAmountsWithoutDecimals() {
        assertEquals(new BigDecimal("5.00"),
                Amounts.first("Rs.5 debited from a/c **4821 on 04-07-26 at 07:19 to UPI/WATER CAN. Avl Bal: Rs.92,213.10."));
    }

    @Test
    void readsTransactionAmountBeforeLaterBalance() {
        String body = "Rs.5 debited from a/c **4821 on 04-07-26 at 11:54 "
                + "to UPI/WATER CAN. Avl Bal: Rs.92,213.10.";

        assertEquals(new BigDecimal("5.00"), Amounts.first(body));
        assertEquals(new BigDecimal("92213.10"), Amounts.statedBalance(body));
    }

    @Test
    void readsAvailableCardLimitSeparately() {
        String body = "Rs 1,249.99 spent on HDFC Bank Card x3310 at BLINKIT "
                + "on 03-07-26 11:51. Avl Limit: Rs.196,250.03.";

        assertEquals(new BigDecimal("1249.99"), Amounts.first(body));
        assertEquals(new BigDecimal("196250.03"), Amounts.statedBalance(body));
    }

    @Test
    void parserIgnoresOtpMessagesEvenWhenTheyContainARupeeAmount() {
        RawMessage otp = new RawMessage("otp-1", "sms", "AD-HDFCBK-S",
                java.time.OffsetDateTime.parse("2026-08-15T11:04:04+05:30"),
                "device", "541027 is your OTP for txn of Rs.6214.00 on HDFC Bank Card.");

        assertFalse(new Parsers().parse(otp).isPresent());
    }

    @Test
    void ignoresUnrelatedNumbersWithoutRupeePrefixes() {
        assertEquals(null, Amounts.first("Your order arrives by 7 PM. Track: dlhvry.in/9856586"));
    }

    @Test
    void corpusABlastRadiusForLegacyWholeRupeeBugIsExact() throws IOException {
        var affected = IngestService.readCorpus(Path.of("fixtures", "corpus-a.jsonl")).stream()
                .filter(AmountsTest::wouldHaveUsedBalanceAsAmountBeforeFix)
                .map(RawMessage::messageId)
                .toList();

        assertEquals(38, affected.size());
        assertTrue(affected.contains("m-00022-2f118b"));
    }

    private static boolean wouldHaveUsedBalanceAsAmountBeforeFix(RawMessage message) {
        String body = message.body();
        var fixed = FIXED_AMOUNT.matcher(body);
        var legacy = LEGACY_DECIMAL_ONLY_AMOUNT.matcher(body);
        var balance = BALANCE_OR_LIMIT.matcher(body);
        return fixed.find()
                && balance.find()
                && legacy.find()
                && fixed.start() < balance.start()
                && !fixed.group(1).contains(".")
                && legacy.start() >= balance.start();
    }
}
