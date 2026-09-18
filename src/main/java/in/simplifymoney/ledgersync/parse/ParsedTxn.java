package in.simplifymoney.ledgersync.parse;

import in.simplifymoney.ledgersync.model.Direction;
import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * What a single message says, before anything has been decided about it.
 *
 * statedBalance is the account balance the bank quoted in the message, when it
 * quoted one. It may be null.
 *
 * bankReferenceId is the explicit transaction reference from the bank (e.g. UPI Ref, NEFT ID).
 * It may be null if the message does not contain a reference.
 */
public record ParsedTxn(
        String accountLast4,
        OffsetDateTime occurredAt,
        Direction direction,
        BigDecimal amount,
        String merchant,
        BigDecimal statedBalance,
        String sourceMessageId,
        String bankReferenceId) {
}
