package in.simplifymoney.ledgersync.model;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record Discrepancy(
        String accountLast4,
        OffsetDateTime occurredAt,
        BigDecimal amount,
        String note
) {
}
