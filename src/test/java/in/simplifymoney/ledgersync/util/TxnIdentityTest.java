package in.simplifymoney.ledgersync.util;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class TxnIdentityTest {

    @Test
    public void identicalBankReferenceOnDifferentAccountsProducesDistinctIdentities() {
        NormalizedTxn t1 = new NormalizedTxn(
                "1234", OffsetDateTime.now(), Direction.DEBIT, new BigDecimal("10.00"),
                Category.SPEND, "MERCH", List.of("msg1"), "REF999"
        );
        NormalizedTxn t2 = new NormalizedTxn(
                "5678", OffsetDateTime.now(), Direction.DEBIT, new BigDecimal("10.00"),
                Category.SPEND, "MERCH", List.of("msg2"), "REF999"
        );
        
        String id1 = TxnIdentity.getId(t1);
        String id2 = TxnIdentity.getId(t2);
        
        assertNotEquals(id1, id2, "Identical bank references on different accounts should have distinct identities");
    }
}
