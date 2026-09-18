import java.nio.file.Path;
import java.util.List;
import in.simplifymoney.ledgersync.ingest.CorpusReader;
import in.simplifymoney.ledgersync.ingest.IngestService;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.model.RawMessage;
import in.simplifymoney.ledgersync.parse.Parsers;
public class TestTrace {
    public static void main(String[] args) throws Exception {
        Parsers parsers = new Parsers();
        List<RawMessage> raw = IngestService.readCorpusDetailed(Path.of("fixtures/corpus-a.jsonl")).validRecords();
        List<NormalizedTxn> txns = IngestService.normalize(raw, parsers);
        
        java.math.BigDecimal lastBalance = null;
        java.math.BigDecimal sumSinceLastBalance = java.math.BigDecimal.ZERO;
        
        for (NormalizedTxn t : txns) {
            if ("9075".equals(t.accountLast4())) {
                java.math.BigDecimal amt = t.direction() == in.simplifymoney.ledgersync.model.Direction.DEBIT ? t.amount().negate() : t.amount();
                if (t.category() != in.simplifymoney.ledgersync.model.Category.TRANSFER) {
                    sumSinceLastBalance = sumSinceLastBalance.add(amt);
                }
                
                java.math.BigDecimal statedBal = null;
                if (t.merchant().equals("BALANCE CORRECTION")) continue; // skip generated ones
                
                for (RawMessage m : raw) {
                    if (m.messageId().equals(t.sourceMessageIds().get(0))) {
                        statedBal = in.simplifymoney.ledgersync.parse.Amounts.statedBalance(m.body());
                        break;
                    }
                }
                
                System.out.println(t.occurredAt() + " | Amt: " + t.amount() + " | Sum: " + sumSinceLastBalance + " | Stated: " + statedBal + " | Msg: " + t.sourceMessageIds().get(0));
                
                if (statedBal != null) {
                    if (lastBalance != null) {
                        java.math.BigDecimal expected = lastBalance.add(sumSinceLastBalance);
                        if (expected.compareTo(statedBal) != 0) {
                            System.out.println("DISCREPANCY: expected " + expected + " but got " + statedBal);
                        }
                    }
                    lastBalance = statedBal;
                    sumSinceLastBalance = java.math.BigDecimal.ZERO;
                }
            }
        }
    }
}
