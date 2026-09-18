import java.nio.file.Path;
import java.util.List;
import in.simplifymoney.ledgersync.ingest.CorpusReader;
import in.simplifymoney.ledgersync.ingest.IngestService;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.model.RawMessage;
import in.simplifymoney.ledgersync.parse.Parsers;
public class TestMerge {
    public static void main(String[] args) throws Exception {
        Parsers parsers = new Parsers();
        List<RawMessage> raw = CorpusReader.readValid(Path.of("fixtures/corpus-a.jsonl"));
        List<NormalizedTxn> txns = IngestService.normalize(raw, parsers);
        int count9075 = 0;
        int balanceGaps = 0;
        for (NormalizedTxn t : txns) {
            if ("9075".equals(t.accountLast4())) {
                count9075++;
                if (t.merchant().equals("BALANCE CORRECTION")) {
                    balanceGaps++;
                    System.out.println("Balance Gap: " + t.amount() + " at " + t.occurredAt());
                } else if (t.amount().compareTo(new java.math.BigDecimal("2499.50")) == 0) {
                    System.out.println("2499.50 txn at " + t.occurredAt() + " sources: " + t.sourceMessageIds());
                }
            }
        }
        System.out.println("Normalized 9075 txns: " + count9075 + " Gaps: " + balanceGaps);
    }
}
