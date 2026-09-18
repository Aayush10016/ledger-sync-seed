import java.nio.file.Path;
import java.util.List;
import in.simplifymoney.ledgersync.ingest.CorpusReader;
import in.simplifymoney.ledgersync.ingest.IngestService;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.model.RawMessage;
import in.simplifymoney.ledgersync.parse.Parsers;
public class TestMerge2 {
    public static void main(String[] args) throws Exception {
        Parsers parsers = new Parsers();
        List<RawMessage> raw = IngestService.readCorpusDetailed(Path.of("fixtures/corpus-a.jsonl")).validRecords();
        List<NormalizedTxn> txns = IngestService.normalize(raw, parsers);
        for (NormalizedTxn t : txns) {
            if ("9075".equals(t.accountLast4())) {
                System.out.println(t.occurredAt() + " | " + t.amount() + " | " + t.merchant() + " | " + t.category() + " | " + t.sourceMessageIds());
            }
        }
    }
}
