import in.simplifymoney.ledgersync.ingest.IngestService;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.parse.Parsers;
import java.nio.file.Path;
import java.util.List;

public class Find2499Txn {
    public static void main(String[] args) throws Exception {
        Parsers parsers = new Parsers();
        List<in.simplifymoney.ledgersync.model.RawMessage> raw = 
            in.simplifymoney.ledgersync.ingest.CorpusReader.readValid(Path.of("fixtures/corpus-a.jsonl"));
        
        List<NormalizedTxn> txns = IngestService.normalize(raw, parsers);
        for (NormalizedTxn t : txns) {
            if (t.amount().compareTo(new java.math.BigDecimal("2499.50")) == 0) {
                System.out.println("FOUND: " + t);
            }
        }
    }
}
