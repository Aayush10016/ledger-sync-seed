import in.simplifymoney.ledgersync.ingest.IngestService;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.model.RawMessage;
import in.simplifymoney.ledgersync.parse.Parsers;
import java.util.List;
import java.time.OffsetDateTime;

public class DebugMerge {
    public static void main(String[] args) throws Exception {
        Parsers parsers = new Parsers();
        RawMessage m1 = new RawMessage("m-00207-62afa0", "sms", "VM-ICICIB-T", OffsetDateTime.now(), "dev1",
            "ICICI Bank Acct XX9075 Dr INR 76.49 on 30-Jul-2026 13:56; NETFLIX ref no 491902275985. BalAvl Rs 48,093.52");
        RawMessage m2 = new RawMessage("m-00428-b5eecf", "sms", "VM-ICICIB-T", OffsetDateTime.now(), "dev1",
            "ICICI Bank Acct XX9075 Dr INR 76.49 on 30-Jul-2026 13:56; NETFLIX ref no 491902275985. BalAvl Rs 48,093.52");
        
        List<NormalizedTxn> out = IngestService.normalize(List.of(m1, m2), parsers);
        System.out.println("Output size: " + out.size());
        for (NormalizedTxn t : out) {
            System.out.println(t.sourceMessageIds());
        }
    }
}
