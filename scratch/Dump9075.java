import in.simplifymoney.ledgersync.store.SqlLedgerStore;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import java.nio.file.Path;
import java.util.List;

public class Dump9075 {
    public static void main(String[] args) throws Exception {
        SqlLedgerStore store = new SqlLedgerStore(Path.of(System.getProperty("user.dir"), "db").toAbsolutePath());
        List<NormalizedTxn> txns = store.all();
        for (NormalizedTxn t : txns) {
            if ("9075".equals(t.accountLast4()) && t.occurredAt().toString().contains("2026-07")) {
                System.out.println(t.occurredAt() + " | " + t.amount() + " | " + t.sourceMessageIds() + " | " + t.category());
            }
        }
        store.close();
    }
}
