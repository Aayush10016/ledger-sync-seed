package in.simplifymoney.ledgersync.scratch;

import in.simplifymoney.ledgersync.model.*;
import in.simplifymoney.ledgersync.store.*;
import java.nio.file.*;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

public class TestSql {
    public static void main(String[] args) throws Exception {
        Path dbFile = Files.createTempDirectory("lifecycle-db").resolve("db");
        System.out.println("DB File: " + dbFile);
        SqlLedgerStore sqlStore = new SqlLedgerStore(dbFile);
        sqlStore.migrate(Path.of("db", "migration"));
        NormalizedTxn original = new NormalizedTxn(
                "9999", OffsetDateTime.parse("2024-06-01T10:00:00Z"),
                Direction.DEBIT, new BigDecimal("75.00"),
                Category.SPEND, "SWIGGY", List.of("lifecycle-1"));
        sqlStore.save(original);
        System.out.println("Size: " + sqlStore.all().size());
        sqlStore.close();
    }
}
