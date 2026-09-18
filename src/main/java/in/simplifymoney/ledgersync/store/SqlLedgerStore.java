package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The store this service has used since it was written: a single relational
 * table, reached over plain JDBC.
 *
 * The driver is a runtime dependency (see build.gradle) - this class compiles
 * against the JDK alone.
 */
public final class SqlLedgerStore implements LedgerStore, AutoCloseable {

    private static final String URL_PREFIX = "jdbc:h2:";
    private final Connection conn;

    public SqlLedgerStore(Path dbFile) {
        try {
            this.conn = DriverManager.getConnection(
                    URL_PREFIX + dbFile.toAbsolutePath(), "sa", "");
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "could not open the ledger database at " + dbFile
                            + " (is the H2 driver on the runtime classpath?)", e);
        }
    }

    /** Applies every db/migration/V*.sql in filename order. */
    public void migrate(Path migrationDir) {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS schema_history ("
                    + "  filename VARCHAR(200) PRIMARY KEY,"
                    + "  applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)");

            List<Path> files;
            try (var s = Files.list(migrationDir)) {
                files = s.filter(p -> p.getFileName().toString().endsWith(".sql")).sorted().toList();
            }
            for (Path f : files) {
                String name = f.getFileName().toString();
                try (PreparedStatement q = conn.prepareStatement(
                        "SELECT 1 FROM schema_history WHERE filename = ?")) {
                    q.setString(1, name);
                    try (ResultSet rs = q.executeQuery()) {
                        if (rs.next()) continue;
                    }
                }
                String sql;
                sql = Files.readString(f, java.nio.charset.StandardCharsets.UTF_8);
                if (sql.startsWith("\uFEFF")) {
                    sql = sql.substring(1);
                }
                for (String stmt : sql.split(";")) {
                    if (!stmt.isBlank()) st.execute(stmt);
                }
                try (PreparedStatement ins = conn.prepareStatement(
                        "INSERT INTO schema_history(filename) VALUES (?)")) {
                    ins.setString(1, name);
                    ins.executeUpdate();
                }
                System.out.println("applied " + name);
            }

            // Java-based backfill for txn_id
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT account_last4, occurred_at, direction, amount, category, merchant, source_message_ids FROM ledger WHERE txn_id IS NULL")) {
                
                List<NormalizedTxn> toUpdate = new ArrayList<>();
                while (rs.next()) {
                    toUpdate.add(new NormalizedTxn(
                        rs.getString(1),
                        OffsetDateTime.parse(rs.getString(2)),
                        Direction.valueOf(rs.getString(3)),
                        rs.getBigDecimal(4).setScale(2),
                        Category.valueOf(rs.getString(5)),
                        rs.getString(6),
                        Arrays.stream(rs.getString(7).split(",")).filter(x -> !x.isBlank()).sorted().toList()
                    ));
                }
                
                if (!toUpdate.isEmpty()) {
                    java.util.Map<String, NormalizedTxn> deduplicated = new java.util.HashMap<>();
                    for (NormalizedTxn t : toUpdate) {
                        deduplicated.merge(in.simplifymoney.ledgersync.util.TxnIdentity.getId(t), t, (e, i) -> {
                            java.util.Set<String> ids = new java.util.HashSet<>(e.sourceMessageIds());
                            ids.addAll(i.sourceMessageIds());
                            java.util.List<String> sorted = new java.util.ArrayList<>(ids);
                            java.util.Collections.sort(sorted);
                            return new NormalizedTxn(e.accountLast4(), e.occurredAt(), e.direction(), e.amount(), e.category(), e.merchant(), sorted);
                        });
                    }
                    System.out.println("Backfilling txn_id for " + deduplicated.size() + " deduplicated legacy records...");
                    
                    try (Statement del = conn.createStatement()) {
                        del.execute("DELETE FROM ledger WHERE txn_id IS NULL");
                    }
                    
                    for (NormalizedTxn t : deduplicated.values()) {
                        save(t);
                    }
                }
            }
            
        } catch (Exception e) {
            throw new IllegalStateException("migration failed", e);
        }
    }

    @Override
    public void save(NormalizedTxn t) {
        boolean autoCommit = true;
        try {
            autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);

            // Idempotency and concurrency safety: Use H2's MERGE INTO to atomically insert or update based on a stable, unique transaction identity (txn_id).
            String txnId = in.simplifymoney.ledgersync.util.TxnIdentity.getId(t);
            
            try (PreparedStatement ps = conn.prepareStatement(
                    "MERGE INTO ledger(account_last4, occurred_at, direction, amount,"
                            + " category, merchant, source_message_ids, txn_id) KEY(txn_id)"
                            + " VALUES (?,?,?,?,?,?,?,?)")) {
                ps.setString(1, t.accountLast4());
                ps.setString(2, t.occurredAt().toString());
                ps.setString(3, t.direction().name());
                ps.setBigDecimal(4, t.amount());
                ps.setString(5, t.category().name());
                ps.setString(6, t.merchant());
                ps.setString(7, String.join(",", t.sourceMessageIds()));
                ps.setString(8, txnId);
                ps.executeUpdate();
            }

            conn.commit();
        } catch (SQLException e) {
            try { conn.rollback(); } catch (SQLException ignored) {}
            throw new IllegalStateException("could not save " + t, e);
        } finally {
            try { conn.setAutoCommit(autoCommit); } catch (SQLException ignored) {}
        }
    }

    @Override
    public List<NormalizedTxn> all() {
        List<NormalizedTxn> out = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT account_last4, occurred_at, direction, amount, category,"
                             + " merchant, source_message_ids FROM ledger ORDER BY occurred_at")) {
            while (rs.next()) {
                out.add(new NormalizedTxn(
                        rs.getString(1),
                        OffsetDateTime.parse(rs.getString(2)),
                        Direction.valueOf(rs.getString(3)),
                        rs.getBigDecimal(4).setScale(2),
                        Category.valueOf(rs.getString(5)),
                        rs.getString(6),
                        Arrays.stream(rs.getString(7).split(","))
                                .filter(s -> !s.isBlank()).sorted().toList()));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("could not read the ledger", e);
        }
        return out;
    }

    @Override
    public long count() {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM ledger")) {
            return rs.next() ? rs.getLong(1) : 0L;
        } catch (SQLException e) {
            throw new IllegalStateException("could not count the ledger", e);
        }
    }

    @Override
    public void save(in.simplifymoney.ledgersync.model.Discrepancy d) {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO discrepancies(account_last4, occurred_at, amount, note) VALUES (?,?,?,?)")) {
            ps.setString(1, d.accountLast4());
            ps.setString(2, d.occurredAt().toString());
            ps.setBigDecimal(3, d.amount());
            ps.setString(4, d.note());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("could not save discrepancy " + d, e);
        }
    }

    @Override
    public List<in.simplifymoney.ledgersync.model.Discrepancy> discrepancies() {
        List<in.simplifymoney.ledgersync.model.Discrepancy> out = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT account_last4, occurred_at, amount, note FROM discrepancies ORDER BY occurred_at")) {
            while (rs.next()) {
                out.add(new in.simplifymoney.ledgersync.model.Discrepancy(
                        rs.getString(1),
                        OffsetDateTime.parse(rs.getString(2)),
                        rs.getBigDecimal(3).setScale(2),
                        rs.getString(4)
                ));
            }
        } catch (SQLException e) {
            if (e.getMessage() != null && (e.getMessage().contains("Table") && e.getMessage().contains("not found"))) {
                return new ArrayList<>(); // Expected when table doesn't exist yet
            }
            throw new IllegalStateException("could not query discrepancies", e);
        }
        return out;
    }

    public BigDecimal sumAmounts() {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT SUM(amount) FROM ledger")) {
            return rs.next() && rs.getBigDecimal(1) != null
                    ? rs.getBigDecimal(1).setScale(2) : BigDecimal.ZERO.setScale(2);
        } catch (SQLException e) {
            throw new IllegalStateException("could not total the ledger", e);
        }
    }

    @Override
    public void close() {
        try { conn.close(); } catch (SQLException ignored) { }
    }
}
