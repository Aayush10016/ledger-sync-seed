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
    private final String dbUrl;

    public SqlLedgerStore(Path dbFile) {
        this.dbUrl = URL_PREFIX + dbFile.toAbsolutePath();
        // Test connection
        try (Connection conn = DriverManager.getConnection(this.dbUrl, "sa", "")) {
            if (conn == null) throw new SQLException("Null connection");
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "could not open the ledger database at " + dbFile
                            + " (is the H2 driver on the runtime classpath?)", e);
        }
    }

    /** Applies every db/migration/V*.sql in filename order. */
    public void migrate(Path migrationDir) {
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "")) {
            boolean autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (Statement st = conn.createStatement()) {
                    st.execute("CREATE TABLE IF NOT EXISTS schema_history ("
                            + "  filename VARCHAR(200) PRIMARY KEY,"
                            + "  applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)");
                }

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
                    String sql = Files.readString(f, java.nio.charset.StandardCharsets.UTF_8);
                    if (sql.startsWith("\uFEFF")) {
                        sql = sql.substring(1);
                    }
                    try (Statement st = conn.createStatement()) {
                        for (String stmt : sql.split(";")) {
                            if (!stmt.isBlank()) st.execute(stmt);
                        }
                    }
                    try (PreparedStatement ins = conn.prepareStatement(
                            "INSERT INTO schema_history(filename) VALUES (?)")) {
                        ins.setString(1, name);
                        ins.executeUpdate();
                    }
                    System.out.println("applied " + name);
                }

                // Java-based backfill for txn_id
                List<NormalizedTxn> toUpdate = new ArrayList<>();
                try (Statement s = conn.createStatement();
                     ResultSet rs = s.executeQuery("SELECT account_last4, occurred_at, direction, amount, category, merchant, source_message_ids FROM ledger WHERE txn_id IS NULL")) {
                    
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
                }
                    
                if (!toUpdate.isEmpty()) {
                    java.util.Map<String, NormalizedTxn> deduplicated = new java.util.LinkedHashMap<>();
                    for (NormalizedTxn t : toUpdate) {
                        deduplicated.merge(in.simplifymoney.ledgersync.util.TxnIdentity.getId(t), t, (e, i) -> {
                            java.util.Set<String> ids = new java.util.HashSet<>(e.sourceMessageIds());
                            ids.addAll(i.sourceMessageIds());
                            java.util.List<String> sorted = new java.util.ArrayList<>(ids);
                            java.util.Collections.sort(sorted);
                            return new NormalizedTxn(e.accountLast4(), e.occurredAt(), e.direction(), e.amount(), e.category(), e.merchant(), sorted);
                        });
                    }
                    System.out.println("Backfilling txn_id for " + deduplicated.size() + " legacy records...");
                    
                    try (Statement del = conn.createStatement()) {
                        del.execute("DELETE FROM ledger WHERE txn_id IS NULL");
                    }
                    
                    for (NormalizedTxn t : deduplicated.values()) {
                        saveInternal(conn, t);
                    }
                }

                rebuildSourceIndex(conn);
                
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw new IllegalStateException("migration failed", e);
            } finally {
                conn.setAutoCommit(autoCommit);
            }
        } catch (Exception e) {
            throw new IllegalStateException("database connection failed during migration", e);
        }
    }

    private void saveInternal(Connection conn, NormalizedTxn t) throws SQLException {
        String txnId = findTxnIdByAnySource(conn, t.sourceMessageIds());
        NormalizedTxn toSave = t;

        if (txnId == null) {
            txnId = in.simplifymoney.ledgersync.util.TxnIdentity.getId(t);
        } else {
            NormalizedTxn existing = loadByTxnId(conn, txnId);
            if (existing != null) {
                validateCompatibleIdentity(existing, t);
                java.util.Set<String> mergedIds = new java.util.TreeSet<>(existing.sourceMessageIds());
                mergedIds.addAll(t.sourceMessageIds());
                toSave = new NormalizedTxn(
                        t.accountLast4(),
                        t.occurredAt(),
                        t.direction(),
                        t.amount(),
                        t.category(),
                        t.merchant(),
                        new java.util.ArrayList<>(mergedIds));
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "MERGE INTO ledger(account_last4, occurred_at, direction, amount,"
                        + " category, merchant, source_message_ids, txn_id) KEY(txn_id)"
                        + " VALUES (?,?,?,?,?,?,?,?)")) {
            ps.setString(1, toSave.accountLast4());
            ps.setString(2, toSave.occurredAt().toString());
            ps.setString(3, toSave.direction().name());
            ps.setBigDecimal(4, toSave.amount());
            ps.setString(5, toSave.category().name());
            ps.setString(6, toSave.merchant());
            ps.setString(7, String.join(",", toSave.sourceMessageIds()));
            ps.setString(8, txnId);
            ps.executeUpdate();
        }

        for (String sourceId : toSave.sourceMessageIds()) {
            upsertSourceMapping(conn, sourceId, txnId);
        }
    }

    private String findTxnIdByAnySource(Connection conn, List<String> sourceIds) throws SQLException {
        String found = null;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT txn_id FROM ledger_sources WHERE source_message_id = ?")) {
            for (String sourceId : sourceIds) {
                ps.setString(1, sourceId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) continue;
                    String current = rs.getString(1);
                    if (found == null) {
                        found = current;
                    } else if (!found.equals(current)) {
                        throw new SQLException("source messages map to conflicting transactions: "
                                + sourceIds + " -> " + found + " and " + current);
                    }
                }
            }
        }
        return found;
    }

    private NormalizedTxn loadByTxnId(Connection conn, String txnId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT account_last4, occurred_at, direction, amount, category,"
                        + " merchant, source_message_ids FROM ledger WHERE txn_id = ?")) {
            ps.setString(1, txnId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new NormalizedTxn(
                        rs.getString(1),
                        OffsetDateTime.parse(rs.getString(2)),
                        Direction.valueOf(rs.getString(3)),
                        rs.getBigDecimal(4).setScale(2),
                        Category.valueOf(rs.getString(5)),
                        rs.getString(6),
                        Arrays.stream(rs.getString(7).split(","))
                                .filter(s -> !s.isBlank()).sorted().toList());
            }
        }
    }

    private void validateCompatibleIdentity(NormalizedTxn existing, NormalizedTxn incoming) throws SQLException {
        if (!existing.accountLast4().equals(incoming.accountLast4())
                || existing.occurredAt().toEpochSecond() != incoming.occurredAt().toEpochSecond()
                || existing.direction() != incoming.direction()
                || existing.amount().compareTo(incoming.amount()) != 0
                || existing.category() != incoming.category()
                || !java.util.Objects.equals(existing.merchant(), incoming.merchant())) {
            throw new SQLException("source message already belongs to an incompatible transaction");
        }
    }

    private void upsertSourceMapping(Connection conn, String sourceId, String txnId) throws SQLException {
        try (PreparedStatement existing = conn.prepareStatement(
                "SELECT txn_id FROM ledger_sources WHERE source_message_id = ?")) {
            existing.setString(1, sourceId);
            try (ResultSet rs = existing.executeQuery()) {
                if (rs.next()) {
                    String current = rs.getString(1);
                    if (!txnId.equals(current)) {
                        throw new SQLException("source message " + sourceId
                                + " already maps to transaction " + current);
                    }
                    return;
                }
            }
        }

        try (PreparedStatement insert = conn.prepareStatement(
                "INSERT INTO ledger_sources(source_message_id, txn_id) VALUES (?, ?)")) {
            insert.setString(1, sourceId);
            insert.setString(2, txnId);
            insert.executeUpdate();
        } catch (SQLException e) {
            // "23" is the standard SQLState class for integrity constraint violations.
            if (e.getSQLState() != null && e.getSQLState().startsWith("23")) {
                try (PreparedStatement existing = conn.prepareStatement(
                        "SELECT txn_id FROM ledger_sources WHERE source_message_id = ?")) {
                    existing.setString(1, sourceId);
                    try (ResultSet rs = existing.executeQuery()) {
                        if (rs.next()) {
                            String current = rs.getString(1);
                            if (!txnId.equals(current)) {
                                throw new SQLException("source message " + sourceId
                                        + " already maps to transaction " + current);
                            }
                            return; // It was safely inserted concurrently by another thread
                        }
                    }
                }
            }
            throw e;
        }
    }

    private void rebuildSourceIndex(Connection conn) throws SQLException {
        try (Statement delete = conn.createStatement()) {
            delete.execute("DELETE FROM ledger_sources");
        }

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT txn_id, source_message_ids FROM ledger WHERE txn_id IS NOT NULL")) {
            while (rs.next()) {
                String txnId = rs.getString(1);
                for (String sourceId : rs.getString(2).split(",")) {
                    if (!sourceId.isBlank()) {
                        upsertSourceMapping(conn, sourceId, txnId);
                    }
                }
            }
        }
    }

    @Override
    public void save(NormalizedTxn t) {
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "")) {
            boolean autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                saveInternal(conn, t);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(autoCommit);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("could not save " + t, e);
        }
    }

    @Override
    public List<NormalizedTxn> all() {
        List<NormalizedTxn> out = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
             Statement st = conn.createStatement();
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
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM ledger")) {
            return rs.next() ? rs.getLong(1) : 0L;
        } catch (SQLException e) {
            throw new IllegalStateException("could not count the ledger", e);
        }
    }

    @Override
    public void save(in.simplifymoney.ledgersync.model.Discrepancy d) {
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
             PreparedStatement ps = conn.prepareStatement(
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
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
             Statement st = conn.createStatement();
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
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT SUM(amount) FROM ledger")) {
            return rs.next() && rs.getBigDecimal(1) != null
                    ? rs.getBigDecimal(1).setScale(2) : BigDecimal.ZERO.setScale(2);
        } catch (SQLException e) {
            throw new IllegalStateException("could not total the ledger", e);
        }
    }

    @Override
    public void close() {
        // No longer caching a single connection.
    }
}
