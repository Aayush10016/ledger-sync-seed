package in.simplifymoney.ledgersync;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MigrationVersionTest {
    private static final Pattern VERSIONED_MIGRATION = Pattern.compile("^V(.+)__.+\\.sql$");

    @Test
    public void migrationVersionsAreUnique() throws Exception {
        Path migrationDir = Path.of(System.getProperty("user.dir"), "db", "migration");
        Map<String, String> seen = new LinkedHashMap<>();
        Map<String, List<String>> duplicates = new LinkedHashMap<>();

        try (var files = Files.list(migrationDir)) {
            for (String filename : files
                    .map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(".sql"))
                    .sorted()
                    .toList()) {
                Matcher matcher = VERSIONED_MIGRATION.matcher(filename);
                if (!matcher.matches()) {
                    continue;
                }
                String version = matcher.group(1).replace('_', '.');
                String previous = seen.putIfAbsent(version, filename);
                if (previous != null) {
                    duplicates.put(version, List.of(previous, filename));
                }
            }
        }

        assertTrue(duplicates.isEmpty(), "Duplicate migration versions: " + duplicates);
    }
}
