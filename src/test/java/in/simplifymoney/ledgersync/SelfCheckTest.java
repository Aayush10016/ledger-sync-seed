package in.simplifymoney.ledgersync;

import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class SelfCheckTest {

    @Test
    public void testValidCorpusPasses() throws Exception {
        // The standard corpus should pass flawlessly
        Path corpus = Path.of("fixtures/corpus-a.jsonl");
        Path totals = Path.of("fixtures/corpus-a-totals.json");
        
        // Assert that a perfectly matching corpus returns true (0 discrepancies or only allowed discrepancies)
        assertTrue(SelfCheck.run(corpus, totals), "SelfCheck should pass for valid corpus");
    }

    @Test
    public void testUnexpectedBalanceDiscrepancyFails() throws Exception {
        Path corpus = Path.of("fixtures/corpus-a.jsonl");
        String alteredTotals = Files.readString(Path.of("fixtures/corpus-a-totals.json"))
                .replace("\"closing_balance\": \"41126.34\"", "\"closing_balance\": \"41127.34\"");
        Path totals = Files.createTempFile("bad-totals", ".json");
        Files.writeString(totals, alteredTotals);

        assertFalse(SelfCheck.run(corpus, totals), "SelfCheck must fail for unexpected discrepancies");
    }
}
