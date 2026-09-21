package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ExistingExtensionSeamGapContractTest {

    @Test
    void gapTableKeepsTheSixExistingSeamsAndFutureSpiBoundaryVisible() throws IOException {
        Path document = locateDocument();
        String table = Files.readString(document);

        for (String seam : new String[]{
                "FeatureRegistry", "TermRegistry", "SqlTermRegistry",
                "ValueCodecRegistry", "RdbDialect", "DatabaseOperator"}) {
            assertTrue(table.contains(seam), () -> "missing extension seam: " + seam);
        }
        assertTrue(table.contains("不新增 public SPI"));
        assertTrue(table.contains("不接管事务、连接池或数据源凭据"));
    }

    private static Path locateDocument() {
        Path directory = Path.of("").toAbsolutePath();
        while (directory != null) {
            Path candidate = directory.resolve(
                    "docs/superpowers/plans/2026-09-02-flying-orm-existing-extension-seam-gap-table.md");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            directory = directory.getParent();
        }
        throw new IllegalStateException("extension seam gap table does not exist above the test directory");
    }
}
