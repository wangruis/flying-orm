package com.flying.orm.core.architecture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CoreDependencyBoundaryTest {

    private static final byte[] JAVA_SQL_REFERENCE = "java/sql/".getBytes(StandardCharsets.US_ASCII);

    @Test
    void productionBytecodeDoesNotReferenceJavaSql() throws IOException {
        Path classes = Path.of("target", "classes");
        List<Path> offenders;
        try (Stream<Path> files = Files.walk(classes)) {
            offenders = files.filter(path -> path.toString().endsWith(".class"))
                             .filter(CoreDependencyBoundaryTest::referencesJavaSql)
                             .map(classes::relativize)
                             .sorted()
                             .toList();
        }

        assertEquals(List.of(), offenders, "core production bytecode must not reference java.sql");
    }

    private static boolean referencesJavaSql(Path path) {
        try {
            byte[] bytes = Files.readAllBytes(path);
            outer:
            for (int offset = 0; offset <= bytes.length - JAVA_SQL_REFERENCE.length; offset++) {
                for (int index = 0; index < JAVA_SQL_REFERENCE.length; index++) {
                    if (bytes[offset + index] != JAVA_SQL_REFERENCE[index]) {
                        continue outer;
                    }
                }
                return true;
            }
            return false;
        } catch (IOException error) {
            throw new IllegalStateException("failed to inspect compiled core class", error);
        }
    }
}
