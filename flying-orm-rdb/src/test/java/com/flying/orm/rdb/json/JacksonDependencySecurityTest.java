package com.flying.orm.rdb.json;

import org.junit.jupiter.api.Test;
import tools.jackson.core.ErrorReportConfiguration;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.json.JsonFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Guards the upstream DataInput error-token fix in the resolved runtime dependency. */
class JacksonDependencySecurityTest {
    @Test
    void dataInputMalformedTokenHonorsConfiguredErrorReportLimit() {
        JsonFactory factory = JsonFactory.builder().errorReportConfiguration(
                ErrorReportConfiguration.builder().maxErrorTokenLength(64).build()).build();
        byte[] input = ("{\"value\":t" + "x".repeat(10_000) + " }").getBytes(StandardCharsets.UTF_8);
        try (DataInputStream stream = new DataInputStream(new ByteArrayInputStream(input));
             JsonParser parser = factory.createParser(ObjectReadContext.empty(), (DataInput) stream)) {
            JacksonException error = assertThrows(JacksonException.class, () -> {
                while (parser.nextToken() != null) {
                    // Scan to the malformed token through the real DataInput parser.
                }
            });
            assertTrue(error.getMessage().length() < 512,
                    () -> "malformed token expanded to " + error.getMessage().length() + " error characters");
        } catch (java.io.IOException error) {
            throw new AssertionError(error);
        }
    }
}
