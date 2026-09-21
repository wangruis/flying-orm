package com.flying.orm.rdb.json;

import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import org.junit.jupiter.api.Test;
import tools.jackson.core.exc.StreamConstraintsException;

import javax.xml.datatype.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FullAuditJacksonSecurityRegressionTest {
    public static final class Durations extends ArrayList<Duration> {
    }

    @Test
    void supportedTypedCollectionStillReadsNormalXmlDuration() {
        assertNotNull(EntityTypeMappingRegistry.standard().resolve(Durations.class));
        Durations values = (Durations) JsonValueCodec.read("[\"P1Y\"]", Durations.class);
        assertEquals(1, values.size());
        assertEquals(1, values.getFirst().getYears());
    }

    @Test
    void jacksonRejectsOverlongXmlDurationNumberAtTypedReadBoundary() {
        String json = "[\"P" + "9".repeat(2000) + "Y\"]";
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> JsonValueCodec.read(json, Durations.class));
        Throwable cause = error;
        while (cause != null && !(cause instanceof StreamConstraintsException)) {
            cause = cause.getCause();
        }
        assertInstanceOf(StreamConstraintsException.class, cause);
    }

    @Test
    void untypedJsonStringsAreNotFilteredByTheOrm() {
        String value = "P" + "9".repeat(2000) + "Y";
        assertEquals(List.of(value), JsonValueCodec.read("[\"" + value + "\"]", List.class));
    }
}
