package com.flying.orm.rdb.json;

import oracle.sql.json.OracleJsonValue;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class OracleJsonValueCodecTest {

    @Test
    void normalizesOracleJsonObjectsForDynamicAndTypedReads() {
        OracleObject value = new OracleObject("{\"name\":\"flying-orm\",\"count\":3}");

        Object dynamic = JsonValueCodec.read(value);
        Object typed = JsonValueCodec.read(value, Map.class);
        JsonNode tree = assertInstanceOf(JsonNode.class, JsonValueCodec.read(value, JsonNode.class));

        assertEquals(Map.of("name", "flying-orm", "count", 3), dynamic);
        assertEquals(Map.of("name", "flying-orm", "count", 3), typed);
        assertEquals("flying-orm", tree.get("name").asString());
    }

    private static final class OracleObject extends AbstractMap<String, OracleJsonValue>
            implements OracleJsonValue {

        private final String json;

        private OracleObject(String json) {
            this.json = json;
        }

        @Override
        public Set<Entry<String, OracleJsonValue>> entrySet() {
            return Set.of(Map.entry("vendor", new OracleScalar("wrapped")));
        }

        @Override
        public String toString() {
            return json;
        }
    }

    private record OracleScalar(String value) implements OracleJsonValue {
        @Override
        public String toString() {
            return '"' + value + '"';
        }
    }
}
