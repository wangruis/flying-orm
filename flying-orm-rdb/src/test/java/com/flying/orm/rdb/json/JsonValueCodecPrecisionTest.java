package com.flying.orm.rdb.json;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonValueCodecPrecisionTest {

    private static final String PRECISE_DECIMAL = "9007199254740993.0";

    @Test
    void preservesDecimalValueWhenWritingJsonText() {
        assertDecimalPreserved(JsonValueCodec.write("  " + PRECISE_DECIMAL + "  "));
    }

    @Test
    void preservesDecimalValueWhenWritingJsonBytes() {
        assertDecimalPreserved(JsonValueCodec.write(PRECISE_DECIMAL.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void stillRejectsMalformedJsonTextAndBytes() {
        String malformed = "{\"n\":}";
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> JsonValueCodec.write(malformed)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> JsonValueCodec.write(malformed.getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void keepsBigDecimalMapSerializationUnchanged() {
        assertEquals("{\"n\":" + PRECISE_DECIMAL + "}",
                JsonValueCodec.write(Map.of("n", new BigDecimal(PRECISE_DECIMAL))));
    }

    @Test
    void keepsCompactingJsonWithoutChangingStringContent() {
        String json = "{ \"n\" : 1, \"text\" : \"a b\" }";
        String compact = "{\"n\":1,\"text\":\"a b\"}";
        assertEquals(compact, JsonValueCodec.write(json));
        assertEquals(compact, JsonValueCodec.write(json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void keepsPublicMapAndListReadNumberTypesUnchanged() {
        Map<?, ?> typedMap = assertInstanceOf(Map.class,
                JsonValueCodec.read("{\"integer\":1,\"decimal\":1.25}", Map.class));
        Map<?, ?> dynamicMap = assertInstanceOf(Map.class,
                JsonValueCodec.read("{\"integer\":1,\"decimal\":1.25}"));
        List<?> typedList = assertInstanceOf(List.class, JsonValueCodec.read("[1,1.25]", List.class));
        List<?> dynamicList = assertInstanceOf(List.class, JsonValueCodec.read("[1,1.25]"));

        assertAll(
                () -> assertInstanceOf(Integer.class, typedMap.get("integer")),
                () -> assertInstanceOf(Double.class, typedMap.get("decimal")),
                () -> assertInstanceOf(Integer.class, dynamicMap.get("integer")),
                () -> assertInstanceOf(Double.class, dynamicMap.get("decimal")),
                () -> assertInstanceOf(Integer.class, typedList.get(0)),
                () -> assertInstanceOf(Double.class, typedList.get(1)),
                () -> assertInstanceOf(Integer.class, dynamicList.get(0)),
                () -> assertInstanceOf(Double.class, dynamicList.get(1)));
    }

    @Test
    void convertsDecodedJsonScalarsWithoutParsingStringValuesAgain() {
        for (String json : List.of("42", "1.25", "true", "false", "\"plain text\"",
                "\"{\\\"looks\\\":\\\"like json\\\"}\"", "\"42\"")) {
            Object decoded = JsonValueCodec.read(json);
            assertEquals(JsonValueCodec.read(json, JsonNode.class),
                    JsonValueCodec.readDecoded(decoded, JsonNode.class), json);
        }
    }

    @Test
    void convertsDecodedJsonContainersToDeclaredTargets() {
        assertEquals(JsonValueCodec.read("{\"k\":1}", JsonNode.class),
                JsonValueCodec.readDecoded(Map.of("k", 1), JsonNode.class));
        assertEquals(JsonValueCodec.read("[1,2]", JsonNode.class),
                JsonValueCodec.readDecoded(List.of(1, 2), JsonNode.class));
        assertEquals(Set.of(1, 2), JsonValueCodec.readDecoded(List.of(1, 2), Set.class));
    }

    @Test
    void keepsRawJsonTextInterpretationSeparateFromDecodedStrings() {
        JsonNode number = assertInstanceOf(JsonNode.class, JsonValueCodec.read("42", JsonNode.class));
        JsonNode object = assertInstanceOf(JsonNode.class,
                JsonValueCodec.read("{\"k\":1}", JsonNode.class));
        JsonNode text = assertInstanceOf(JsonNode.class,
                JsonValueCodec.read("\"plain text\"", JsonNode.class));
        assertAll(
                () -> assertTrue(number.isNumber()),
                () -> assertEquals(42, number.intValue()),
                () -> assertTrue(object.isObject()),
                () -> assertTrue(text.isTextual()),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> JsonValueCodec.read("plain text", JsonNode.class)),
                () -> assertNull(JsonValueCodec.read(null, JsonNode.class)),
                () -> assertNull(JsonValueCodec.readDecoded(null, JsonNode.class)),
                () -> assertNull(JsonValueCodec.readDecoded(JsonValueCodec.read("null"), JsonNode.class)));
    }

    private static void assertDecimalPreserved(String actual) {
        assertEquals(0, new BigDecimal(PRECISE_DECIMAL).compareTo(new BigDecimal(actual)), actual);
    }
}
