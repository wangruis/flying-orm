package com.flying.orm.rdb.json;

import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import com.flying.orm.rdb.mapping.EntityMappingEvent;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Jackson 3 的最小生产兼容契约。 */
class Jackson3CompatibilityTest {

    @Test
    void exposesJackson3TreeValues() {
        assertTrue(JsonValueCodec.supportsTarget(JsonNode.class));

        Object decoded = JsonValueCodec.read("{\"name\":\"Alice\"}", JsonNode.class);

        JsonNode tree = assertInstanceOf(JsonNode.class, decoded);
        assertEquals("Alice", tree.get("name").asString());
    }

    @Test
    void keepsDynamicJsonValuesIndependentOfTheTreeApi() {
        assertEquals(Map.of("name", "Alice"), JsonValueCodec.read("{\"name\":\"Alice\"}"));
    }

    @Test
    void rejectsTrailingJsonDocumentsBeforeWriting() {
        assertThrows(IllegalArgumentException.class,
                     () -> JsonValueCodec.write("{\"first\":1}{\"second\":2}"));
    }

    @Test
    void rejectsTrailingJsonDocumentsAtBothReadBoundaries() {
        String trailingDocument = "{\"first\":1}{\"second\":2}";

        assertThrows(IllegalArgumentException.class, () -> JsonValueCodec.read(trailingDocument));
        assertThrows(IllegalArgumentException.class, () -> JsonValueCodec.read(trailingDocument, JsonNode.class));
    }

    @Test
    void doesNotMineArbitraryJacksonCauseGraphsForFatalErrors() {
        OutOfMemoryError fatal = new OutOfMemoryError("fatal-json-write");

        IllegalArgumentException actual = assertThrows(
                IllegalArgumentException.class, () -> JsonValueCodec.write(new FatalJsonBean(fatal)));

        assertEquals("json value cannot be serialized", actual.getMessage());
    }

    @Test
    void keepsJackson3TreesByReferenceAtTheMappingEventBoundary() {
        ObjectNode source = JsonNodeFactory.instance.objectNode().put("name", "Alice");
        EntityMappingEvent event = mappingEvent(source);

        source.put("name", "caller");
        ObjectNode exposed = (ObjectNode) event.values().get("json");

        JsonNode stable = (JsonNode) event.values().get("json");
        assertSame(source, exposed);
        assertSame(exposed, stable);
        assertEquals("caller", stable.get("name").asString());
        assertEquals("caller", source.get("name").asString());
    }

    @Test
    void doesNotTraverseCyclicJackson3Trees() {
        ObjectNode cyclic = JsonNodeFactory.instance.objectNode();
        cyclic.set("self", cyclic);

        assertSame(cyclic, mappingEvent(cyclic).values().get("json"));
    }

    @Test
    void doesNotTraverseDeepJackson3Trees() {
        JsonNode nested = nestedTree(65);

        assertSame(nested, mappingEvent(nested).values().get("json"));
    }

    @Test
    void acceptsJackson3TreesAtTheNestingBoundary() {
        JsonNode stable = (JsonNode) mappingEvent(nestedTree(64)).values().get("json");

        assertTrue(stable.isObject());
    }

    @Test
    void infersJackson3JsonNodeEntityFieldsAsJson() {
        assertEquals("JSON", EntityMetadataResolver.createUncached(JsonEntity.class).field("payload").dataType());
    }

    @Test
    void keepsNestedJackson3TreesSerializable() {
        ObjectNode currentTree = JsonNodeFactory.instance.objectNode().put("name", "Alice");

        assertEquals("{\"current\":{\"name\":\"Alice\"}}",
                     JsonValueCodec.write(Map.of("current", currentTree)));
    }

    @Test
    void serializesNativeJavaTimeValuesWithJackson3() {
        Instant instant = Instant.parse("2026-08-21T03:04:05.123456Z");
        OffsetDateTime offset = OffsetDateTime.parse("2026-08-21T11:04:05.123456+08:00");

        String json = JsonValueCodec.write(Map.of("instant", instant, "offset", offset));
        Map<?, ?> decoded = assertInstanceOf(Map.class, JsonValueCodec.read(json));

        assertEquals(instant.toString(), decoded.get("instant"));
        assertEquals(offset.toString(), decoded.get("offset"));
    }

    private static EntityMappingEvent mappingEvent(JsonNode value) {
        TestEntity entity = new TestEntity(1L);
        return new EntityMappingEvent(
                EntityMetadataResolver.createUncached(TestEntity.class), entity, Map.of("json", value));
    }

    private static ObjectNode nestedTree(int depth) {
        ObjectNode root = JsonNodeFactory.instance.objectNode();
        ObjectNode cursor = root;
        for (int level = 0; level < depth; level++) {
            cursor = cursor.putObject("child");
        }
        return root;
    }

    private record TestEntity(Long id) {
    }

    private record JsonEntity(Long id, JsonNode payload) {
    }

    private static final class FatalJsonBean {

        private final OutOfMemoryError fatal;

        private FatalJsonBean(OutOfMemoryError fatal) {
            this.fatal = fatal;
        }

        public String getValue() {
            throw new IllegalArgumentException("wrapped-json-failure", fatal);
        }
    }
}
