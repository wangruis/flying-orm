package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.codec.DriverValueAdapter;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.codec.DialectScalarValueCodec;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.json.JsonValueCodec;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import tools.jackson.databind.JsonNode;

import java.sql.Timestamp;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityEnumCompositeMatrixTest {

    @TestFactory
    List<DynamicTest> absoluteTimeCodesSurviveDriverOffsetNormalization() {
        OffsetDateTime time = OffsetCode.VALUE.value;
        List<Object> carriers = List.of(time, time.withOffsetSameInstant(ZoneOffset.UTC),
                time.toInstant(), Timestamp.from(time.toInstant()),
                DialectScalarValueCodec.write(time, "TIMESTAMPTZ", "mysql", true,
                        ValueCodecRegistry.standard()));
        List<DynamicTest> tests = new ArrayList<>();
        for (int index = 0; index < carriers.size(); index++) {
            Object carrier = carriers.get(index);
            for (boolean bean : List.of(false, true)) {
                tests.add(DynamicTest.dynamicTest("offset/" + index + "/bean=" + bean, () -> {
                    Map<String, Object> row = Map.of("offset", carrier);
                    Object actual = assertDoesNotThrow(() -> bean
                            ? RowMapper.of(ValueBean.class).map(row).offset
                            : RowMapper.of(ValueRecord.class).map(row).offset());
                    assertSame(OffsetCode.VALUE, actual);
                }));
            }
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> structuredCodesReadRawAndDecodedValues() {
        List<Carrier> carriers = List.of(new Carrier("map", MapCode.VALUE, MapCode.VALUE.value),
                new Carrier("list", ListCode.VALUE, ListCode.VALUE.value),
                new Carrier("node", NodeCode.VALUE, NodeCode.VALUE.value));
        List<DynamicTest> tests = new ArrayList<>();
        for (Carrier carrier : carriers) {
            for (boolean raw : List.of(false, true)) {
                for (boolean bean : List.of(false, true)) {
                    tests.add(DynamicTest.dynamicTest(carrier.field() + "/raw=" + raw + "/bean=" + bean, () -> {
                        Object value = raw ? JsonValueCodec.write(carrier.value()) : carrier.value();
                        Map<String, Object> row = Map.of(carrier.field(), value);
                        Object actual = assertDoesNotThrow(() -> bean
                                ? RowMapper.of(ValueBean.class).map(row).value(carrier.field())
                                : RowMapper.of(ValueRecord.class).map(row).value(carrier.field()));
                        assertSame(carrier.expected(), actual);
                    }));
                }
            }
        }
        return tests;
    }

    @Test
    void duplicateAbsoluteInstantsAreRejectedBeforeExecution() {
        assertThrows(MappingException.class, () -> RowMapper.of(DuplicateRecord.class));
    }

    @TestFactory
    List<DynamicTest> structuredCodesSurviveActualJsonRoundTrip() {
        List<Carrier> carriers = List.of(
                new Carrier("precise", PreciseCode.VALUE, PreciseCode.VALUE.value),
                new Carrier("integer", IntegralCode.VALUE, IntegralCode.VALUE.value),
                new Carrier("floating", FloatCode.VALUE, FloatCode.VALUE.value),
                new Carrier("textnode", TextNodeCode.VALUE, TextNodeCode.VALUE.value),
                new Carrier("textnode", TextNodeCode.NUMBER, TextNodeCode.NUMBER.value),
                new Carrier("textnode", TextNodeCode.BOOLEAN, TextNodeCode.BOOLEAN.value));
        List<DynamicTest> tests = new ArrayList<>();
        for (Carrier carrier : carriers) {
            for (boolean entityCodecs : List.of(false, true)) {
                for (boolean decoded : List.of(false, true)) {
                    for (boolean bean : List.of(false, true)) {
                        tests.add(DynamicTest.dynamicTest(carrier.field() + "/entityCodecs=" + entityCodecs
                                + "/decoded=" + decoded + "/bean=" + bean, () -> {
                            String json = JsonValueCodec.write(carrier.value());
                            Map<String, Object> row = Map.of(carrier.field(), decoded ? JsonValueCodec.read(json) : json);
                            try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
                                ValueCodecRegistry codecs = entityCodecs ? EntityTypeMappingRegistry.standard().valueCodecs()
                                        : ValueCodecRegistry.standard();
                                if (carrier.field().equals("precise")) {
                                    // 默认 JSON 已丢失此成员精度；不引入近似匹配来强行命中。
                                    assertThrows(MappingException.class,
                                            () -> mapMember(models, bean, decoded, codecs, row, carrier.field()));
                                } else {
                                    assertSame(carrier.expected(), mapMember(models, bean, decoded, codecs, row, carrier.field()));
                                }
                            }
                        }));
                    }
                }
            }
        }
        return tests;
    }

    private static Object mapMember(EntityModelRegistry models, boolean bean, boolean decoded,
                                     ValueCodecRegistry codecs, Map<String, Object> row, String field) {
        return bean
                ? (decoded ? models.decodedRowMapper(ValueBean.class, codecs)
                           : models.rawRowMapper(ValueBean.class, codecs)).map(row).value(field)
                : (decoded ? models.decodedRowMapper(ValueRecord.class, codecs)
                           : models.rawRowMapper(ValueRecord.class, codecs)).map(row).value(field);
    }

    @Test
    void jsonCodecAndDriverAdapterKeepTheirPriority() {
        AtomicInteger calls = new AtomicInteger();
        ValueCodec codec = new ValueCodec() {
            @Override public boolean supports(Class<?> type) { return type == Map.class; }
            @Override public Object read(Object value, Class<?> type) {
                assertEquals("application-json", value);
                calls.incrementAndGet();
                return MapCode.VALUE.value;
            }
        };
        DriverValueAdapter adapter = new DriverValueAdapter() {
            @Override public boolean supports(Object value) { return value instanceof WrappedJson; }
            @Override public Object unwrap(Object value) { return ((WrappedJson) value).text(); }
        };
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(codec).withDriverAdapter(adapter);
        assertSame(MapCode.VALUE, RowMapper.of(ValueRecord.class, codecs)
                .map(Map.of("map", new WrappedJson("application-json"))).map());
        assertEquals(1, calls.get());
    }

    @Test
    void selectedJsonCodecFailureIsNotReplacedByTheFallback() {
        IllegalArgumentException failure = new IllegalArgumentException("application rejection");
        ValueCodec codec = new ValueCodec() {
            @Override public boolean supports(Class<?> type) { return type == Map.class; }
            @Override public Object read(Object value, Class<?> type) { throw failure; }
        };
        MappingException actual = assertThrows(MappingException.class, () -> RowMapper.of(ValueRecord.class,
                ValueCodecRegistry.standard().withFirst(codec)).map(Map.of("map", "{\"name\":\"active\"}")));
        assertSame(failure, actual.getCause());
    }

    @Test
    void customJsonCodecKeepsDistinctPreciseMembers() {
        ValueCodec codec = new ValueCodec() {
            @Override public boolean supports(Class<?> type) { return type == Map.class; }
            @Override public Object read(Object value, Class<?> type) {
                return PrecisePair.valueOf(value.toString()).value;
            }
        };
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(codec);
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            for (PrecisePair constant : PrecisePair.values()) {
                assertSame(constant, models.rawRowMapper(PrecisePairRecord.class, codecs)
                        .map(Map.of("value", constant.name())).value());
                assertSame(constant, models.decodedRowMapper(PrecisePairRecord.class, codecs)
                        .map(Map.of("value", constant.value)).value());
            }
        }
    }

    @Test
    void structuredCodesIgnoreObjectOrderButPreserveArrayOrderAndNumericValue() {
        RowMapper<ValueRecord> mapper = RowMapper.of(ValueRecord.class);
        assertSame(MapCode.VALUE, mapper.map(Map.of("map", "{\"price\":1,\"count\":1.0,\"name\":\"active\"}")).map());
        assertThrows(MappingException.class, () -> mapper.map(Map.of("list", "[1,\"active\",1]")));
        assertThrows(MappingException.class, () -> mapper.map(Map.of("map", "{\"price\":2,\"count\":1,\"name\":\"active\"}")));
    }

    @Test
    void explicitTextStoragePreservesOffsetsAsDistinctCodes() {
        ValueCodec codec = new ValueCodec() {
            @Override public boolean supports(Class<?> type) { return TemporalAccessor.class.isAssignableFrom(type); }
            @Override public Object write(Object value) { return value.toString(); }
            @Override public Object read(Object value, Class<?> type) { return OffsetDateTime.parse(value.toString()); }
        };
        EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
                .register("offset-text", TemporalAccessor.class, DatabaseType.of("VARCHAR"), codec).build();
        EntitySchemaDescriptor<TextOffsetRecord> descriptor = EntitySchemaDescriptor.builder(TextOffsetRecord.class)
                .typeMappings(mappings).build();
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults(),
                IdGenerator.none(), EntityFieldFiller.none(), Map.of(TextOffsetRecord.class, descriptor))) {
            for (DuplicateOffsetCode constant : DuplicateOffsetCode.values()) {
                assertSame(constant, models.rawRowMapper(TextOffsetRecord.class, mappings.valueCodecs())
                        .map(Map.of("value", constant.value.toString())).value());
                assertEquals(constant.value, models.entityValues(TextOffsetRecord.class)
                        .read(new TextOffsetRecord(constant)).get("value"));
            }
        }
    }

    private record Carrier(String field, Enum<?> expected, Object value) { }
    private record WrappedJson(String text) { }
    private record TextOffsetRecord(@TableColumn(databaseTypeId = "offset-text") DuplicateOffsetCode value) { }

    private record ValueRecord(OffsetCode offset, MapCode map, ListCode list, NodeCode node,
                               PreciseCode precise, IntegralCode integer, FloatCode floating, TextNodeCode textnode) {
        Object value(String name) {
            return switch (name) {
                case "map" -> map;
                case "list" -> list;
                case "node" -> node;
                case "precise" -> precise;
                case "integer" -> integer;
                case "floating" -> floating;
                case "textnode" -> textnode;
                default -> throw new AssertionError(name);
            };
        }
    }

    private static final class ValueBean {
        private OffsetCode offset;
        private MapCode map;
        private ListCode list;
        private NodeCode node;
        private PreciseCode precise;
        private IntegralCode integer;
        private FloatCode floating;
        private TextNodeCode textnode;
        Object value(String name) {
            return switch (name) {
                case "map" -> map;
                case "list" -> list;
                case "node" -> node;
                case "precise" -> precise;
                case "integer" -> integer;
                case "floating" -> floating;
                case "textnode" -> textnode;
                default -> throw new AssertionError(name);
            };
        }
    }

    private enum OffsetCode {
        VALUE(OffsetDateTime.parse("2026-09-20T12:30:15.123456789+08:00"));
        @EnumValue private final OffsetDateTime value;
        OffsetCode(OffsetDateTime value) { this.value = value; }
    }

    private enum MapCode {
        VALUE(Map.of("name", "active", "count", 1L, "price", new BigDecimal("1.00")));
        @EnumValue private final Map<String, Object> value;
        MapCode(Map<String, Object> value) { this.value = value; }
    }

    private enum ListCode {
        VALUE(List.of("active", 1L, new BigDecimal("1.00")));
        @EnumValue private final List<Object> value;
        ListCode(List<Object> value) { this.value = value; }
    }

    private enum NodeCode {
        VALUE((JsonNode) JsonValueCodec.read("{\"state\":\"active\"}", JsonNode.class));
        @EnumValue private final JsonNode value;
        NodeCode(JsonNode value) { this.value = value; }
    }

    private record DuplicateRecord(DuplicateOffsetCode value) { }

    private enum PreciseCode {
        VALUE(Map.of("n", new BigDecimal("9007199254740993.0")));
        @EnumValue private final Map<String, Object> value;
        PreciseCode(Map<String, Object> value) { this.value = value; }
    }

    private enum IntegralCode {
        VALUE(Map.of("n", new BigDecimal("9007199254740993")));
        @EnumValue private final Map<String, Object> value;
        IntegralCode(Map<String, Object> value) { this.value = value; }
    }

    private enum FloatCode {
        VALUE(List.of(0.1f));
        @EnumValue private final List<Object> value;
        FloatCode(List<Object> value) { this.value = value; }
    }

    private enum TextNodeCode {
        VALUE((JsonNode) JsonValueCodec.read("\"active\"", JsonNode.class)),
        NUMBER((JsonNode) JsonValueCodec.read("42", JsonNode.class)),
        BOOLEAN((JsonNode) JsonValueCodec.read("true", JsonNode.class));
        @EnumValue private final JsonNode value;
        TextNodeCode(JsonNode value) { this.value = value; }
    }

    private record PrecisePairRecord(PrecisePair value) { }

    private enum PrecisePair {
        FIRST(Map.of("n", new BigDecimal("9007199254740992.0"))),
        SECOND(Map.of("n", new BigDecimal("9007199254740993.0")));
        @EnumValue private final Map<String, Object> value;
        PrecisePair(Map<String, Object> value) { this.value = value; }
    }

    private enum DuplicateOffsetCode {
        FIRST(OffsetDateTime.parse("2026-09-20T12:30:15+08:00")),
        SECOND(OffsetDateTime.parse("2026-09-20T04:30:15Z"));
        @EnumValue private final OffsetDateTime value;
        DuplicateOffsetCode(OffsetDateTime value) { this.value = value; }
    }
}
