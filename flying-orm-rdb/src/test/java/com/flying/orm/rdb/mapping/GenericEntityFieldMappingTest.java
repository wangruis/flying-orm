package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.json.JsonValueCodec;
import com.flying.orm.rdb.internal.mapping.EntityMetadataHierarchy;
import com.flying.orm.rdb.internal.mapping.EntityPropertyResolver;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class GenericEntityFieldMappingTest {

    @Test
    void scalarTypeIsResolvedForMetadataAndSchema() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            assertEquals(DatabaseType.of("BIGINT"), models.metadata(LongValue.class).field("value").databaseType());
            assertEquals(DatabaseType.of("BIGINT"), models.schemaDescriptor(LongValue.class)
                    .metadata().field("value").databaseType());
        }
    }

    @Test
    void scalarFieldsAndSettersConvertToTheConcreteInheritedType() {
        for (boolean decoded : new boolean[]{false, true}) {
            try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
                assertEquals(7L, (Object) mapper(models, LongValue.class, decoded).map(Map.of("value", 7)).value);
                SetterLongValue result = mapper(models, SetterLongValue.class, decoded).map(Map.of("value", 7));
                assertEquals(7L, (Object) result.getValue());
                assertEquals(1, result.assignments);
            }
        }
    }

    @Test
    void inheritedEnumUsesTheSamePersistentValueForConditionsAssignmentsAndEntityWrites() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            assertEquals("D", models.condition(StateValue.class, "value", "=", State.DISABLED).value());
            assertEquals("D", models.writeValue(StateValue.class, "value", State.DISABLED));
            StateValue entity = new StateValue();
            entity.value = State.DISABLED;
            assertEquals(Map.of("value", "D"), models.entityValues(StateValue.class).readForInsert(entity));
            SetterStateValue bean = new SetterStateValue();
            bean.setValue(State.DISABLED);
            assertEquals(Map.of("value", "D"), models.entityValues(SetterStateValue.class).readForUpdate(bean));
        }
    }

    @Test
    void inheritedEnumReadsPersistentValueThroughFieldsAndSetters() {
        for (boolean decoded : new boolean[]{false, true}) {
            try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
                assertSame(State.DISABLED, mapper(models, StateValue.class, decoded).map(Map.of("value", "D")).value);
                SetterStateValue bean = mapper(models, SetterStateValue.class, decoded).map(Map.of("value", "D"));
                assertSame(State.DISABLED, bean.getValue());
                assertEquals(1, bean.assignments);
            }
        }
    }

    @Test
    void concreteContainerArrayAndNestedTypesShareTheMemberResolution() {
        ListValue list = RowMapper.of(ListValue.class).map(Map.of("value", "[{\"name\":\"first\"}]"));
        assertItem(list.value.getFirst());
        ArrayValue array = RowMapper.of(ArrayValue.class).map(Map.of("values", new Integer[]{7, 8}));
        assertInstanceOf(Long[].class, array.values);
        assertEquals(List.of(7L, 8L), List.of(array.values));
        ItemValue nested = RowMapper.nested(ItemValue.class).map(Map.of("value.name", "first"));
        assertItem(nested.value);
    }

    @Test
    void rawGenericTypesKeepTheirDeclaredUpperBound() {
        BoundedValue<?> result = RowMapper.of(BoundedValue.class).map(Map.of("value", 7));
        assertEquals(7L, result.value);
    }

    @Test
    void inheritedInterfaceAccessorsResolveTheirTypeArguments() throws Exception {
        assertSame(Long.class, EntityMetadataHierarchy.memberClass(InterfaceValue.class,
                ValueAccess.class.getMethod("getValue").getGenericReturnType()));
        assertSame(Long.class, EntityMetadataHierarchy.memberClass(InterfaceValue.class,
                ValueAccess.class.getMethod("setValue", Object.class).getGenericParameterTypes()[0]));
        InterfaceValue result = RowMapper.of(InterfaceValue.class).map(Map.of("value", 7));
        assertEquals(7L, result.getValue());
    }

    @Test
    void defaultInterfaceGettersResolveTheSamePersistentProperty() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<InterfaceValue> metadata = models.metadata(InterfaceValue.class);
            assertEquals("value", EntityPropertyResolver.column(metadata, InterfaceValue::getValue));
            InterfaceValue entity = new InterfaceValue();
            assertEquals("value", EntityPropertyResolver.column(metadata, entity::getValue));
        }
    }

    @Test
    void untypedContainersKeepTheirExistingIdentityFastPath() {
        Map<String, Object> properties = Map.of("name", "first");
        List<Object> items = List.of(properties);
        Untyped result = RowMapper.of(Untyped.class).map(Map.of("properties", properties, "items", items));
        assertSame(properties, result.properties());
        assertSame(items, result.items());
    }

    @Test
    void arrayCarriersStillMapToCollectionFields() {
        ItemsRecord result = RowMapper.of(ItemsRecord.class)
                .map(Map.of("items", new Object[]{Map.of("name", "first")}));
        assertItem(result.items().getFirst());
    }

    @TestFactory
    java.util.stream.Stream<DynamicTest> typedContainers() {
        return java.util.stream.Stream.of(false, true).flatMap(decoded -> java.util.stream.Stream.of(
                DynamicTest.dynamicTest("record decoded=" + decoded,
                        () -> recordPreservesNestedElementTypes(decoded)),
                DynamicTest.dynamicTest("inherited field decoded=" + decoded,
                        () -> inheritedFieldsResolveConcreteTypeArguments(decoded)),
                DynamicTest.dynamicTest("inherited setter decoded=" + decoded,
                        () -> inheritedSettersResolveConcreteTypeArguments(decoded)),
                DynamicTest.dynamicTest("ordinary mapper decoded=" + decoded,
                        () -> ordinaryMapperConvertsBothTextAndAlreadyDecodedContainers(decoded))));
    }

    void recordPreservesNestedElementTypes(boolean decoded) {
        Map<String, Object> row = row(decoded);
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            ItemsRecord result = mapper(models, ItemsRecord.class, decoded).map(row);
            assertItem(result.items().getFirst());
            assertItem(result.grouped().get("team").getFirst());
        }
    }

    void inheritedFieldsResolveConcreteTypeArguments(boolean decoded) {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            Items result = mapper(models, Items.class, decoded).map(row(decoded));
            assertItem(result.items.getFirst());
            assertItem(result.grouped.get("team").getFirst());
        }
    }

    void inheritedSettersResolveConcreteTypeArguments(boolean decoded) {
        Object items = row(decoded).get("items");
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            SetterItems result = mapper(models, SetterItems.class, decoded).map(Map.of("items", items));
            assertItem(result.getItems().getFirst());
            assertEquals(1, result.assignments);
        }
    }

    void ordinaryMapperConvertsBothTextAndAlreadyDecodedContainers(boolean decoded) {
        ItemsRecord result = RowMapper.of(ItemsRecord.class).map(row(decoded));
        assertItem(result.items().getFirst());
        assertItem(result.grouped().get("team").getFirst());
    }

    private static <T> RowMapper<T> mapper(EntityModelRegistry models, Class<T> type, boolean decoded) {
        return decoded ? models.decodedRowMapper(type, ValueCodecRegistry.standard())
                : models.rawRowMapper(type, ValueCodecRegistry.standard());
    }

    private static Map<String, Object> row(boolean decoded) {
        List<Map<String, String>> items = List.of(Map.of("name", "first"));
        Map<String, List<Map<String, String>>> grouped = Map.of("team", items);
        return Map.of("items", decoded ? items : JsonValueCodec.write(items),
                "grouped", decoded ? grouped : JsonValueCodec.write(grouped));
    }

    private static void assertItem(Object value) {
        assertEquals("first", assertInstanceOf(Item.class, value).name());
    }

    record Item(String name) { }
    record Untyped(Map<String, Object> properties, List<Object> items) { }
    record ItemsRecord(List<Item> items, Map<String, List<Item>> grouped) { }
    static class Base<T> { public List<T> items; }
    static class Middle<T> extends Base<T> { }
    static class Items extends Middle<Item> { public Map<String, List<Item>> grouped; }
    static class SetterBase<T> {
        private List<T> items;
        transient int assignments;
        public List<T> getItems() { return items; }
        public void setItems(List<T> items) { this.items = items; assignments++; }
    }
    static class SetterItems extends SetterBase<Item> { }
    static class ScalarBase<T> { public T value; }
    static class ScalarMiddle<T> extends ScalarBase<T> { }
    static class LongValue extends ScalarMiddle<Long> { }
    static class StateValue extends ScalarMiddle<State> { }
    static class ListValue extends ScalarMiddle<List<Item>> { }
    static class ItemValue extends ScalarMiddle<Item> { }
    static class ArrayBase<T> { public T[] values; }
    static class ArrayValue extends ArrayBase<Long> { }
    static class BoundedValue<T extends Long> { public T value; }
    interface ValueAccess<T> {
        T read();
        void write(T value);
        default T getValue() { return read(); }
        default void setValue(T value) { write(value); }
    }
    static class InterfaceValue implements ValueAccess<Long> {
        private Long value;
        public Long read() { return value; }
        public void write(Long value) { this.value = value; }
    }
    static class ScalarSetterBase<T> {
        private T value;
        transient int assignments;
        public T getValue() { return value; }
        public void setValue(T value) { this.value = value; assignments++; }
    }
    static class SetterLongValue extends ScalarSetterBase<Long> { }
    static class SetterStateValue extends ScalarSetterBase<State> { }
    enum State {
        DISABLED("D");
        @EnumValue private final String code;
        State(String code) { this.code = code; }
    }
}
