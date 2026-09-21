package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.id.IdGenerator;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BeanColumnPrecedenceTest {

    @TestFactory
    List<DynamicTest> physicalColumnWinsRegardlessOfOrderAndNullValue() {
        List<DynamicTest> tests = new ArrayList<>();
        for (MapperEntry entry : MapperEntry.values()) {
            for (boolean physicalFirst : List.of(false, true)) {
                for (boolean nullPhysical : List.of(false, true)) {
                    tests.add(DynamicTest.dynamicTest(entry + " / physicalFirst=" + physicalFirst
                            + " / nullPhysical=" + nullPhysical, () -> {
                        Map<String, Object> row = new LinkedHashMap<>();
                        Object physical = nullPhysical ? null : "physical";
                        row.put(physicalFirst ? "stored_name" : "name", physicalFirst ? physical : "alias");
                        row.put(physicalFirst ? "name" : "stored_name", physicalFirst ? "alias" : physical);
                        try (EntityModelRegistry registry = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
                            SetterBean bean = entry.mapper(SetterBean.class, registry).map(row);
                            assertEquals(physical, bean.name);
                            assertEquals(1, bean.assignments);
                            assertEquals(physical, entry.mapper(FieldBean.class, registry).map(row).name);
                        }
                    }));
                }
            }
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> propertyLabelRemainsAFallbackWhenPhysicalColumnIsAbsent() {
        List<DynamicTest> tests = new ArrayList<>();
        for (MapperEntry entry : MapperEntry.values()) {
            tests.add(DynamicTest.dynamicTest(entry.toString(), () -> {
                try (EntityModelRegistry registry = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
                    Map<String, Object> row = Map.of("name", "alias", "extra", "ignored");
                    SetterBean bean = entry.mapper(SetterBean.class, registry).map(row);
                    assertEquals("alias", bean.name);
                    assertEquals(1, bean.assignments);
                    assertEquals("alias", entry.mapper(FieldBean.class, registry).map(row).name);
                }
            }));
        }
        return tests;
    }

    private enum MapperEntry {
        DIRECT, REGISTRY, DECODED;

        <T> RowMapper<T> mapper(Class<T> type, EntityModelRegistry registry) {
            return switch (this) {
                case DIRECT -> RowMapper.of(type);
                case REGISTRY -> registry.rowMapper(type, ValueCodecRegistry.standard());
                case DECODED -> registry.decodedRowMapper(type, ValueCodecRegistry.standard());
            };
        }
    }

    @TestFactory
    List<DynamicTest> rawCustomCodecReadsOnlyTheSelectedPhysicalColumn() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean physicalFirst : List.of(false, true)) {
            tests.add(DynamicTest.dynamicTest("physicalFirst=" + physicalFirst, () -> {
                AtomicInteger reads = new AtomicInteger();
                ValueCodec codec = new ValueCodec() {
                    @Override
                    public boolean supports(Class<?> type) {
                        return type == Name.class;
                    }

                    @Override
                    public Object read(Object value, Class<?> type) {
                        reads.incrementAndGet();
                        return new Name((String) value);
                    }
                };
                EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
                        .register("mapped-name", Name.class, DatabaseType.of("VARCHAR(64)"), codec).build();
                EntitySchemaDescriptor<CustomBean> schema = EntitySchemaDescriptor.builder(CustomBean.class)
                        .typeMappings(mappings).build();
                Map<String, Object> row = new LinkedHashMap<>();
                row.put(physicalFirst ? "stored_name" : "name", physicalFirst ? "physical" : "alias");
                row.put(physicalFirst ? "name" : "stored_name", physicalFirst ? "alias" : "physical");
                try (EntityModelRegistry registry = EntityModelRegistry.create(CacheRegionPolicy.disabled(),
                        IdGenerator.none(), EntityFieldFiller.none(), Map.of(CustomBean.class, schema))) {
                    CustomBean bean = registry.rawRowMapper(CustomBean.class, ValueCodecRegistry.standard()).map(row);
                    assertEquals(new Name("physical"), bean.name);
                    assertEquals(1, reads.get());
                }
            }));
        }
        return tests;
    }

    private static final class SetterBean {
        @TableField("stored_name")
        private String name;
        @TableField(exist = false)
        private int assignments;

        public String getName() {
            return name;
        }

        public void setName(String value) {
            assignments++;
            name = value;
        }
    }

    private static final class FieldBean {
        @TableField("stored_name")
        private String name;
    }

    private record Name(String value) {
    }

    @TableName("names")
    private static final class CustomBean {
        @TableField("stored_name")
        private Name name;
    }
}
