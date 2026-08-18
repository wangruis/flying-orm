package com.flying.orm.rdb.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import com.flying.orm.rdb.internal.mapping.EntityValues;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 覆盖 record/bean 映射计划对列别名、共享 codec 和复杂字段类型的处理。 */
class MappingPlanTest {

    /** 映射监听器拿到的是独立现场，不能借可变 JDBC 时间对象改写随后执行的实体值。 */
    @Test
    void snapshotsMutableJdbcTimeValuesAtTheMappingEventBoundary() {
        Timestamp source = Timestamp.valueOf("2026-08-18 10:11:12.123456789");
        long expectedMillis = source.getTime();
        int expectedNanos = source.getNanos();
        EntityMetadata<RecordUser> metadata = EntityMetadataResolver.createUncached(RecordUser.class);
        EntityMappingEvent event = new EntityMappingEvent(
                metadata,
                new RecordUser(1, true, Status.ACTIVE, LocalDateTime.of(2026, 8, 18, 10, 11)),
                Map.of("created_at", source));

        source.setTime(0L);
        Timestamp exposed = (Timestamp) event.values().get("created_at");
        exposed.setTime(1L);

        Timestamp stable = (Timestamp) event.values().get("created_at");
        assertNotSame(source, stable);
        assertNotSame(exposed, stable);
        assertEquals(expectedMillis, stable.getTime());
        assertEquals(expectedNanos, stable.getNanos());
    }

    @Test
    void entityModelRegistryHonorsConfiguredMappingCachePolicy() {
        EntityModelRegistry disabled = EntityModelRegistry.create(CacheRegionPolicy.disabled());
        assertNotSame(disabled.metadata(RecordUser.class), disabled.metadata(RecordUser.class));
        assertNotSame(disabled.entityValues(RecordUser.class), disabled.entityValues(RecordUser.class));
        assertNotSame(disabled.rowMapper(RecordUser.class, ValueCodecRegistry.standard()),
                      disabled.rowMapper(RecordUser.class, ValueCodecRegistry.standard()));

        EntityModelRegistry bounded = EntityModelRegistry.create(new CacheRegionPolicy(
                true, 8, 8, Duration.ofMinutes(5), false));
        RowMapper<RecordUser> first = bounded.rowMapper(RecordUser.class, ValueCodecRegistry.standard());
        RowMapper<RecordUser> second = bounded.rowMapper(RecordUser.class, ValueCodecRegistry.standard());
        assertSame(first, second);
        EntityMetadata<RecordUser> metadata = bounded.metadata(RecordUser.class);
        for (int attempt = 0; attempt < 64; attempt++) {
            assertSame(first, bounded.rowMapper(RecordUser.class, ValueCodecRegistry.standard()));
            assertSame(metadata, bounded.metadata(RecordUser.class));
        }
        assertEquals(2L, bounded.estimatedMappings());
        assertEquals(0L, bounded.stats().requestCount());
    }

    @Test
    void mapsRecordValuesThroughStandardValueCodecs() {
        LocalDateTime createdAt = LocalDateTime.of(2026, 7, 29, 13, 50, 0);
        Map<String, Object> row = row("id", BigDecimal.valueOf(7),
                                      "enabled", "1",
                                      "status", "ACTIVE",
                                      "created_at", Timestamp.valueOf(createdAt));

        RecordUser user = MappingPlan.of(RecordUser.class).map(row);

        assertEquals(7, user.id());
        assertTrue(user.enabled());
        assertEquals(Status.ACTIVE, user.status());
        assertEquals(createdAt, user.createdAt());
    }

    /** 实体反射访问点不能把 JVM 致命错误降级成普通映射异常。 */
    @Test
    void propagatesVirtualMachineErrorsFromEntityGetterSetterAndConstructor() {
        OutOfMemoryError getterFailure = new OutOfMemoryError("entity getter fatal");
        FatalGetterBean.failure = getterFailure;
        OutOfMemoryError observedGetter = assertThrows(
                OutOfMemoryError.class,
                () -> EntityValues.createUncached(FatalGetterBean.class).read(new FatalGetterBean()));

        OutOfMemoryError setterFailure = new OutOfMemoryError("entity setter fatal");
        FatalSetterBean.failure = setterFailure;
        OutOfMemoryError observedSetter = assertThrows(
                OutOfMemoryError.class,
                () -> MappingPlan.of(FatalSetterBean.class).map(row("value", "stable")));

        OutOfMemoryError constructorFailure = new OutOfMemoryError("entity constructor fatal");
        FatalConstructorBean.failure = constructorFailure;
        OutOfMemoryError observedConstructor = assertThrows(
                OutOfMemoryError.class,
                () -> MappingPlan.of(FatalConstructorBean.class).map(row("value", "stable")));

        assertSame(getterFailure, observedGetter);
        assertSame(setterFailure, observedSetter);
        assertSame(constructorFailure, observedConstructor);
    }

    /** 反射目标深层包装的 JVM 致命错误仍须保持原对象，不能因诊断图预算而降级成映射异常。 */
    @Test
    void propagatesDeeplyWrappedVirtualMachineErrorFromEntityGetter() {
        OutOfMemoryError fatal = new OutOfMemoryError("deep entity getter fatal");
        RuntimeException wrapped = new IllegalStateException("wrapper-0", fatal);
        for (int depth = 1; depth < 70; depth++) {
            wrapped = new IllegalStateException("wrapper-" + depth, wrapped);
        }
        DeepFatalGetterBean.failure = wrapped;

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> EntityValues.createUncached(DeepFatalGetterBean.class).read(new DeepFatalGetterBean()));

        assertSame(fatal, observed);
    }

    @Test
    void mapsBeanValuesThroughStandardValueCodecs() {
        LocalDateTime createdAt = LocalDateTime.of(2026, 7, 29, 13, 55, 0);
        Map<String, Object> row = row("id", "9",
                                      "enabled", BigDecimal.ONE,
                                      "status", "DISABLED",
                                      "created_at", Timestamp.valueOf(createdAt));

        BeanUser user = MappingPlan.of(BeanUser.class).map(row);

        assertEquals(9L, user.id);
        assertTrue(user.enabled);
        assertEquals(Status.DISABLED, user.status);
        assertEquals(createdAt, user.createdAt);
    }

    @Test
    void rejectsQualifiedColumnsThatCollapseToTheSameProperty() {
        Map<String, Object> row = row("users.id", 7,
                                      "orders.id", 19,
                                      "users.enabled", true,
                                      "users.status", "ACTIVE",
                                      "users.created_at", LocalDateTime.now());

        MappingException error = assertThrows(MappingException.class,
                                               () -> MappingPlan.of(RecordUser.class).map(row));

        assertEquals("column names become ambiguous after normalization", error.getMessage());
    }

    @Test
    void ignoresCollidingJoinColumnsWhenTheBeanDoesNotMapThem() {
        Map<String, Object> row = row("users.id", 7,
                                      "orders.id", 19,
                                      "display_name", "Alice");

        NameOnlyBean bean = MappingPlan.of(NameOnlyBean.class).map(row);

        assertEquals("Alice", bean.displayName);
    }

    @Test
    void entityValuesUseColumnNamesFromFlyingAnnotations() {
        AnnotatedBeanUser user = new AnnotatedBeanUser();
        user.id = 11L;
        user.name = "Alice";
        user.enabled = true;

        Map<String, Object> values = EntityValues.createUncached(AnnotatedBeanUser.class).read(user);

        assertEquals(11L, values.get("id"));
        assertEquals("Alice", values.get("user_name"));
        assertEquals(true, values.get("enabled"));
    }

    /** 实体推断为 VARCHAR 的标准值必须先转成文本，供写入与 Repository 自动条件复用同一数据库形态。 */
    @Test
    void storesStandardTextBackedEntityValuesAsText() {
        UUID id = UUID.fromString("06fb6f53-ae7d-4e2b-8d38-17a72865e726");
        Instant createdAt = Instant.parse("2026-08-12T01:02:03Z");
        OffsetDateTime updatedAt = OffsetDateTime.of(2026, 8, 12, 9, 2, 3, 0, ZoneOffset.ofHours(8));

        Map<String, Object> values = EntityValues.createUncached(TextBackedRecord.class)
                                                  .read(new TextBackedRecord(id, createdAt, updatedAt, 'A'));

        assertEquals(id.toString(), values.get("id"));
        assertEquals(createdAt.toString(), values.get("created_at"));
        assertEquals(updatedAt.toString(), values.get("updated_at"));
        assertEquals("A", values.get("marker"));
    }

    @Test
    void mappingPlanWritesAnnotatedBeanFieldsFromColumnNames() {
        Map<String, Object> row = row("id", "12",
                                      "user_name", "Bob",
                                      "enabled", "1");

        AnnotatedBeanUser user = MappingPlan.of(AnnotatedBeanUser.class).map(row);

        assertEquals(12L, user.id);
        assertEquals("Bob", user.name);
        assertTrue(user.enabled);
    }

    @Test
    void mapsJsonTextIntoRecordCollections() {
        JsonProfile profile = MappingPlan.of(JsonProfile.class)
                                         .map(row("attributes", "{\"name\":\"Alice\",\"enabled\":true}",
                                                  "tags", "[\"admin\",\"operator\"]"));

        assertEquals(Map.of("name", "Alice", "enabled", true), profile.attributes());
        assertEquals(List.of("admin", "operator"), profile.tags());
        assertEquals("JSON",
                     EntityMetadataResolver.createUncached(JsonProfile.class)
                                           .toDynamicForm()
                                           .field("attributes")
                                           .dataType());
    }

    @Test
    void mapsMaterializedLargeObjectsIntoRecordFields() {
        LargeObjectRecord record = MappingPlan.of(LargeObjectRecord.class)
                                              .map(row("payload", ByteBuffer.wrap(new byte[]{1, 2, 3}),
                                                       "content", new StringBuilder("large text")));

        assertArrayEquals(new byte[]{1, 2, 3}, record.payload());
        assertEquals("large text", record.content());
    }

    @Test
    void mapsDriverArraysIntoEntityArrayFields() {
        ArrayRecord record = MappingPlan.of(ArrayRecord.class)
                                        .map(row("ids", new Integer[]{1, 2, 3}));

        assertArrayEquals(new Long[]{1L, 2L, 3L}, record.ids());
    }

    @Test
    void mapsEntityValuesThroughAnApplicationCodecRegistry() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == Money.class;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return new Money(new BigDecimal(value.toString()));
            }
        });

        Invoice invoice = MappingPlan.of(Invoice.class, codecs).map(row("amount", "12.30"));

        assertEquals(new Money(new BigDecimal("12.30")), invoice.amount());
    }

    /** 应用 codec 包装的 JVM 致命错误不能被实体值转换降级成普通映射失败。 */
    @Test
    void preservesNestedVirtualMachineErrorFromApplicationValueCodec() {
        OutOfMemoryError fatal = new OutOfMemoryError("application codec fatal");
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(failingCodec(Money.class, fatal));

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> MappingPlan.of(Invoice.class, codecs).map(row("amount", "12.30")));

        assertSame(fatal, observed);
    }

    /** 自定义枚举值读取使用应用 codec 时也必须保留其包装的 JVM 致命错误。 */
    @Test
    void preservesNestedVirtualMachineErrorFromEnumValueCodec() {
        OutOfMemoryError fatal = new OutOfMemoryError("enum codec fatal");
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(failingCodec(Integer.class, fatal));

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> MappingPlan.of(CodedStateRecord.class, codecs).map(row("state", "1")));

        assertSame(fatal, observed);
    }

    private static ValueCodec failingCodec(Class<?> supportedType, OutOfMemoryError fatal) {
        return new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == supportedType;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                throw new IllegalArgumentException("codec wrapper", fatal);
            }
        };
    }

    @Test
    void appliesEnumNameStorageAndSkipsJavaTransientPropertiesInBothDirections() {
        FlyingMappedBean source = new FlyingMappedBean();
        source.state = Status.DISABLED;
        source.displayName = "not persisted";

        Map<String, Object> values = EntityValues.createUncached(FlyingMappedBean.class).read(source);
        FlyingMappedBean restored = RowMapper.of(FlyingMappedBean.class)
                                             .map(row("state", "DISABLED", "display_name", "ignored"));

        assertEquals(Map.of("state", "DISABLED"), values);
        assertEquals(Status.DISABLED, restored.state);
        assertEquals(null, restored.displayName);
    }

    @Test
    void publishesWriteAndReadEventsWithoutPuttingListenerStateIntoCachedPlans() {
        AtomicInteger beforeWrite = new AtomicInteger();
        AtomicInteger afterRead = new AtomicInteger();
        EntityMappingListener listener = new EntityMappingListener() {
            @Override
            public void beforeWrite(EntityMappingEvent event) {
                beforeWrite.incrementAndGet();
                assertEquals(AnnotatedBeanUser.class, event.metadata().type());
            }

            @Override
            public void afterRead(EntityMappingEvent event) {
                afterRead.incrementAndGet();
                assertEquals(AnnotatedBeanUser.class, event.metadata().type());
            }
        };

        AnnotatedBeanUser source = new AnnotatedBeanUser();
        source.id = 21L;
        EntityValues.createUncached(AnnotatedBeanUser.class).read(source, listener);
        RowMapper.of(AnnotatedBeanUser.class, listener).map(row("id", 21));

        assertEquals(1, beforeWrite.get());
        assertEquals(1, afterRead.get());
    }

    /** 映射监听器包装的 JVM 致命错误在写前和读后边界都必须恢复为原对象。 */
    @Test
    void preservesNestedVirtualMachineErrorFromEntityMappingListener() {
        OutOfMemoryError beforeWriteFatal = new OutOfMemoryError("before-write fatal");
        RuntimeException beforeWriteFailure = new IllegalStateException("listener wrapper", beforeWriteFatal);
        AnnotatedBeanUser source = new AnnotatedBeanUser();

        OutOfMemoryError observedBeforeWrite = assertThrows(
                OutOfMemoryError.class,
                () -> EntityValues.createUncached(AnnotatedBeanUser.class)
                                  .read(source, new EntityMappingListener() {
                                      @Override
                                      public void beforeWrite(EntityMappingEvent event) {
                                          throw beforeWriteFailure;
                                      }
                                  }));
        assertSame(beforeWriteFatal, observedBeforeWrite);

        OutOfMemoryError afterReadFatal = new OutOfMemoryError("after-read fatal");
        RuntimeException afterReadFailure = new IllegalStateException("listener wrapper", afterReadFatal);
        RowMapper<AnnotatedBeanUser> mapper = RowMapper.of(
                AnnotatedBeanUser.class,
                new EntityMappingListener() {
                    @Override
                    public void afterRead(EntityMappingEvent event) {
                        throw afterReadFailure;
                    }
                });

        OutOfMemoryError observedAfterRead = assertThrows(
                OutOfMemoryError.class,
                () -> mapper.map(row("id", 21L)));
        assertSame(afterReadFatal, observedAfterRead);
    }

    /** 映射事件冻结数组元素，但实际 SQL 写入值继续保留既有的低层零复制交接。 */
    @Test
    void snapshotsArrayValuesAtTheMappingEventBoundaryWithoutCopyingWriteParameters() {
        byte[] payload = {1, 2, 3};
        LargeObjectRecord source = new LargeObjectRecord(payload, "content");
        AtomicReference<EntityMappingEvent> captured = new AtomicReference<>();

        Map<String, Object> writeValues = EntityValues.createUncached(LargeObjectRecord.class)
                                                       .read(source, new EntityMappingListener() {
                                                           @Override
                                                           public void beforeWrite(EntityMappingEvent event) {
                                                               captured.set(event);
                                                           }
                                                       });
        byte[] exposedEventValue = (byte[]) captured.get().values().get("payload");

        assertNotSame(payload, exposedEventValue);
        assertSame(payload, writeValues.get("payload"));
        exposedEventValue[0] = 9;
        payload[1] = 8;
        assertArrayEquals(new byte[]{1, 2, 3},
                          (byte[]) captured.get().values().get("payload"));
    }

    /** 映射事件递归冻结标准 JSON 容器，但真正交给 SQL 的实体值继续沿用原对象。 */
    @Test
    @SuppressWarnings("unchecked")
    void snapshotsNestedJsonContainersAtTheMappingEventBoundary() {
        List<Object> roles = new ArrayList<>(List.of("user"));
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("roles", roles);
        Collection<Object> queue = new ArrayDeque<>(List.of("queued"));
        attributes.put("queue", queue);
        List<Object> tags = new ArrayList<>(List.of("stable"));
        JsonProfile source = new JsonProfile(attributes, tags);
        AtomicReference<EntityMappingEvent> captured = new AtomicReference<>();

        Map<String, Object> writeValues = EntityValues.createUncached(JsonProfile.class)
                                                       .read(source, new EntityMappingListener() {
                                                           @Override
                                                           public void beforeWrite(EntityMappingEvent event) {
                                                               captured.set(event);
                                                           }
                                                       });
        Map<String, Object> exposedAttributes =
                (Map<String, Object>) captured.get().values().get("attributes");
        List<Object> exposedRoles = (List<Object>) exposedAttributes.get("roles");
        Collection<Object> exposedQueue = (Collection<Object>) exposedAttributes.get("queue");
        List<Object> exposedTags = (List<Object>) captured.get().values().get("tags");

        try {
            exposedRoles.add("listener");
        } catch (UnsupportedOperationException ignored) {
            // 只读容器可以直接拒绝；可修改副本也必须与真实写入参数隔离。
        }
        try {
            exposedTags.add("listener");
        } catch (UnsupportedOperationException ignored) {
            // 同上。
        }
        try {
            exposedQueue.add("listener");
        } catch (UnsupportedOperationException ignored) {
            // 同上。
        }
        roles.add("caller");
        tags.add("caller");
        queue.add("caller");

        assertSame(attributes, writeValues.get("attributes"));
        assertSame(tags, writeValues.get("tags"));
        Map<String, Object> stableAttributes =
                (Map<String, Object>) captured.get().values().get("attributes");
        assertEquals(List.of("user"), stableAttributes.get("roles"));
        assertEquals(List.of("queued"), List.copyOf((Collection<?>) stableAttributes.get("queue")));
        assertEquals(List.of("stable"), captured.get().values().get("tags"));
    }

    /** 映射事件隔离 Jackson 树节点，实际 SQL 写入值仍保留实体提供的节点对象。 */
    @Test
    void snapshotsJsonNodesAtTheMappingEventBoundary() {
        ObjectNode attributes = JsonNodeFactory.instance.objectNode().put("role", "user");
        JsonNodeProfile source = new JsonNodeProfile(attributes);
        AtomicReference<EntityMappingEvent> captured = new AtomicReference<>();

        Map<String, Object> writeValues = EntityValues.createUncached(JsonNodeProfile.class)
                                                       .read(source, new EntityMappingListener() {
                                                           @Override
                                                           public void beforeWrite(EntityMappingEvent event) {
                                                               captured.set(event);
                                                           }
                                                       });
        ObjectNode exposed = (ObjectNode) captured.get().values().get("attributes");
        exposed.put("role", "listener");
        attributes.put("role", "caller");

        assertSame(attributes, writeValues.get("attributes"));
        JsonNode stable = (JsonNode) captured.get().values().get("attributes");
        assertEquals("user", stable.path("role").asText());
    }

    /** 映射事件复制可变 ByteBuffer，实际 SQL 写入仍保持原缓冲区的零复制交接。 */
    @Test
    void snapshotsByteBuffersAtTheMappingEventBoundary() {
        ByteBuffer payload = ByteBuffer.wrap(new byte[]{1, 2, 3, 99});
        payload.limit(3);
        ByteBufferRecord source = new ByteBufferRecord(payload);
        AtomicReference<EntityMappingEvent> captured = new AtomicReference<>();

        Map<String, Object> writeValues = EntityValues.createUncached(ByteBufferRecord.class)
                                                       .read(source, new EntityMappingListener() {
                                                           @Override
                                                           public void beforeWrite(EntityMappingEvent event) {
                                                               captured.set(event);
                                                           }
                                                       });
        ByteBuffer exposed = (ByteBuffer) captured.get().values().get("payload");
        exposed.put(0, (byte) 9);
        payload.put(1, (byte) 8);

        assertSame(payload, writeValues.get("payload"));
        ByteBuffer stable = (ByteBuffer) captured.get().values().get("payload");
        assertEquals(3, stable.capacity());
        assertEquals(1, stable.get(0));
        assertEquals(2, stable.get(1));
        assertEquals(3, stable.get(2));
    }

    /** 具体集合数组冻结后允许安全拓宽数组容器，不能因只读元素类型变化触发 ArrayStoreException。 */
    @Test
    @SuppressWarnings("unchecked")
    void snapshotsConcreteCollectionArraysAtTheMappingEventBoundary() {
        ArrayList<String> roles = new ArrayList<>(List.of("user"));
        ArrayList<?>[] roleGroups = new ArrayList<?>[]{roles};
        Long[] ids = {1L, 2L};
        RecordUser entity = new RecordUser(1, true, Status.ACTIVE,
                                           LocalDateTime.of(2026, 8, 11, 12, 0));

        EntityMappingEvent event = new EntityMappingEvent(
                EntityMetadataResolver.createUncached(RecordUser.class),
                entity,
                Map.of("roleGroups", roleGroups, "ids", ids));
        Object[] exposed = (Object[]) event.values().get("roleGroups");
        try {
            ((List<Object>) exposed[0]).add("listener");
        } catch (UnsupportedOperationException ignored) {
            // 只读容器可以直接拒绝；可修改副本也必须与事件内部快照隔离。
        }
        roles.add("caller");
        ids[0] = 9L;

        Object[] stable = (Object[]) event.values().get("roleGroups");
        assertEquals(List.of("user"), stable[0]);
        assertTrue(event.values().get("ids") instanceof Long[]);
        assertArrayEquals(new Long[]{1L, 2L}, (Long[]) event.values().get("ids"));
    }

    /** 映射事件必须在耗尽 JVM 栈之前稳定拒绝不受控的容器嵌套。 */
    @Test
    void rejectsExcessivelyNestedMappingValuesWithoutStackOverflow() {
        Object nested = "leaf";
        for (int depth = 0; depth < 10_000; depth++) {
            nested = List.of(nested);
        }
        Object unsafeValue = nested;
        RecordUser entity = new RecordUser(1, true, Status.ACTIVE,
                                           LocalDateTime.of(2026, 8, 11, 12, 0));

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> new EntityMappingEvent(
                        EntityMetadataResolver.createUncached(RecordUser.class),
                        entity,
                        Map.of("nested", unsafeValue)));

        assertEquals("entity mapping value nesting exceeds 64", failure.getMessage());
    }

    /** Jackson 树也必须受相同深度保护，不能绕过容器图边界耗尽 JVM 栈。 */
    @Test
    void rejectsExcessivelyNestedJsonMappingValuesWithoutStackOverflow() {
        ObjectNode root = JsonNodeFactory.instance.objectNode();
        ObjectNode current = root;
        for (int depth = 0; depth < 10_000; depth++) {
            ObjectNode child = JsonNodeFactory.instance.objectNode();
            current.set("child", child);
            current = child;
        }
        RecordUser entity = new RecordUser(1, true, Status.ACTIVE,
                                           LocalDateTime.of(2026, 8, 11, 12, 0));

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> new EntityMappingEvent(
                        EntityMetadataResolver.createUncached(RecordUser.class),
                        entity,
                        Map.of("nested", root)));

        assertEquals("entity mapping value nesting exceeds 64", failure.getMessage());
    }

    /** 非法循环 Jackson 图必须稳定拒绝，不能交给递归 deepCopy 形成栈溢出。 */
    @Test
    void rejectsCyclicJsonMappingValuesWithoutStackOverflow() {
        ObjectNode cyclic = JsonNodeFactory.instance.objectNode();
        cyclic.set("self", cyclic);
        RecordUser entity = new RecordUser(1, true, Status.ACTIVE,
                                           LocalDateTime.of(2026, 8, 11, 12, 0));

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> new EntityMappingEvent(
                        EntityMetadataResolver.createUncached(RecordUser.class),
                        entity,
                        Map.of("cyclic", cyclic)));

        assertEquals("entity mapping JSON tree must not contain cycles", failure.getMessage());
    }

    /** 普通 Java 容器图也不能保留自循环，否则后续日志、hashCode 或序列化仍可能耗尽 JVM 栈。 */
    @Test
    void rejectsCyclicStandardContainerMappingValuesWithoutStackOverflow() {
        List<Object> cyclic = new ArrayList<>();
        cyclic.add(cyclic);
        RecordUser entity = new RecordUser(1, true, Status.ACTIVE,
                                           LocalDateTime.of(2026, 8, 11, 12, 0));

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> new EntityMappingEvent(
                        EntityMetadataResolver.createUncached(RecordUser.class),
                        entity,
                        Map.of("cyclic", cyclic)));

        assertEquals("entity mapping value graph must not contain cycles", failure.getMessage());
    }

    /** Map 键参与同一容器图；键回指自身时也必须在触发递归 hashCode 前稳定拒绝。 */
    @Test
    void rejectsCyclicMappingContainerKeysWithoutStackOverflow() {
        Map<Object, Object> cyclic = new LinkedHashMap<>();
        cyclic.put(cyclic, "self");
        RecordUser entity = new RecordUser(1, true, Status.ACTIVE,
                                           LocalDateTime.of(2026, 8, 11, 12, 0));

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> new EntityMappingEvent(
                        EntityMetadataResolver.createUncached(RecordUser.class),
                        entity,
                        Map.of("cyclic", cyclic)));

        assertEquals("entity mapping value graph must not contain cycles", failure.getMessage());
    }

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            values.put((String) pairs[i], pairs[i + 1]);
        }
        return values;
    }

    private enum Status {
        ACTIVE,
        DISABLED
    }

    private record RecordUser(int id, boolean enabled, Status status, LocalDateTime createdAt) {
    }

    private record JsonProfile(Map<String, Object> attributes, List<Object> tags) {
    }

    private record JsonNodeProfile(JsonNode attributes) {
    }

    private record LargeObjectRecord(byte[] payload, String content) {
    }

    private record ByteBufferRecord(ByteBuffer payload) {
    }

    private record ArrayRecord(Long[] ids) {
    }

    private record TextBackedRecord(UUID id, Instant createdAt, OffsetDateTime updatedAt, Character marker) {
    }

    private record Invoice(Money amount) {
    }

    private record CodedStateRecord(CodedState state) {
    }

    private enum CodedState {
        ACTIVE(1);

        @EnumValue
        private final int code;

        CodedState(int code) {
            this.code = code;
        }
    }

    private record Money(BigDecimal amount) {
    }

    private static final class NameOnlyBean {
        private String displayName;
    }

    public static final class BeanUser {

        private long id;

        private boolean enabled;

        private Status status;

        private LocalDateTime createdAt;

        public void setId(long id) {
            this.id = id;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public void setStatus(Status status) {
            this.status = status;
        }

        public void setCreatedAt(LocalDateTime createdAt) {
            this.createdAt = createdAt;
        }
    }

    public static final class AnnotatedBeanUser {

        private Long id;

        @TableField("user_name")
        private String name;

        private Boolean enabled;

        public void setId(Long id) {
            this.id = id;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static final class FlyingMappedBean {

        private Status state;

        private transient String displayName;
    }

    public static final class FatalGetterBean {

        private static OutOfMemoryError failure;

        private String value;

        public String getValue() {
            throw failure;
        }
    }

    public static final class DeepFatalGetterBean {

        private static RuntimeException failure;

        private String value;

        public String getValue() {
            throw failure;
        }
    }

    public static final class FatalSetterBean {

        private static OutOfMemoryError failure;

        private String value;

        public void setValue(String value) {
            throw failure;
        }
    }

    public static final class FatalConstructorBean {

        private static OutOfMemoryError failure;

        private String value;

        public FatalConstructorBean() {
            throw failure;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
