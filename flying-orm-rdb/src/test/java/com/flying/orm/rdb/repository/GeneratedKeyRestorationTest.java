package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证批量生成键回填失败后的恢复完整性。
 *
 * <p>同步和响应式保留表都必须尽力恢复全部尚未确认的实体键。某个 setter 失败时，后续实体仍须恢复，
 * 内部引用也必须释放。该测试同时固定反射写入把 VME 包装为映射异常时，边界仍原样传播 VME 的契约。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class GeneratedKeyRestorationTest {

    /** 非法 UUID 生成键必须停留在统一映射异常边界，不能把 JDK 解析异常直接暴露给调用方。 */
    @Test
    void rejectsMalformedUuidGeneratedKeyThroughTheMappingBoundary() {
        EntityMetadata<UuidGeneratedEntity> metadata = metadata(UuidGeneratedEntity.class);
        RepositoryEntityIdSupport<UuidGeneratedEntity> support =
                RepositoryEntityIdSupport.create(metadata, IdGenerator.none());
        UuidGeneratedEntity entity = new UuidGeneratedEntity("device");

        MappingException failure = assertThrows(
                MappingException.class,
                () -> support.applyGeneratedKey(entity, DynamicRow.copyOf(Map.of("device_id", "not-a-uuid"))));

        assertEquals("generated primary key cannot be converted to java.util.UUID", failure.getMessage());
        assertNull(failure.getCause());
        assertNull(entity.getDeviceId());
    }

    /** 单实体 insert 只能接收一行生成键，不能在多行驱动结果中静默选择第一行。 */
    @Test
    void rejectsAmbiguousGeneratedKeyRowsForSingleEntityInsert() {
        EntityMetadata<RestoreEntity> metadata = metadata(RestoreEntity.class);
        RepositoryEntityIdSupport<RestoreEntity> support =
                RepositoryEntityIdSupport.create(metadata, IdGenerator.none());
        RestoreEntity entity = new RestoreEntity("device", null);
        SqlWriteResult result = new SqlWriteResult(1L, List.of(generatedKey(101L), generatedKey(102L)));

        MappingException failure = assertThrows(
                MappingException.class,
                () -> support.applyGeneratedKey(entity, result));

        assertEquals("database must return exactly one generated primary key", failure.getMessage());
        assertNull(entity.getDeviceId());
    }

    /** 自定义 ID 生成器包装的 JVM 致命错误必须保持原对象，不能降级成普通映射失败。 */
    @Test
    void preservesNestedVirtualMachineErrorFromAssignedIdGenerator() {
        OutOfMemoryError fatal = new OutOfMemoryError("id generator fatal");
        EntityMetadata<AssignedEntity> metadata = metadata(AssignedEntity.class);
        RepositoryEntityIdSupport<AssignedEntity> support = RepositoryEntityIdSupport.create(
                metadata, (entity, property, type) -> {
                    throw new IllegalStateException("generator wrapper", fatal);
                });

        Throwable observed = assertThrows(Throwable.class,
                                          () -> support.prepare(new AssignedEntity()));

        assertSame(fatal, observed);
    }

    /** 同步 abort 不能因第一个实体的恢复失败而遗漏其余实体或保留内部引用。 */
    @Test
    void syncLifecycleRestoresRemainingKeysAndClearsAfterNestedVirtualMachineError() {
        OutOfMemoryError failure = new OutOfMemoryError("first restore failure");
        RestoreEntity first = new RestoreEntity("first", failure);
        RestoreEntity second = new RestoreEntity("second", null);
        SyncRepositoryBatchLifecycle<RestoreEntity> lifecycle = syncLifecycle();
        lifecycle.remember(0L, first);
        lifecycle.remember(1L, second);
        lifecycle.generatedKeys().accept(0L, generatedKey(101L));
        lifecycle.generatedKeys().accept(1L, generatedKey(102L));

        Throwable observed = assertThrows(Throwable.class, lifecycle::abort);

        assertAll(
                () -> assertSame(failure, observed),
                () -> assertEquals(101L, first.getDeviceId()),
                () -> assertNull(second.getDeviceId()),
                () -> assertDoesNotThrow(lifecycle::abort));
    }

    /** 响应式 tracker 的取消清理与同步路径保持同一恢复和释放契约。 */
    @Test
    void reactiveTrackerRestoresRemainingKeysAndClearsAfterNestedVirtualMachineError() {
        OutOfMemoryError failure = new OutOfMemoryError("first restore failure");
        RestoreEntity first = new RestoreEntity("first", failure);
        RestoreEntity second = new RestoreEntity("second", null);
        BatchLifecycleTracker<RestoreEntity> tracker = reactiveTracker();
        Flux.from(tracker.rows(Flux.just(first, second), ignored -> Map.of(), EntityLifecyclePhase.PRE_PERSIST))
               .then()
               .block();
        tracker.generatedKeys().accept(0L, generatedKey(101L));
        tracker.generatedKeys().accept(1L, generatedKey(102L));

        Throwable observed = assertThrows(Throwable.class, tracker::abort);

        assertAll(
                () -> assertSame(failure, observed),
                () -> assertEquals(101L, first.getDeviceId()),
                () -> assertNull(second.getDeviceId()),
                () -> assertDoesNotThrow(tracker::abort));
    }

    /** 反射 setter 在写入生成键时包装 VME，仍必须在恢复后保留原始 VME 身份。 */
    @Test
    void syncLifecyclePropagatesNestedVirtualMachineErrorFromGeneratedKeyApplication() {
        OutOfMemoryError failure = new OutOfMemoryError("generated key application failure");
        ApplyFailingEntity entity = new ApplyFailingEntity("device", failure);
        SyncRepositoryBatchLifecycle<ApplyFailingEntity> lifecycle = syncApplyFailingLifecycle();
        lifecycle.remember(0L, entity);

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> lifecycle.generatedKeys().accept(0L, generatedKey(101L)));

        assertSame(failure, observed);
        assertNull(entity.getDeviceId());
        assertDoesNotThrow(lifecycle::abort);
    }

    /** 响应式 tracker 在生成键应用阶段也不能把嵌套 VME 降级为 MappingException。 */
    @Test
    void reactiveTrackerPropagatesNestedVirtualMachineErrorFromGeneratedKeyApplication() {
        OutOfMemoryError failure = new OutOfMemoryError("generated key application failure");
        ApplyFailingEntity entity = new ApplyFailingEntity("device", failure);
        BatchLifecycleTracker<ApplyFailingEntity> tracker = reactiveApplyFailingTracker();
        Flux.from(tracker.rows(Flux.just(entity), ignored -> Map.of(), EntityLifecyclePhase.PRE_PERSIST))
               .then()
               .block();

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> tracker.generatedKeys().accept(0L, generatedKey(101L)));

        assertSame(failure, observed);
        assertNull(entity.getDeviceId());
        assertDoesNotThrow(tracker::abort);
    }

    private static SyncRepositoryBatchLifecycle<RestoreEntity> syncLifecycle() {
        EntityMetadata<RestoreEntity> metadata = metadata(RestoreEntity.class);
        return new SyncRepositoryBatchLifecycle<>(
                new SyncRepositoryLifecycleSupport<>(metadata, null, new SyncRepositoryAwaiter(Duration.ofSeconds(1))),
                EntityLifecyclePhase.POST_PERSIST,
                ignored -> Map.of(),
                RepositoryEntityIdSupport.create(metadata, IdGenerator.none()),
                true,
                4096L);
    }

    private static BatchLifecycleTracker<RestoreEntity> reactiveTracker() {
        EntityMetadata<RestoreEntity> metadata = metadata(RestoreEntity.class);
        return new BatchLifecycleTracker<>(
                new EntityLifecycleDispatcher<>(metadata, null),
                EntityLifecyclePhase.POST_PERSIST,
                RepositoryEntityIdSupport.create(metadata, IdGenerator.none()),
                true,
                4096L,
                ignored -> 64L);
    }

    private static SyncRepositoryBatchLifecycle<ApplyFailingEntity> syncApplyFailingLifecycle() {
        EntityMetadata<ApplyFailingEntity> metadata = metadata(ApplyFailingEntity.class);
        return new SyncRepositoryBatchLifecycle<>(
                new SyncRepositoryLifecycleSupport<>(metadata, null, new SyncRepositoryAwaiter(Duration.ofSeconds(1))),
                EntityLifecyclePhase.POST_PERSIST,
                ignored -> Map.of(),
                RepositoryEntityIdSupport.create(metadata, IdGenerator.none()),
                true,
                4096L);
    }

    private static BatchLifecycleTracker<ApplyFailingEntity> reactiveApplyFailingTracker() {
        EntityMetadata<ApplyFailingEntity> metadata = metadata(ApplyFailingEntity.class);
        return new BatchLifecycleTracker<>(
                new EntityLifecycleDispatcher<>(metadata, null),
                EntityLifecyclePhase.POST_PERSIST,
                RepositoryEntityIdSupport.create(metadata, IdGenerator.none()),
                true,
                4096L,
                ignored -> 64L);
    }

    private static <T> EntityMetadata<T> metadata(Class<T> type) {
        return EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()).metadata(type);
    }

    private static DynamicRow generatedKey(long value) {
        return DynamicRow.copyOf(Map.of("device_id", value));
    }

    @TableName("device")
    private static final class RestoreEntity {

        @TableId(value = "device_id", type = IdType.AUTO)
        private Long deviceId;

        @TableField("device_name")
        private final String name;

        @TableField(exist = false)
        private final OutOfMemoryError restoreFailure;

        private RestoreEntity(String name, OutOfMemoryError restoreFailure) {
            this.name = name;
            this.restoreFailure = restoreFailure;
        }

        public Long getDeviceId() {
            return deviceId;
        }

        public void setDeviceId(Long deviceId) {
            if (deviceId == null && restoreFailure != null) {
                throw restoreFailure;
            }
            this.deviceId = deviceId;
        }

        public String getName() {
            return name;
        }
    }

    @TableName("device")
    private static final class ApplyFailingEntity {

        @TableId(value = "device_id", type = IdType.AUTO)
        private Long deviceId;

        @TableField("device_name")
        private final String name;

        @TableField(exist = false)
        private final OutOfMemoryError applicationFailure;

        private ApplyFailingEntity(String name, OutOfMemoryError applicationFailure) {
            this.name = name;
            this.applicationFailure = applicationFailure;
        }

        public Long getDeviceId() {
            return deviceId;
        }

        public void setDeviceId(Long deviceId) {
            if (deviceId != null) {
                throw applicationFailure;
            }
            this.deviceId = null;
        }

        public String getName() {
            return name;
        }
    }

    @TableName("device")
    private static final class UuidGeneratedEntity {

        @TableId(value = "device_id", type = IdType.AUTO)
        private UUID deviceId;

        @TableField("device_name")
        private final String name;

        private UuidGeneratedEntity(String name) {
            this.name = name;
        }

        public UUID getDeviceId() {
            return deviceId;
        }

        public void setDeviceId(UUID deviceId) {
            this.deviceId = deviceId;
        }

        public String getName() {
            return name;
        }
    }

    @TableName("assigned_device")
    private static final class AssignedEntity {

        @TableId(value = "device_id", type = IdType.ASSIGN_ID)
        private Long deviceId;

        public Long getDeviceId() {
            return deviceId;
        }

        public void setDeviceId(Long deviceId) {
            this.deviceId = deviceId;
        }
    }
}
