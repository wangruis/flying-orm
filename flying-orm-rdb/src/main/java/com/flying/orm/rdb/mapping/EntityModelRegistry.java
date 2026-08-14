package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.cache.BoundedCacheRegion;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCacheSnapshot;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.id.IdGenerator;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * 单套 ORM 客户端持有的实体映射计划注册表。
 *
 * <p>注册表以“实体 Class 身份＋codec 注册表身份”为键，保证不同应用客户端的转换规则互不污染；容量、
 * 单条重量、访问后过期和统计完全遵守 {@link CacheRegionPolicy}。关闭区域时每次直接编译，不保留静态 ClassValue
 * 或固定 64 条的隐藏缓存，因此用户配置是真实运行契约，也不会由全局静态状态长期持有应用类。</p>
 *
 * <p>实例构造后没有请求级可变状态，可由响应式、同步、Repository 和上层单例安全共享。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class EntityModelRegistry implements AutoCloseable {

    private final BoundedCacheRegion<ModelKey, Object> models;

    private final long maximumWeight;
    private final IdGenerator idGenerator;
    private final EntityFieldFiller fieldFiller;

    private final AtomicBoolean closed = new AtomicBoolean();

    private EntityModelRegistry(CacheRegionPolicy policy, IdGenerator idGenerator, EntityFieldFiller fieldFiller) {
        CacheRegionPolicy safePolicy = Objects.requireNonNull(policy,
                                                               "entity mapping cache policy must not be null");
        this.maximumWeight = safePolicy.maximumWeight();
        this.idGenerator = Objects.requireNonNull(idGenerator, "id generator must not be null");
        this.fieldFiller = Objects.requireNonNull(fieldFiller, "entity field filler must not be null");
        models = BoundedCacheRegion.create(safePolicy, (ignored, model) -> logicalWeight(model));
    }

    /**
     * 创建实例级实体模型注册表。
     *
     * @param policy 实体映射缓存区域策略
     * @return 可并发共享的注册表
     */
    public static EntityModelRegistry create(CacheRegionPolicy policy) {
        return new EntityModelRegistry(policy, IdGenerator.none(), EntityFieldFiller.none());
    }

    /** 创建使用显式主键生成器的实例级实体模型注册表。 */
    public static EntityModelRegistry create(CacheRegionPolicy policy, IdGenerator idGenerator) {
        return new EntityModelRegistry(policy, idGenerator, EntityFieldFiller.none());
    }

    /** 创建同时绑定主键生成器和字段填充器的实例级实体模型注册表。 */
    public static EntityModelRegistry create(CacheRegionPolicy policy,
                                             IdGenerator idGenerator,
                                             EntityFieldFiller fieldFiller) {
        return new EntityModelRegistry(policy, idGenerator, fieldFiller);
    }

    /** Repository 读取当前客户端绑定的生成器；普通映射调用不会触发它。 */
    @InternalApi
    public IdGenerator idGenerator() {
        return idGenerator;
    }

    /**
     * 获取或编译目标类型的行映射器。codec 注册表按对象身份隔离，避免自定义转换规则串到另一客户端。
     *
     * @param type 目标实体类型
     * @param valueCodecs 应用级 codec 注册表
     * @param <T> 实体类型
     * @return 不可变、可并发复用的映射器
     */
    @SuppressWarnings("unchecked")
    public <T> RowMapper<T> rowMapper(Class<T> type, ValueCodecRegistry valueCodecs) {
        Class<T> safeType = Objects.requireNonNull(type, "mapping type must not be null");
        ValueCodecRegistry safeCodecs = Objects.requireNonNull(valueCodecs,
                                                               "value codec registry must not be null");
        EntityMetadata<T> metadata = metadata(safeType);
        ModelKey key = ModelKey.mapping(safeType, safeCodecs);
        return (MappingPlan<T>) model(key,
                                      ignored -> MappingPlan.createUncached(safeType, metadata, safeCodecs));
    }

    /**
     * 获取或编译实体约定元数据，容量和过期策略与行映射计划共用同一真实边界。
     *
     * @param type 实体类型
     * @param <T> 实体类型
     * @return 不可变实体元数据
     */
    @SuppressWarnings("unchecked")
    public <T> EntityMetadata<T> metadata(Class<T> type) {
        Class<T> safeType = Objects.requireNonNull(type, "entity type must not be null");
        return (EntityMetadata<T>) model(ModelKey.metadata(safeType),
                                         ignored -> EntityMetadataResolver.createUncached(safeType));
    }

    /**
     * 获取或编译实体写入取值计划，计划复用当前注册表中的同一份实体元数据。
     *
     * @param type 实体类型
     * @param <T> 实体类型
     * @return 可并发复用的实体取值计划
     */
    @SuppressWarnings("unchecked")
    @InternalApi
    public <T> EntityValues<T> entityValues(Class<T> type) {
        Class<T> safeType = Objects.requireNonNull(type, "entity type must not be null");
        EntityMetadata<T> metadata = metadata(safeType);
        return (EntityValues<T>) model(ModelKey.values(safeType),
                                       ignored -> EntityValues.createUncached(safeType, metadata, fieldFiller));
    }

    /** @return 当前近似缓存条目数；区域关闭时始终为零。 */
    public long estimatedMappings() {
        return closed.get() ? 0L : models.snapshot().estimatedSize();
    }

    /** @return 不依赖监控框架的缓存统计快照。 */
    public OrmCacheSnapshot stats() {
        return closed.get() ? emptySnapshot() : models.snapshot();
    }

    /**
     * 清空当前实例持有的全部实体元数据、写值计划和行映射计划。
     *
     * <p>该操作不会关闭注册表，后续访问仍可按当前策略重新加载。它适合类模型热更新或上层应用在明确
     * 生命周期边界上主动释放应用 Class 引用。方法可并发调用并且幂等。</p>
     */
    public void invalidateAll() {
        models.invalidateAll();
    }

    /**
     * 永久关闭当前注册表并释放已经缓存的应用 Class、codec 和反射计划引用。
     *
     * <p>关闭是幂等且线程安全的。关闭后公开映射能力仍可使用，但每次直接编译并且不再保留结果，
     * 从而避免应用生命周期结束后重新把 Class 写回缓存。并发加载与关闭竞争时，加载方会在返回结果前
     * 再检查关闭状态并删除可能刚写入的条目。</p>
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            models.invalidateAll();
        }
    }

    private Object model(ModelKey key, Function<ModelKey, Object> compiler) {
        if (closed.get()) {
            return Objects.requireNonNull(compiler.apply(key), "entity model compiler must not return null");
        }
        Object model = models.get(key, compiler);
        if (closed.get()) {
            // close 可能发生在第一次检查与 Caffeine 原子加载之间；二次检查保证关闭后不再残留 Class 引用。
            models.invalidate(key);
        }
        return model;
    }

    private OrmCacheSnapshot emptySnapshot() {
        return new OrmCacheSnapshot(0L, 0L, maximumWeight,
                                    0L, 0L, 1D,
                                    0L, 0L, 0L,
                                    0L, 0L, 0L);
    }

    private static int logicalWeight(Object model) {
        if (model instanceof MappingPlan<?> plan) {
            return plan.logicalWeight();
        }
        if (model instanceof EntityMetadata<?> metadata) {
            return metadata.logicalWeight();
        }
        if (model instanceof EntityValues<?> values) {
            return values.logicalWeight();
        }
        return 1;
    }

    /** 使用对象身份实现键相等，不能让不同类别或不同 codec 生命周期的模型错误共享计划。 */
    private static final class ModelKey {

        private final Kind kind;
        private final Class<?> type;
        private final ValueCodecRegistry valueCodecs;
        private final int hash;

        private ModelKey(Kind kind, Class<?> type, ValueCodecRegistry valueCodecs) {
            this.kind = kind;
            this.type = type;
            this.valueCodecs = valueCodecs;
            this.hash = 31 * (31 * kind.hashCode() + System.identityHashCode(type))
                    + System.identityHashCode(valueCodecs);
        }

        private static ModelKey mapping(Class<?> type, ValueCodecRegistry codecs) {
            return new ModelKey(Kind.ROW_MAPPING, type, codecs);
        }

        private static ModelKey metadata(Class<?> type) {
            return new ModelKey(Kind.METADATA, type, null);
        }

        private static ModelKey values(Class<?> type) {
            return new ModelKey(Kind.VALUES, type, null);
        }

        @Override
        public boolean equals(Object other) {
            return this == other || other instanceof ModelKey key
                    && kind == key.kind
                    && type == key.type
                    && valueCodecs == key.valueCodecs;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    private enum Kind {
        METADATA,
        VALUES,
        ROW_MAPPING
    }
}
