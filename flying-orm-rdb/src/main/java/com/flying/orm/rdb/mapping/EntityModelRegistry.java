package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.cache.BoundedCacheRegion;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCacheSnapshot;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.id.IdGenerator;

import java.util.LinkedHashMap;
import java.util.Map;
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
    private final Map<Class<?>, EntitySchemaDescriptor<?>> entitySchemas;

    private final AtomicBoolean closed = new AtomicBoolean();

    private EntityModelRegistry(CacheRegionPolicy policy,
                                IdGenerator idGenerator,
                                EntityFieldFiller fieldFiller,
                                Map<Class<?>, EntitySchemaDescriptor<?>> entitySchemas) {
        CacheRegionPolicy safePolicy = Objects.requireNonNull(policy,
                                                               "entity mapping cache policy must not be null");
        this.maximumWeight = safePolicy.maximumWeight();
        this.idGenerator = Objects.requireNonNull(idGenerator, "id generator must not be null");
        this.fieldFiller = Objects.requireNonNull(fieldFiller, "entity field filler must not be null");
        this.entitySchemas = copySchemas(entitySchemas);
        models = BoundedCacheRegion.create(safePolicy, (ignored, model) -> logicalWeight(model));
    }

    /**
     * 创建实例级实体模型注册表。
     *
     * @param policy 实体映射缓存区域策略
     * @return 可并发共享的注册表
     */
    public static EntityModelRegistry create(CacheRegionPolicy policy) {
        return new EntityModelRegistry(policy, IdGenerator.none(), EntityFieldFiller.none(), Map.of());
    }

    /** 创建使用显式主键生成器的实例级实体模型注册表。 */
    public static EntityModelRegistry create(CacheRegionPolicy policy, IdGenerator idGenerator) {
        return new EntityModelRegistry(policy, idGenerator, EntityFieldFiller.none(), Map.of());
    }

    /** 创建同时绑定主键生成器和字段填充器的实例级实体模型注册表。 */
    public static EntityModelRegistry create(CacheRegionPolicy policy,
                                             IdGenerator idGenerator,
                                             EntityFieldFiller fieldFiller) {
        return new EntityModelRegistry(policy, idGenerator, fieldFiller, Map.of());
    }

    /**
     * 创建已经绑定完整实体关系描述的注册表。
     *
     * <p>描述在客户端启动期一次注册。之后 metadata、写值计划、行映射和 Schema 都从同一个描述出发；
     * 没有注册的实体仍走原来的 CRUD-only 编译路径。</p>
     */
    @InternalApi
    public static EntityModelRegistry create(
            CacheRegionPolicy policy,
            IdGenerator idGenerator,
            EntityFieldFiller fieldFiller,
            Map<Class<?>, EntitySchemaDescriptor<?>> entitySchemas) {
        return new EntityModelRegistry(policy, idGenerator, fieldFiller, entitySchemas);
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
        EntitySchemaDescriptor<T> schema = registeredSchema(safeType);
        Map<String, EntityTypeMappingRegistry.Mapping> customMappings = schema == null
                ? Map.of() : schema.customFieldMappings();
        ModelKey key = ModelKey.mapping(safeType, safeCodecs);
        return (MappingPlan<T>) model(key,
                                      ignored -> MappingPlan.createUncached(
                                              safeType, metadata, safeCodecs, customMappings));
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
        EntitySchemaDescriptor<T> schema = registeredSchema(safeType);
        if (schema != null) {
            return schema.metadata();
        }
        return (EntityMetadata<T>) model(ModelKey.metadata(safeType),
                                         ignored -> EntityMetadataResolver.createUncached(safeType));
    }

    /**
     * 使用框架标准类型映射获取实体的严格 Schema 描述。
     *
     * <p>启动期注册过完整描述时直接返回同一对象；未注册实体才在显式 Schema 冷路径按标准映射编译。</p>
     */
    public <T> EntitySchemaDescriptor<T> schemaDescriptor(Class<T> type) {
        Class<T> safeType = Objects.requireNonNull(type, "entity type must not be null");
        EntitySchemaDescriptor<T> schema = registeredSchema(safeType);
        if (schema != null) {
            return schema;
        }
        return schemaDescriptor(safeType, EntityTypeMappingRegistry.standard());
    }

    /**
     * 使用指定类型映射获取或编译严格 Schema 描述。
     *
     * <p>缓存按注册表对象身份隔离；即使两个注册表具有相同稳定指纹，也不会跨应用生命周期共享
     * codec 或反射模型。</p>
     */
    @SuppressWarnings("unchecked")
    public <T> EntitySchemaDescriptor<T> schemaDescriptor(Class<T> type,
                                                           EntityTypeMappingRegistry typeMappings) {
        Class<T> safeType = Objects.requireNonNull(type, "entity type must not be null");
        EntityTypeMappingRegistry safeMappings = Objects.requireNonNull(
                typeMappings, "entity type mappings must not be null");
        EntitySchemaDescriptor<T> schema = registeredSchema(safeType);
        if (schema != null && schema.typeMappings() == safeMappings) {
            return schema;
        }
        ModelKey key = ModelKey.schemaDescriptor(safeType, safeMappings);
        return (EntitySchemaDescriptor<T>) model(
                key,
                ignored -> EntitySchemaDescriptor.builder(safeType)
                        .typeMappings(safeMappings)
                        .build());
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

    /**
     * 返回启动期注册 descriptor 中数据库生成主键的精确自定义映射。未注册实体和标准类型返回 null，
     * Repository 创建后会缓存结果，逐次回填不扫描元数据。
     */
    @InternalApi
    public EntityTypeMappingRegistry.Mapping databaseGeneratedKeyMapping(Class<?> type) {
        Class<?> safeType = Objects.requireNonNull(type, "entity type must not be null");
        EntitySchemaDescriptor<?> schema = registeredSchema(safeType);
        if (schema == null) {
            return null;
        }
        return schema.metadata().fields().stream()
                .filter(field -> field.primaryKey() && field.generation().generated())
                .findFirst()
                .map(field -> schema.customFieldMappings().get(field.name()))
                .orElse(null);
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

    @SuppressWarnings("unchecked")
    private <T> EntitySchemaDescriptor<T> registeredSchema(Class<T> type) {
        if (entitySchemas.isEmpty()) {
            return null;
        }
        return (EntitySchemaDescriptor<T>) entitySchemas.get(type);
    }

    private static Map<Class<?>, EntitySchemaDescriptor<?>> copySchemas(
            Map<Class<?>, EntitySchemaDescriptor<?>> schemas) {
        Map<Class<?>, EntitySchemaDescriptor<?>> safeSchemas = Objects.requireNonNull(
                schemas, "entity schemas must not be null");
        if (safeSchemas.isEmpty()) {
            return Map.of();
        }
        Map<Class<?>, EntitySchemaDescriptor<?>> copy = new LinkedHashMap<>(safeSchemas.size());
        safeSchemas.forEach((type, schema) -> {
            Class<?> safeType = Objects.requireNonNull(type, "entity schema type must not be null");
            EntitySchemaDescriptor<?> safeSchema = Objects.requireNonNull(
                    schema, "entity schema descriptor must not be null");
            if (safeSchema.metadata().type() != safeType) {
                throw new IllegalArgumentException("entity schema key must match its entity type");
            }
            copy.put(safeType, safeSchema);
        });
        return Map.copyOf(copy);
    }

    private static int logicalWeight(Object model) {
        if (model instanceof MappingPlan<?> plan) {
            return plan.logicalWeight();
        }
        if (model instanceof EntityMetadata<?> metadata) {
            return metadata.logicalWeight();
        }
        if (model instanceof EntitySchemaDescriptor<?> descriptor) {
            return descriptor.metadata().logicalWeight();
        }
        if (model instanceof EntityValues<?> values) {
            return values.logicalWeight();
        }
        return 1;
    }

    /** 使用对象身份实现键相等，不能让不同类别、codec 或类型映射生命周期的模型错误共享计划。 */
    private static final class ModelKey {

        private final Kind kind;
        private final Class<?> type;
        private final Object modelIdentity;
        private final int hash;

        private ModelKey(Kind kind, Class<?> type, Object modelIdentity) {
            this.kind = kind;
            this.type = type;
            this.modelIdentity = modelIdentity;
            int modelHash = System.identityHashCode(modelIdentity);
            if (modelIdentity instanceof ValueCodecRegistry codecs && codecs.hasDescriptors()) {
                modelHash = 31 * modelHash + codecs.descriptorFingerprint().hashCode();
            }
            this.hash = 31 * (31 * kind.hashCode() + System.identityHashCode(type)) + modelHash;
        }

        private ModelKey(Class<?> type, EntityTypeMappingRegistry typeMappings) {
            this.kind = Kind.SCHEMA_DESCRIPTOR;
            this.type = type;
            this.modelIdentity = typeMappings;
            int identityHash = 31 * (31 * kind.hashCode() + System.identityHashCode(type))
                    + System.identityHashCode(typeMappings);
            this.hash = 31 * identityHash + typeMappings.fingerprint().hashCode();
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

        private static ModelKey schemaDescriptor(Class<?> type, EntityTypeMappingRegistry typeMappings) {
            return new ModelKey(type, typeMappings);
        }

        @Override
        public boolean equals(Object other) {
            return this == other || other instanceof ModelKey key
                    && kind == key.kind
                    && type == key.type
                    && modelIdentity == key.modelIdentity;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    private enum Kind {
        METADATA,
        VALUES,
        ROW_MAPPING,
        SCHEMA_DESCRIPTOR
    }
}
