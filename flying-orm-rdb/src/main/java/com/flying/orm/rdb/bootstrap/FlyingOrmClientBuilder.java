package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.RdbDialectResolver;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionLogObserver;
import com.flying.orm.rdb.observation.SqlExecutionLogOptions;
import com.flying.orm.rdb.observation.SqlExecutionLogSelection;
import com.flying.orm.rdb.observation.SqlExecutionLogSink;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservers;
import com.flying.orm.rdb.protection.MaskingPolicyRegistry;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedValueNormalizerRegistry;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.schema.SchemaMigrationObserver;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.template.SqlTemplateParameterProvider;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import com.flying.orm.rdb.template.SyncSqlTemplateParameterProvider;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 启动期配置与构建入口：持有一份配置，在 build 时选择执行器并交由装配器创建客户端。
 * 构建器不进入 SQL 执行路径，构建结果不引用其可变配置。
 *
 * @author wangr
 * @version v2.0.0
 */
public final class FlyingOrmClientBuilder {
    private final R2dbcConnectionAccess reactiveConnectionAccess;
    private final JdbcConnectionAccess jdbcConnectionAccess;
    private final ReactiveSqlExecutor customReactiveExecutor;
    private JdbcFormMetadataReader jdbcMetadata;
    private ReactiveFormMetadataReader reactiveMetadata;
    private RdbDialect configuredDialectValue;
    private SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
    private String configuredDialect;
    private StructuredConditionResolver resolver;
    private SqlExecutionOptions executionOptions = SqlExecutionOptions.unlimited();
    private BatchWriteOptions batchWriteOptions;
    private BatchMemoryLimits batchMemoryLimits = BatchMemoryLimits.defaults();
    private OrmCachePolicy cachePolicy = OrmCachePolicy.safeDefaults();
    private IdGenerator idGenerator = IdGenerator.none();
    private EntityFieldFiller fieldFiller = EntityFieldFiller.none();
    private Map<Class<?>, EntitySchemaDescriptor<?>> entitySchemas = Map.of();
    private EntityTypeMappingRegistry sharedEntityTypeMappings;
    private ProtectedFieldKeyRing protectedFieldKeys;
    private ProtectedValueNormalizerRegistry protectedValueNormalizers = ProtectedValueNormalizerRegistry.standard();
    private MaskingPolicyRegistry maskingPolicies = MaskingPolicyRegistry.standard();
    private SqlExecutionOptions migrationOptions;
    private SchemaMigrationObserver migrationObserver;
    private SqlTemplateRegistry templates = SqlTemplateRegistry.builder().build();
    private SqlTemplateParameterProvider reactiveParameters = SqlTemplateParameterProvider.none();
    private SyncSqlTemplateParameterProvider syncParameters = SyncSqlTemplateParameterProvider.none();
    private SqlExecutionObserver sqlObserver;
    private BatchExecutionObserver batchObserver;
    private SqlExecutionLogObserver logObserver;
    private boolean protectedFieldKeysTransferred;

    private FlyingOrmClientBuilder(JdbcConnectionAccess jdbcConnectionAccess, R2dbcConnectionAccess reactiveConnectionAccess) {
        this.jdbcConnectionAccess = jdbcConnectionAccess;
        this.reactiveConnectionAccess = reactiveConnectionAccess;
        this.customReactiveExecutor = null;
        this.configuredDialectValue = null;
    }

    private FlyingOrmClientBuilder(ReactiveSqlExecutor executor, RdbDialect dialect) {
        this.jdbcConnectionAccess = null;
        this.reactiveConnectionAccess = null;
        this.customReactiveExecutor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.configuredDialectValue = Objects.requireNonNull(dialect, "rdb dialect must not be null");
    }

    static FlyingOrmClientBuilder reactive(R2dbcConnectionAccess cf) {
        return new FlyingOrmClientBuilder(null, Objects.requireNonNull(cf));
    }

    static FlyingOrmClientBuilder jdbc(JdbcConnectionAccess ds) {
        return new FlyingOrmClientBuilder(Objects.requireNonNull(ds), null);
    }

    static FlyingOrmClientBuilder dual(JdbcConnectionAccess ds, R2dbcConnectionAccess cf) {
        return new FlyingOrmClientBuilder(Objects.requireNonNull(ds), Objects.requireNonNull(cf));
    }

    static FlyingOrmClientBuilder reactive(ReactiveSqlExecutor e, RdbDialect d) {
        return new FlyingOrmClientBuilder(e, d);
    }

    public FlyingOrmClientBuilder renderer(SqlRenderer v) {
        this.renderer = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder configuredDialect(String v) {
        this.configuredDialect = v;
        this.configuredDialectValue = null;
        return this;
    }

    /** 显式选择完整 SQL 方言，保留 Oracle 版本及自定义方言能力；不探测数据库。 */
    public FlyingOrmClientBuilder dialect(RdbDialect v) {
        this.configuredDialectValue = Objects.requireNonNull(v, "rdb dialect must not be null");
        this.configuredDialect = null;
        return this;
    }

    public FlyingOrmClientBuilder structuredConditionResolver(StructuredConditionResolver v) {
        this.resolver = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder executionOptions(SqlExecutionOptions v) {
        this.executionOptions = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder batchWriteOptions(BatchWriteOptions v) {
        this.batchWriteOptions = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder batchMemoryLimits(BatchMemoryLimits v) {
        this.batchMemoryLimits = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder cachePolicy(OrmCachePolicy v) {
        this.cachePolicy = Objects.requireNonNull(v);
        return this;
    }

    /** 使用自定义 JDBC 元数据读取器；原样复用，其缓存和失效由该实现负责。 */
    public FlyingOrmClientBuilder metadataReader(JdbcFormMetadataReader reader) {
        this.jdbcMetadata = Objects.requireNonNull(reader, "JDBC metadata reader must not be null");
        return this;
    }

    /**
     * 使用自定义响应式元数据读取器，不额外包装缓存。
     * 缓存实现应同时实现 MetadataCacheInvalidator，以参与客户端统一失效。
     */
    public FlyingOrmClientBuilder metadataReader(ReactiveFormMetadataReader reader) {
        this.reactiveMetadata = Objects.requireNonNull(reader, "reactive metadata reader must not be null");
        return this;
    }

    /** 为 ASSIGN_ID 实体主键设置当前客户端共享的线程安全生成器。 */
    public FlyingOrmClientBuilder idGenerator(IdGenerator v) {
        this.idGenerator = Objects.requireNonNull(v);
        return this;
    }

    /** 为 TableField.fill 设置当前客户端共享的线程安全填充器。 */
    public FlyingOrmClientBuilder fieldFiller(EntityFieldFiller v) {
        this.fieldFiller = Objects.requireNonNull(v);
        return this;
    }

    /**
     * 注册一个已经编译完成的实体关系描述，让 Repository、Schema 和值转换共用同一份启动期事实。
     *
     * <p>同一个描述对象可以重复注册；同一实体不能换成另一份描述。一个客户端里的所有描述还必须共用
     * 同一个类型映射注册表，避免写入和读取各自拿到不同的 codec。</p>
     *
     * @param descriptor 实体关系描述
     * @return 当前构建器
     */
    public FlyingOrmClientBuilder entitySchema(EntitySchemaDescriptor<?> descriptor) {
        this.addEntitySchema(descriptor);
        return this;
    }

    /**
     * 配置字段保护使用的版本化主密钥环。
     *
     * <p>构建成功后密钥环由返回的客户端对象图管理，并在客户端关闭时清零；未声明受保护字段时不会改变 SQL。
     * 上层只需提供当前写入密钥和可读旧密钥，不需要把基础设施 SDK 交给 ORM。配置密钥后当前 Builder 只能成功
     * 构建一次，避免两个独立客户端同时声称拥有同一密钥环。</p>
     *
     * @param keys 版本化主密钥环
     * @return 当前构建器
     */
    public FlyingOrmClientBuilder protectedFields(ProtectedFieldKeyRing keys) {
        this.protectedFieldKeys = Objects.requireNonNull(keys);
        return this;
    }

    /**
     * 配置字段保护使用的值规范化器和脱敏策略；普通使用无需调用，默认提供通用内置规则。
     *
     * <p>两个 registry 都必须是上层在启动期创建的不可变、并发安全对象。只扩展其中一类规则时，另一参数传入
     * 对应的 {@code standard()} registry，避免把两项相关配置继续扩张成多个顶层 Builder 方法。</p>
     *
     * @param normalizers 字段保护值规范化器
     * @param policies    结果脱敏策略
     * @return 当前构建器
     */
    public FlyingOrmClientBuilder protectedFieldPolicies(ProtectedValueNormalizerRegistry normalizers,
                                                         MaskingPolicyRegistry policies) {
        this.protectedValueNormalizers = Objects.requireNonNull(
                normalizers, "protected value normalizer registry must not be null");
        this.maskingPolicies = Objects.requireNonNull(policies, "masking policy registry must not be null");
        return this;
    }

    public FlyingOrmClientBuilder migrationExecutionOptions(SqlExecutionOptions v) {
        this.migrationOptions = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder migrationObserver(SchemaMigrationObserver v) {
        this.migrationObserver = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder sqlTemplates(SqlTemplateRegistry v) {
        this.templates = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder sqlTemplateParameterProvider(SqlTemplateParameterProvider v) {
        this.reactiveParameters = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder syncSqlTemplateParameterProvider(SyncSqlTemplateParameterProvider v) {
        this.syncParameters = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder observers(SqlExecutionObserver sql, BatchExecutionObserver batch) {
        this.sqlObserver = Objects.requireNonNull(sql);
        this.batchObserver = Objects.requireNonNull(batch);
        return this;
    }

    public FlyingOrmClientBuilder sqlExecutionLog(SqlExecutionLogOptions o, SqlExecutionLogSink sink) {
        logObserver = SqlExecutionLogObserver.create(o, SqlExecutionLogSelection.defaults(), sink);
        return this;
    }

    public FlyingOrmClientBuilder sqlExecutionLog(SqlExecutionLogOptions o,
                                                  SqlExecutionLogSelection selection,
                                                  SqlExecutionLogSink sink) {
        logObserver = SqlExecutionLogObserver.create(o, selection, sink);
        return this;
    }

    public FlyingOrmClients build() {
        if (protectedFieldKeys != null && protectedFieldKeysTransferred) {
            throw new IllegalStateException("protected field key ring is already owned by a client");
        }
        if (batchWriteOptions != null) {
            batchMemoryLimits.check(batchWriteOptions);
        }
        RdbDialect dialect = selectDialect();
        SqlExecutionObserver selectedSqlObserver = combinedSqlObserver();
        BatchExecutionObserver selectedBatchObserver = combinedBatchObserver();
        ReactiveSqlExecutor reactive = reactiveExecutor(dialect, selectedSqlObserver, selectedBatchObserver);
        SyncSqlExecutor sync = null;
        SyncBatchExecutor batch = null;
        if (jdbcConnectionAccess != null) {
            JdbcSqlExecutor jdbc = JdbcSqlExecutor.create(jdbcConnectionAccess, dialect)
                                                   .withDefaultExecutionOptions(executionOptions);
            if (selectedSqlObserver != null) jdbc = jdbc.withObserver(selectedSqlObserver);
            sync = jdbc;
            JdbcBatchWriter jdbcBatch = JdbcBatchWriter.create(jdbcConnectionAccess, dialect);
            BatchExecutionObserver jdbcObserver = jdbcBatchObserver(
                    selectedBatchObserver, jdbcCleanupObserver(selectedSqlObserver));
            if (jdbcObserver != null) {
                jdbcBatch = jdbcBatch.withBatchObserver(jdbcObserver);
            }
            batch = BatchMemoryLimitedSyncBatchExecutor.create(jdbcBatch, batchMemoryLimits);
        }
        Map<Class<?>, EntitySchemaDescriptor<?>> entitySchemaSnapshot = entitySchemas.isEmpty()
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(entitySchemas));
        SqlRenderer selectedRenderer = renderer;
        if (!entitySchemaSnapshot.isEmpty()) {
            // 启动期合并一次 codec；没有注册描述时仍沿用原 renderer 对象和原装配路径。
            selectedRenderer = renderer.withValueCodecs(
                    sharedEntityTypeMappings.valueCodecs(renderer.valueCodecs()));
        }
        StructuredConditionResolver selectedResolver = resolver == null
                ? StructuredConditionResolver.defaults(selectedRenderer.valueCodecs()) : resolver;
        ProtectedFieldRuntime protectedFields = protectedFieldKeys == null
                ? ProtectedFieldRuntime.withoutKeys(protectedValueNormalizers, maskingPolicies)
                : ProtectedFieldRuntime.create(protectedFieldKeys, protectedValueNormalizers, maskingPolicies);
        FlyingOrmClients clients = FlyingOrmClientAssembler.assemble(new FlyingOrmAssemblyRequest(
                reactive, sync, batch, selectedRenderer, dialect, selectedResolver,
                cachePolicy, executionOptions, idGenerator, fieldFiller,
                templates, reactiveParameters, syncParameters, protectedFields, entitySchemaSnapshot,
                jdbcMetadata, reactiveMetadata, batchWriteOptions, migrationOptions, migrationObserver));
        if (protectedFieldKeys != null) {
            // 构建到客户端对象图后，密钥清零责任已经转移；同一 Builder 不能再把它交给第二个独立对象图。
            protectedFieldKeysTransferred = true;
        }
        return clients;
    }

    /** 只在配置线程注册；对象身份约束在这里一次完成，不进入任何 SQL 执行路径。 */
    private void addEntitySchema(EntitySchemaDescriptor<?> descriptor) {
        EntitySchemaDescriptor<?> safeDescriptor = Objects.requireNonNull(
                descriptor, "entity schema descriptor must not be null");
        Class<?> entityType = safeDescriptor.metadata().type();
        EntitySchemaDescriptor<?> existing = entitySchemas.get(entityType);
        if (existing != null) {
            if (existing != safeDescriptor) {
                throw new IllegalArgumentException("entity schema is already registered for "
                                                           + entityType.getTypeName());
            }
            return;
        }
        EntityTypeMappingRegistry mappings = safeDescriptor.typeMappings();
        if (sharedEntityTypeMappings != null && sharedEntityTypeMappings != mappings) {
            throw new IllegalArgumentException(
                    "all entity schemas in one client must share the same type mapping registry");
        }
        if (entitySchemas.isEmpty()) {
            // 可选能力第一次启用时才分配 Map；普通客户端构建继续保持原来的零配置对象形状。
            entitySchemas = new LinkedHashMap<>();
        }
        sharedEntityTypeMappings = mappings;
        entitySchemas.put(entityType, safeDescriptor);
    }

    private ReactiveSqlExecutor reactiveExecutor(RdbDialect dialect,
                                                 SqlExecutionObserver sql,
                                                 BatchExecutionObserver batch) {
        if (reactiveConnectionAccess == null && customReactiveExecutor == null) return null;
        ReactiveSqlExecutor selected = customReactiveExecutor == null
                ? R2dbcSqlExecutor.create(reactiveConnectionAccess, dialect) : customReactiveExecutor;
        selected = selected.withDefaultExecutionOptions(executionOptions);
        if (sql != null || batch != null) selected = selected.withObservers(
                sql == null ? SqlExecutionObserver.noop() : sql,
                batch == null ? BatchExecutionObserver.noop() : batch);
        return selected.withBatchMemoryLimits(batchMemoryLimits);
    }

    private RdbDialect selectDialect() {
        if (configuredDialectValue != null) return configuredDialectValue;
        if (configuredDialect == null || configuredDialect.isBlank()) {
            throw new IllegalArgumentException("an explicit rdb dialect must be configured");
        }
        return RdbDialectResolver.tryResolveName(configuredDialect)
                .orElseThrow(() -> new IllegalArgumentException("unsupported configured rdb dialect"));
    }

    private SqlExecutionObserver combinedSqlObserver() {
        if (logObserver == null) return sqlObserver;
        return sqlObserver == null ? logObserver : SqlExecutionObservers.composite(sqlObserver, logObserver);
    }

    private BatchExecutionObserver combinedBatchObserver() {
        if (logObserver == null) return batchObserver;
        return batchObserver == null ? logObserver : BatchExecutionObserver.composite(batchObserver, logObserver);
    }

    /**
     * SQL observer 接收清理事实；若调用方把同一对象仅配置为批量 observer，也保留其已有的清理能力。
     * 身份相同的 SQL/batch observer 不重复组合，避免一次 close 故障产生两次回调。
     */
    private SqlExecutionObserver jdbcCleanupObserver(SqlExecutionObserver selectedSqlObserver) {
        SqlExecutionObserver cleanup = selectedSqlObserver;
        if (batchObserver instanceof SqlExecutionObserver batchCleanup
                && batchCleanup != sqlObserver
                && batchCleanup != logObserver) {
            cleanup = cleanup == null
                    ? batchCleanup : SqlExecutionObservers.composite(cleanup, batchCleanup);
        }
        return cleanup == null ? null : SqlExecutionObservers.safe(cleanup);
    }

    private static BatchExecutionObserver jdbcBatchObserver(BatchExecutionObserver batch,
                                                            SqlExecutionObserver sql) {
        if (sql == null) {
            return batch;
        }
        return new JdbcBatchObservationBridge(
                batch == null ? BatchExecutionObserver.noop() : batch, sql);
    }

    /**
     * JDBC 批量复用现有 SQL 清理事件出口，不把清理回调扩展成新的公开 API。
     *
     * @author wangr
     * @date 2026-08-12
     * @version v1.0
     */
    private static final class JdbcBatchObservationBridge
            implements BatchExecutionObserver, SqlExecutionObserver {

        private final BatchExecutionObserver batch;
        private final SqlExecutionObserver sql;

        private JdbcBatchObservationBridge(BatchExecutionObserver batch, SqlExecutionObserver sql) {
            this.batch = Objects.requireNonNull(batch, "batch observer must not be null");
            this.sql = Objects.requireNonNull(sql, "sql observer must not be null");
        }

        @Override
        public boolean enabled() {
            return batch.enabled() || sql.enabled();
        }

        @Override
        public void onExecution(BatchExecutionObservation observation) {
            batch.onExecution(observation);
        }

        @Override
        public void onExecutionEvidence(BatchExecutionEvidence evidence) {
            batch.onExecutionEvidence(evidence);
        }

        @Override
        public void onExecution(SqlExecutionObservation observation) {
            sql.onExecution(observation);
        }

        @Override
        public void onResourceCleanup(ResourceCleanupObservation observation) {
            sql.onResourceCleanup(observation);
        }
    }

}
