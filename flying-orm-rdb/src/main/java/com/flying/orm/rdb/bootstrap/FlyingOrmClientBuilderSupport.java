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
import com.flying.orm.rdb.form.StructuredConditionResolvers;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
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
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationObserver;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.template.SqlTemplateParameterProvider;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import com.flying.orm.rdb.template.SyncSqlTemplateParameterProvider;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;

import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** 保存启动期可变配置并在 build 时一次校验；该对象不会进入 SQL 热路径。 */
final class FlyingOrmClientBuilderSupport {

    final R2dbcConnectionAccess reactiveConnectionAccess;
    final JdbcConnectionAccess jdbcConnectionAccess;
    final ReactiveSqlExecutor customReactiveExecutor;
    RdbDialect configuredDialectValue;
    SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
    String configuredDialect;
    StructuredConditionResolver resolver;
    SqlExecutionOptions executionOptions = SqlExecutionOptions.safeDefaults();
    BatchWriteOptions batchWriteOptions;
    BatchMemoryLimits batchMemoryLimits = BatchMemoryLimits.defaults();
    OrmCachePolicy cachePolicy = OrmCachePolicy.safeDefaults();
    IdGenerator idGenerator = IdGenerator.none();
    EntityFieldFiller fieldFiller = EntityFieldFiller.none();
    Map<Class<?>, EntitySchemaDescriptor<?>> entitySchemas = Map.of();
    EntityTypeMappingRegistry sharedEntityTypeMappings;
    ProtectedFieldKeyRing protectedFieldKeys;
    ProtectedValueNormalizerRegistry protectedValueNormalizers = ProtectedValueNormalizerRegistry.standard();
    MaskingPolicyRegistry maskingPolicies = MaskingPolicyRegistry.standard();
    SchemaMigrationExecutionOptions migrationOptions;
    SchemaMigrationObserver migrationObserver;
    SqlTemplateRegistry templates = SqlTemplateRegistry.builder().build();
    SqlTemplateParameterProvider reactiveParameters = SqlTemplateParameterProvider.none();
    SyncSqlTemplateParameterProvider syncParameters = SyncSqlTemplateParameterProvider.none();
    SqlExecutionObserver sqlObserver;
    BatchExecutionObserver batchObserver;
    SqlExecutionLogObserver logObserver;
    boolean protectedFieldKeysTransferred;

    FlyingOrmClientBuilderSupport(JdbcConnectionAccess jdbcConnectionAccess, R2dbcConnectionAccess reactiveConnectionAccess) {
        this.jdbcConnectionAccess = jdbcConnectionAccess;
        this.reactiveConnectionAccess = reactiveConnectionAccess;
        this.customReactiveExecutor = null;
        this.configuredDialectValue = null;
    }

    FlyingOrmClientBuilderSupport(ReactiveSqlExecutor executor, RdbDialect dialect) {
        this.jdbcConnectionAccess = null;
        this.reactiveConnectionAccess = null;
        this.customReactiveExecutor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.configuredDialectValue = Objects.requireNonNull(dialect, "rdb dialect must not be null");
    }

    FlyingOrmClients build() {
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
                ? StructuredConditionResolvers.defaults(selectedRenderer.valueCodecs()) : resolver;
        ProtectedFieldRuntime protectedFields = protectedFieldKeys == null
                ? ProtectedFieldRuntime.withoutKeys(protectedValueNormalizers, maskingPolicies)
                : ProtectedFieldRuntime.create(protectedFieldKeys, protectedValueNormalizers, maskingPolicies);
        FlyingOrmClients clients = FlyingOrmClientAssembler.assemble(new FlyingOrmAssemblyRequest(
                reactive, sync, batch, selectedRenderer, dialect, selectedResolver,
                cachePolicy, executionOptions, idGenerator, fieldFiller,
                templates, reactiveParameters, syncParameters, protectedFields, entitySchemaSnapshot));
        if (protectedFieldKeys != null) {
            // 构建到客户端对象图后，密钥清零责任已经转移；同一 Builder 不能再把它交给第二个独立对象图。
            protectedFieldKeysTransferred = true;
        }
        try {
            if (migrationOptions != null) {
                clients = replace(clients, clients.withDefaultSchemaMigrationExecutionOptions(migrationOptions));
            }
            if (migrationObserver != null) {
                clients = replace(clients, clients.withSchemaMigrationObserver(migrationObserver));
            }
            if (batchWriteOptions != null) {
                clients = replace(clients, clients.withDefaultBatchWriteOptions(batchWriteOptions));
            }
            return clients;
        } catch (RuntimeException | Error error) {
            clients.close();
            throw error;
        }
    }

    /** 派生对象已经持有共享引用后，立即释放只供 Builder 过渡使用的旧视图。 */
    private static FlyingOrmClients replace(FlyingOrmClients current, FlyingOrmClients replacement) {
        current.close();
        return replacement;
    }

    /** 只在配置线程注册；对象身份约束在这里一次完成，不进入任何 SQL 执行路径。 */
    void addEntitySchema(EntitySchemaDescriptor<?> descriptor) {
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

    void configureLog(SqlExecutionLogOptions options, SqlExecutionLogSelection selection, SqlExecutionLogSink sink) {
        logObserver = SqlExecutionLogObserver.create(options, selection, sink);
    }
}
