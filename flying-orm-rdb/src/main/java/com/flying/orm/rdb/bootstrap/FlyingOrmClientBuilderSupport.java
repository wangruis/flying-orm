package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.JdbcDialectResolver;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.RdbDialectResolver;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.StructuredConditionResolvers;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
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
import com.flying.orm.rdb.observation.SqlTransactionSource;
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
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Objects;

/** 保存启动期可变配置并在 build 时一次校验；该对象不会进入 SQL 热路径。 */
final class FlyingOrmClientBuilderSupport {

    final ConnectionFactory connectionFactory;
    final DataSource dataSource;
    final ReactiveSqlExecutor customReactiveExecutor;
    final RdbDialect fixedDialect;
    SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
    String configuredDialect;
    Map<String, ? extends ConnectionFactory> reactiveDataSources = Map.of();
    Map<String, ? extends DataSource> jdbcDataSources = Map.of();
    StructuredConditionResolver resolver;
    SqlExecutionOptions executionOptions = SqlExecutionOptions.safeDefaults();
    BatchWriteOptions batchWriteOptions;
    BatchMemoryLimits batchMemoryLimits = BatchMemoryLimits.defaults();
    OrmCachePolicy cachePolicy = OrmCachePolicy.safeDefaults();
    IdGenerator idGenerator = IdGenerator.none();
    EntityFieldFiller fieldFiller = EntityFieldFiller.none();
    ProtectedFieldKeyRing protectedFieldKeys;
    ProtectedValueNormalizerRegistry protectedValueNormalizers = ProtectedValueNormalizerRegistry.standard();
    MaskingPolicyRegistry maskingPolicies = MaskingPolicyRegistry.standard();
    SchemaMigrationExecutionOptions migrationOptions;
    SchemaMigrationObserver migrationObserver;
    SqlTemplateRegistry templates = SqlTemplateRegistry.builder().build();
    SqlTemplateParameterProvider reactiveParameters = SqlTemplateParameterProvider.none();
    SyncSqlTemplateParameterProvider syncParameters = SyncSqlTemplateParameterProvider.none();
    R2dbcTransactionParticipant reactiveTransaction;
    JdbcTransactionParticipant jdbcTransaction = JdbcTransactionParticipant.none();
    SqlExecutionObserver sqlObserver;
    BatchExecutionObserver batchObserver;
    SqlExecutionLogObserver logObserver;
    boolean protectedFieldKeysTransferred;

    FlyingOrmClientBuilderSupport(DataSource dataSource, ConnectionFactory connectionFactory) {
        this.dataSource = dataSource;
        this.connectionFactory = connectionFactory;
        this.customReactiveExecutor = null;
        this.fixedDialect = null;
    }

    FlyingOrmClientBuilderSupport(ReactiveSqlExecutor executor, RdbDialect dialect) {
        this.dataSource = null;
        this.connectionFactory = null;
        this.customReactiveExecutor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.fixedDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
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
        ReactiveSqlExecutor reactive = reactiveExecutor(selectedSqlObserver, selectedBatchObserver);
        SyncSqlExecutor sync = null;
        SyncBatchExecutor batch = null;
        if (dataSource != null) {
            JdbcSqlExecutor jdbc = JdbcSqlExecutor.create(dataSource)
                                                   .withDefaultExecutionOptions(executionOptions)
                                                   .withTransactionParticipant(jdbcTransaction);
            if (selectedSqlObserver != null) jdbc = jdbc.withObserver(selectedSqlObserver);
            sync = jdbc;
            JdbcBatchWriter jdbcBatch = JdbcBatchWriter.create(dataSource)
                                                         .withTransactionParticipant(jdbcTransaction);
            BatchExecutionObserver jdbcObserver = jdbcBatchObserver(
                    selectedBatchObserver, jdbcCleanupObserver(selectedSqlObserver));
            if (jdbcObserver != null) {
                jdbcBatch = jdbcBatch.withBatchObserver(jdbcObserver);
            }
            batch = BatchMemoryLimitedSyncBatchExecutor.create(jdbcBatch, batchMemoryLimits);
        }
        StructuredConditionResolver selectedResolver = resolver == null
                ? StructuredConditionResolvers.defaults(renderer.valueCodecs()) : resolver;
        ProtectedFieldRuntime protectedFields = protectedFieldKeys == null
                ? ProtectedFieldRuntime.withoutKeys(protectedValueNormalizers, maskingPolicies)
                : ProtectedFieldRuntime.create(protectedFieldKeys, protectedValueNormalizers, maskingPolicies);
        FlyingOrmClients clients = FlyingOrmClientAssembler.assemble(new FlyingOrmAssemblyRequest(
                reactive, sync, batch, jdbcTransaction, renderer, dialect, selectedResolver,
                cachePolicy, executionOptions, idGenerator, fieldFiller,
                templates, reactiveParameters, syncParameters, protectedFields));
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

    private ReactiveSqlExecutor reactiveExecutor(SqlExecutionObserver sql, BatchExecutionObserver batch) {
        if (connectionFactory == null && customReactiveExecutor == null) return null;
        ReactiveSqlExecutor selected = customReactiveExecutor == null
                ? R2dbcSqlExecutor.create(connectionFactory) : customReactiveExecutor;
        if (reactiveTransaction != null) {
            if (!(selected instanceof R2dbcSqlExecutor r2dbc)) {
                throw new IllegalStateException("transaction participant requires the standard R2dbcSqlExecutor");
            }
            selected = r2dbc.withTransactionParticipant(reactiveTransaction);
        }
        selected = selected.withDefaultExecutionOptions(executionOptions);
        if (sql != null || batch != null) selected = selected.withObservers(
                sql == null ? SqlExecutionObserver.noop() : sql,
                batch == null ? BatchExecutionObserver.noop() : batch);
        return selected.withBatchMemoryLimits(batchMemoryLimits);
    }

    private RdbDialect selectDialect() {
        if (fixedDialect != null) return fixedDialect;
        RdbDialect reactive = connectionFactory == null ? null : RdbDialectResolver.resolveAndValidate(
                configuredDialect, connectionFactory, reactiveDataSources);
        RdbDialect jdbc = dataSource == null ? null : JdbcDialectResolver.resolveAndValidate(
                configuredDialect, dataSource, jdbcDataSources);
        RdbDialect selected = reactive == null ? jdbc : reactive;
        if (selected == null) throw new IllegalStateException("no database runtime is configured");
        if (jdbc != null && !selected.name().equals(jdbc.name())) {
            throw new IllegalArgumentException("JDBC and R2DBC dialects must match: "
                    + jdbc.name() + " != " + selected.name());
        }
        return selected;
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
        public void onExecution(BatchExecutionObservation observation) {
            batch.onExecution(observation);
        }

        @Override
        public void onExecution(BatchExecutionObservation observation, SqlTransactionSource transactionSource) {
            batch.onExecution(observation, transactionSource);
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
