package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.cache.OrmCacheSnapshot;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.jdbc.JdbcAdvancedOperations;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.operator.DatabaseOperator;
import com.flying.orm.rdb.operator.SyncDatabaseOperator;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.EntitySchemaSynchronizer;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationObserver;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;

import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * flying-orm 的统一客户端对象图。
 *
 * <p>R2DBC 和 JDBC 是两个真正独立的执行内核。应用可以只配置其中一个，也可以同时配置；没有配置的内核
 * 不会被另一个内核或线程等待隐式补出来。两者同时存在时共享方言、渲染器、Scope、codec 和结构缓存。</p>
 *
 * <p>这是稳定的使用门面，具体查询、写入、批量和 DDL 责任分别下沉到对应 Operator 及其客户端；这样门面可以保持
 * 入口统一，而不会把执行细节堆在一个大类里。实例和内部客户端都可以并发共享。关闭对象只释放当前客户端对 ORM
 * 有界缓存和字段保护密钥的引用；派生客户端共享这些资源，并在最后一个客户端关闭时统一清理。调用方拥有的数据源、
 * 连接池或执行器始终不会被关闭。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public final class FlyingOrmClients implements AutoCloseable {

    private final DatabaseOperator reactive;
    private final SyncDatabaseOperator jdbc;
    private final StructuralPlanCaches planCaches;
    private final FlyingOrmSharedResources sharedResources;
    private final AtomicBoolean closed = new AtomicBoolean();

    FlyingOrmClients(DatabaseOperator reactive,
                     SyncDatabaseOperator jdbc,
                     StructuralPlanCaches planCaches,
                     FlyingOrmSharedResources sharedResources) {
        if (reactive == null && jdbc == null) {
            throw new IllegalArgumentException("at least one SQL runtime must be configured");
        }
        this.reactive = reactive;
        this.jdbc = jdbc;
        this.planCaches = Objects.requireNonNull(planCaches, "structural plan caches must not be null");
        this.sharedResources = Objects.requireNonNull(
                sharedResources, "shared client resources must not be null");
    }

    public static FlyingOrmClientBuilder builder(R2dbcConnectionAccess reactiveConnectionAccess) {
        return FlyingOrmClientBuilder.reactive(reactiveConnectionAccess);
    }

    public static FlyingOrmClientBuilder builder(JdbcConnectionAccess jdbcConnectionAccess) {
        return FlyingOrmClientBuilder.jdbc(jdbcConnectionAccess);
    }

    public static FlyingOrmClientBuilder builder(JdbcConnectionAccess jdbcConnectionAccess, R2dbcConnectionAccess reactiveConnectionAccess) {
        return FlyingOrmClientBuilder.dual(jdbcConnectionAccess, reactiveConnectionAccess);
    }

    /** 返回继续收紧默认数据范围的新对象图。 */
    public FlyingOrmClients withDefaultDataScope(DataScope scope) {
        requireOpen();
        DataScope safeScope = Objects.requireNonNull(scope, "data scope must not be null");
        return derive(reactive == null ? null : reactive.withDefaultDataScope(safeScope),
                jdbc == null ? null : jdbc.withDefaultDataScope(safeScope));
    }

    /** 返回使用新默认批量策略的对象图。 */
    public FlyingOrmClients withDefaultBatchWriteOptions(BatchWriteOptions options) {
        requireOpen();
        BatchWriteOptions safe = Objects.requireNonNull(options, "batch write options must not be null");
        return derive(reactive == null ? null : reactive.withDefaultBatchWriteOptions(safe),
                jdbc == null ? null : jdbc.withDefaultBatchWriteOptions(safe));
    }

    public FlyingOrmClients withDefaultSchemaMigrationExecutionOptions(SqlExecutionOptions options) {
        requireOpen();
        SqlExecutionOptions safe = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        return derive(reactive == null ? null : reactive.withDefaultSchemaMigrationExecutionOptions(safe),
                jdbc == null ? null : jdbc.withSchemaExecutionOptions(safe));
    }

    public FlyingOrmClients withSchemaMigrationObserver(SchemaMigrationObserver observer) {
        requireOpen();
        SchemaMigrationObserver safe = Objects.requireNonNull(observer, "schema migration observer must not be null");
        return derive(reactive == null ? null : reactive.withSchemaMigrationObserver(safe),
                jdbc == null ? null : jdbc.withSchemaObserver(safe));
    }

    public boolean reactiveAvailable() { return reactive != null; }
    public boolean jdbcAvailable() { return jdbc != null; }
    public ReactiveSqlExecutor executor() { return requireReactive().executor(); }
    public ReactiveFormClient forms() { return requireReactive().forms(); }
    public ReactiveSchemaClient schema() { return requireReactive().schema(); }
    public ReactiveFormMetadataReader metadata() { return requireReactive().metadataReader(); }
    public DatabaseOperator operator() { return requireReactive(); }
    public SyncSqlExecutor syncExecutor() { return requireJdbc().executor(); }
    /**
     * JDBC 的显式高级入口。普通 CRUD 使用 syncForms、syncRepository 或 syncOperator 即可，不必接触它。
     */
    public JdbcAdvancedOperations jdbcAdvanced() {
        SyncSqlExecutor executor = requireJdbc().executor();
        if (executor instanceof JdbcSqlExecutor jdbcExecutor) {
            return jdbcExecutor.advanced();
        }
        throw new IllegalStateException("JDBC advanced operations require the native JdbcSqlExecutor");
    }
    public SyncFormClient syncForms() { return requireJdbc().forms(); }
    public JdbcSchemaClient syncSchema() { return requireJdbc().schema(); }
    public SyncDatabaseOperator syncOperator() { return requireJdbc(); }

    /**
     * 返回框架无关的实体结构同步入口。外部容器只负责提供扫描到的实体类型，真正的差异计算和 DDL 仍走统一 Schema 内核。
     */
    public EntitySchemaSynchronizer entitySchemas() {
        requireOpen();
        return new EntitySchemaSynchronizer(
                reactive != null ? reactive.forms().entityModels() : jdbc.forms().entityModels(),
                reactive == null ? null : reactive.schema(),
                reactive == null ? null : reactive.metadataReader(),
                jdbc == null ? null : jdbc.schema(),
                jdbc == null ? null : jdbc.metadataReader());
    }

    public <T> ReactiveFormRepository<T> repository(Class<T> type) {
        ReactiveFormClient client = forms();
        Class<T> safeType = Objects.requireNonNull(type, "repository entity type must not be null");
        return ReactiveFormRepository.create(client, client.entityModels().metadata(safeType).toDynamicForm(), safeType);
    }

    public <T> SyncFormRepository<T> syncRepository(Class<T> type) {
        SyncFormClient client = syncForms();
        Class<T> safeType = Objects.requireNonNull(type, "sync repository entity type must not be null");
        return SyncFormRepository.create(client, client.entityModels().metadata(safeType).toDynamicForm(), safeType);
    }

    public OrmCacheSnapshot sqlPlanCacheSnapshot() { return planCaches.sqlSnapshot(); }
    public OrmCacheSnapshot conditionPlanCacheSnapshot() { return planCaches.conditionSnapshot(); }

    /**
     * 释放当前客户端持有的共享资源引用；重复关闭无副作用，其他仍存活的派生客户端不受影响。
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            sharedResources.release();
        }
    }

    private FlyingOrmClients derive(DatabaseOperator configuredReactive,
                                    SyncDatabaseOperator configuredJdbc) {
        sharedResources.retain();
        if (closed.get()) {
            sharedResources.release();
            throw new IllegalStateException("flying ORM client is closed");
        }
        try {
            return new FlyingOrmClients(configuredReactive, configuredJdbc, planCaches, sharedResources);
        } catch (RuntimeException | Error error) {
            sharedResources.release();
            throw error;
        }
    }

    private DatabaseOperator requireReactive() {
        requireOpen();
        if (reactive == null) throw new IllegalStateException("R2DBC runtime is not configured");
        return reactive;
    }

    private SyncDatabaseOperator requireJdbc() {
        requireOpen();
        if (jdbc == null) throw new IllegalStateException("JDBC runtime is not configured");
        return jdbc;
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("flying ORM client is closed");
        }
    }

}
