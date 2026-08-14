package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataCache;
import com.flying.orm.rdb.observation.BatchExecutionEventType;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.protection.MaskingPolicyRegistry;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedValueNormalizerRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlyingOrmClientsTest {

    @Test
    void createsReactiveClientsWithoutInventingJdbcRuntime() {
        FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory("MySQL"))
                                                   .build();

        assertNotNull(clients.forms());
        assertNotNull(clients.operator());
        assertTrue(clients.reactiveAvailable());
        assertFalse(clients.jdbcAvailable());
        assertThrows(IllegalStateException.class, clients::syncForms);
        assertNotNull(clients.repository(UserRow.class));
        assertThrows(IllegalStateException.class, () -> clients.syncRepository(UserRow.class));
    }

    /** JDBC 和 R2DBC 的 Builder 入口必须是并列能力，不能通过同步桥伪装成另一条内核。 */
    @Test
    void exposesNativeJdbcAndDualKernelBuilderEntrypoints() {
        DataSource jdbc = dataSource();

        assertNotNull(FlyingOrmClients.builder(jdbc));
        assertNotNull(FlyingOrmClients.builder(jdbc, connectionFactory("H2")));
    }

    @Test
    void jdbcOnlyGraphExecutesSqlWithoutCreatingReactiveRuntime() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:clients_jdbc_only;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        FlyingOrmClients clients = FlyingOrmClients.builder(dataSource)
                                                   .configuredDialect("h2")
                                                   .build();

        clients.syncOperator().unsafeNativeSql(
                "create table users(id varchar(32) primary key, name varchar(64))").execute();
        clients.syncOperator().unsafeNativeSql("insert into users(id, name) values(:id, :name)")
               .bind("id", "u-1")
               .bind("name", "Alice")
               .execute();
        DynamicRow row = clients.syncOperator().unsafeNativeSql(
                "select id, name from users where id = :id").bind("id", "u-1").one();

        assertTrue(clients.jdbcAvailable());
        assertFalse(clients.reactiveAvailable());
        assertEquals("Alice", row.get("NAME"));
        assertThrows(IllegalStateException.class, clients::forms);
    }

    @Test
    void jdbcBuilderPassesTheConfiguredBatchObserverToTheNativeWriter() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:clients_jdbc_batch_observer;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        List<BatchExecutionObservation> events = new ArrayList<>();
        FlyingOrmClients clients = FlyingOrmClients.builder(dataSource)
                                                   .configuredDialect("h2")
                                                   .observers(ignored -> { }, events::add)
                                                   .build();
        clients.syncOperator().unsafeNativeSql(
                "create table Users(id bigint primary key)").execute();

        BatchWriteResult result = clients.syncForms().writeBatch(
                BatchSpec.insert(form(), Flux.just(Map.of("id", 1L)))
                         .withOptions(BatchWriteOptions.atomic(1)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(List.of(BatchExecutionEventType.CHUNK, BatchExecutionEventType.SUMMARY),
                     events.stream().map(BatchExecutionObservation::eventType).toList());
        assertTrue(events.stream().allMatch(event -> event.backend() == SqlExecutionBackend.JDBC));
    }

    /** JDBC 批量连接在结果确认后归还失败时，builder 配置的 SQL observer 仍须收到一次清理事实。 */
    @Test
    void jdbcBuilderRoutesConfirmedBatchCleanupToSqlObserver() throws Exception {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:clients_jdbc_batch_cleanup;DB_CLOSE_DELAY=-1");
        source.setUser("sa");
        try (java.sql.Connection connection = source.getConnection();
             java.sql.Statement statement = connection.createStatement()) {
            statement.execute("create table Users(id bigint primary key)");
        }
        AtomicBoolean failClose = new AtomicBoolean();
        AtomicInteger abortCalls = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupEvents = new ArrayList<>();
        SqlExecutionObserver sqlObserver = new SqlExecutionObserver() {
            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // 本测试只关心批量连接清理事件。
            }

            @Override
            public void onResourceCleanup(ResourceCleanupObservation observation) {
                cleanupEvents.add(observation);
            }
        };
        FlyingOrmClients clients = FlyingOrmClients.builder(
                                                           closeFailingDataSource(source, failClose, abortCalls))
                                                   .configuredDialect("h2")
                                                   .observers(sqlObserver, ignored -> { })
                                                   .build();
        failClose.set(true);

        BatchWriteResult result = clients.syncForms().writeBatch(
                BatchSpec.insert(form(), Flux.just(Map.of("id", 1L)))
                         .withOptions(BatchWriteOptions.atomic(1)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1, abortCalls.get());
        assertEquals(1, cleanupEvents.size());
        ResourceCleanupObservation cleanup = cleanupEvents.getFirst();
        assertEquals(SqlExecutionOperation.CHUNKED_BATCH_WRITE, cleanup.operation());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_CLOSE, cleanup.phase());
        assertTrue(cleanup.outcomeConfirmed());
    }

    /** JDBC builder 不能因批量 observer 的双接口身份在组合后丢失其资源清理回调。 */
    @Test
    void jdbcBuilderRetainsCleanupCapabilityOfDualBatchObserver() throws Exception {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:clients_jdbc_dual_batch_cleanup;DB_CLOSE_DELAY=-1");
        source.setUser("sa");
        try (java.sql.Connection connection = source.getConnection();
             java.sql.Statement statement = connection.createStatement()) {
            statement.execute("create table Users(id bigint primary key)");
        }
        AtomicBoolean failClose = new AtomicBoolean();
        AtomicInteger abortCalls = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupEvents = new ArrayList<>();
        class DualObserver implements BatchExecutionObserver, SqlExecutionObserver {
            @Override
            public void onExecution(BatchExecutionObservation observation) {
                // 本契约只验证同一对象承载的清理能力没有在 builder 组合时丢失。
            }

            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // 普通 SQL 由独立 observer 接收，双接口对象在这里仅作为批量 observer 配置。
            }

            @Override
            public void onResourceCleanup(ResourceCleanupObservation observation) {
                cleanupEvents.add(observation);
            }
        }
        DualObserver dualObserver = new DualObserver();
        FlyingOrmClients clients = FlyingOrmClients.builder(
                                                            closeFailingDataSource(source, failClose, abortCalls))
                                                   .configuredDialect("h2")
                                                   .observers(ignored -> { }, dualObserver)
                                                   .build();
        failClose.set(true);

        BatchWriteResult result = clients.syncForms().writeBatch(
                BatchSpec.insert(form(), Flux.just(Map.of("id", 1L)))
                         .withOptions(BatchWriteOptions.atomic(1)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1, abortCalls.get());
        assertEquals(1, cleanupEvents.size());
        assertTrue(cleanupEvents.getFirst().outcomeConfirmed());
    }

    /** 超限请求必须在订阅输入、获取 JDBC 连接之前由同步执行入口拒绝。 */
    @Test
    void syncBatchHardLimitsRejectBothEntrypointsBeforeSourceSubscriptionOrConnectionAcquire() {
        AtomicBoolean subscribed = new AtomicBoolean();
        AtomicInteger connectionAcquires = new AtomicInteger();
        DataSource dataSource = countingDataSource(connectionAcquires);
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users(id) values(?)", 1, List.of(Long.class),
                com.flying.orm.core.sql.render.SqlBindMarkerStyle.CANONICAL,
                Flux.defer(() -> {
                    subscribed.set(true);
                    return Flux.<Object[]>just(new Object[]{1L});
                }),
                BatchWriteOptions.atomic(2));
        var executor = BatchMemoryLimitedSyncBatchExecutor.create(
                JdbcBatchWriter.create(dataSource),
                new BatchMemoryLimits(1, BatchMemoryLimits.DEFAULT_MAX_CONCURRENCY,
                                      BatchMemoryLimits.DEFAULT_MAX_ROWS,
                                      BatchMemoryLimits.DEFAULT_MAX_BUFFERED_BYTES,
                                      BatchMemoryLimits.DEFAULT_MAX_RESULT_CHUNKS));

        assertThrows(BatchMemoryLimitExceededException.class, () -> executor.writeBatch(request));
        assertFalse(subscribed.get());
        assertEquals(0, connectionAcquires.get());

        assertThrows(BatchMemoryLimitExceededException.class, () -> executor.writeBatchChunks(request));
        assertFalse(subscribed.get());
        assertEquals(0, connectionAcquires.get());
    }

    /** 同步批量硬上限装饰器不能剥离业务表与 CONTAINS 侧索引的原子写能力。 */
    @Test
    void forwardsProtectedBatchEntrypointsThroughSyncMemoryLimits() {
        BatchWriteRequest atomic = new BatchWriteRequest(
                "insert into Users(id) values(?)", 1, List.of(Long.class),
                com.flying.orm.core.sql.render.SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1L}), BatchWriteOptions.atomic(1));
        BatchWriteRequest independent = new BatchWriteRequest(
                "insert into Users(id) values(?)", 1, List.of(Long.class),
                com.flying.orm.core.sql.render.SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1L}), BatchWriteOptions.independent(1));
        com.flying.orm.rdb.batch.BatchChunkResult committed =
                com.flying.orm.rdb.batch.BatchChunkResult.committed(0, 0, 1, 1);
        BatchWriteResult expected = BatchWriteResult.from(atomic.options().mode(), List.of(committed));
        List<BatchWriteRequest> observed = new ArrayList<>();
        SyncBatchExecutor delegate = new SyncBatchExecutor() {
            @Override
            public BatchWriteResult writeBatch(BatchWriteRequest request) {
                return expected;
            }

            @Override
            public BatchWriteResult writeProtectedBatch(BatchWriteRequest request) {
                observed.add(request);
                return expected;
            }

            @Override
            public List<com.flying.orm.rdb.batch.BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
                return List.of(committed);
            }

            @Override
            public List<com.flying.orm.rdb.batch.BatchChunkResult> writeProtectedBatchChunks(
                    BatchWriteRequest request) {
                observed.add(request);
                return List.of(committed);
            }
        };
        SyncBatchExecutor limited = BatchMemoryLimitedSyncBatchExecutor.create(
                delegate, BatchMemoryLimits.defaults());

        assertEquals(expected, limited.writeProtectedBatch(atomic));
        assertEquals(List.of(committed), limited.writeProtectedBatchChunks(independent));
        assertEquals(List.of(atomic, independent), observed);
    }

    /** Builder 的默认批量选项也必须在任何运行时探测和资源获取之前受硬限制校验。 */
    @Test
    void builderRejectsDefaultBatchOptionsOutsideHardLimitsBeforeDialectResolution() {
        AtomicInteger connectionAcquires = new AtomicInteger();

        assertThrows(BatchMemoryLimitExceededException.class,
                     () -> FlyingOrmClients.builder(countingDataSource(connectionAcquires))
                                               .configuredDialect("h2")
                                               .batchMemoryLimits(new BatchMemoryLimits(
                                                       1, BatchMemoryLimits.DEFAULT_MAX_CONCURRENCY,
                                                       BatchMemoryLimits.DEFAULT_MAX_ROWS,
                                                       BatchMemoryLimits.DEFAULT_MAX_BUFFERED_BYTES,
                                                       BatchMemoryLimits.DEFAULT_MAX_RESULT_CHUNKS))
                                               .batchWriteOptions(BatchWriteOptions.atomic(2))
                                               .build());

        assertEquals(0, connectionAcquires.get());
    }

    /** 合法同步配置应继续透传至原生 JDBC 批量写入器。 */
    @Test
    void jdbcBuilderKeepsLegalBatchOptionsExecutableWithHardLimits() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:clients_jdbc_batch_hard_limits;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        FlyingOrmClients clients = FlyingOrmClients.builder(dataSource)
                                                   .configuredDialect("h2")
                                                   .batchMemoryLimits(new BatchMemoryLimits(
                                                           2, BatchMemoryLimits.DEFAULT_MAX_CONCURRENCY,
                                                           BatchMemoryLimits.DEFAULT_MAX_ROWS,
                                                           BatchMemoryLimits.DEFAULT_MAX_BUFFERED_BYTES,
                                                           BatchMemoryLimits.DEFAULT_MAX_RESULT_CHUNKS))
                                                   .batchWriteOptions(BatchWriteOptions.atomic(1))
                                                   .build();
        clients.syncOperator().unsafeNativeSql("create table Users(id bigint primary key)").execute();

        BatchWriteResult result = clients.syncForms().writeBatch(
                BatchSpec.insert(form(), Flux.just(Map.of("id", 1L))));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1L, result.affectedRows());
    }

    @Test
    void exposesJdbcSpecificCapabilitiesOnlyThroughTheExplicitAdvancedEntry() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:clients_jdbc_advanced;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        FlyingOrmClients clients = FlyingOrmClients.builder(dataSource).configuredDialect("h2").build();

        String productName = clients.jdbcAdvanced().metadata(metadata -> metadata.getDatabaseProductName());

        assertTrue(productName.toLowerCase().contains("h2"));
        assertThrows(IllegalStateException.class,
                     () -> FlyingOrmClients.builder(connectionFactory("H2")).build().jdbcAdvanced());
    }

    @Test
    void createsOneClientGraphFromRoutingFactoryAndConsistentPhysicalDataSources() {
        FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory("routing-proxy"))
                                                   .validateDataSourceDialects(Map.of(
                                                           "primary", connectionFactory("PostgreSQL"),
                                                           "replica", connectionFactory("Postgres")))
                                                   .build();

        assertNotNull(clients.executor());
        assertNotNull(clients.forms());
        assertNotNull(clients.operator());
        assertNotNull(clients.schema());
        assertNotNull(clients.metadata());
    }

    @Test
    void createsClientsWithDefaultExecutionOptions() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(8).withTimeout(Duration.ofSeconds(2));
        FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                                                   .executionOptions(options)
                                                   .build();

        StepVerifier.create(clients.forms().select(QuerySpec.of(form(), ConditionGroup.and().build())))
                    .verifyComplete();
        assertEquals(options, executor.options());

        StepVerifier.create(clients.operator()
                                   .dml()
                                   .query()
                                   .select("id")
                                   .from("Users")
                                   .fetchMap())
                    .verifyComplete();
        assertEquals(options, executor.options());

        DynamicForm versioned = DynamicForm.builder("users", "Users")
                                           .addField(DynamicField.primaryKey("id", "BIGINT"))
                                           .addField(DynamicField.of("name", "VARCHAR"))
                                           .addField(DynamicField.of("version", "INTEGER"))
                                           .build();
        StepVerifier.create(clients.forms().update(
                            WriteSpec.update(versioned,
                                             Map.of("name", "Alice"),
                                             ConditionGroup.and().where("id", "=", 1L).build())
                                     .withLock(OptimisticLockOptions.increment("version", 3))))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals(options, executor.options());
    }

    /**
     * 验证统一 Builder 能把执行保护、同步等待和元数据缓存一次装好，避免调用方在大量重载中选错入口。
     */
    @Test
    void buildsClientsWithExecutionProtectionAndMetadataCache() {
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(1).withTimeout(Duration.ofSeconds(3));

        FlyingOrmClients clients = FlyingOrmClients.builder(
                                                           ConnectionFactories.get(
                                                                   "r2dbc:h2:mem:///builder-test;DB_CLOSE_DELAY=-1"))
                                                   .executionOptions(options)
                                                   .migrationExecutionOptions(
                                                           SchemaMigrationExecutionOptions.defaults()
                                                                                          .withTimeout(
                                                                                                  Duration.ofSeconds(4)))
                                                   .migrationObserver(ignored -> {
                                                   })
                                                   .batchWriteOptions(BatchWriteOptions.independent(7, 2))
                                                   .build();

        assertInstanceOf(ReactiveFormMetadataCache.class, clients.metadata());

        StepVerifier.create(clients.executor().query(
                            new SqlRequest("SELECT 1 AS id UNION ALL SELECT 2 AS id", List.of())))
                    .expectNextCount(1)
                    .expectError(SqlRowLimitExceededException.class)
                    .verify();

        StepVerifier.create(clients.forms().writeBatch(
                            BatchSpec.insert(form(), Flux.<Map<String, Object>>empty())))
                    .assertNext(result -> assertEquals(BatchWriteOptions.Mode.INDEPENDENT, result.mode()))
                    .verifyComplete();
    }

    @Test
    void builderKeepsRendererValueCodecsByDefault() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == ExternalId.class;
            }

            @Override
            public Object write(Object value) {
                return "EXT-" + ((ExternalId) value).value();
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return new ExternalId(value.toString());
            }
        });
        FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                                                   .renderer(SqlRenderer.builder()
                                                                        .valueCodecs(codecs)
                                                                        .addDefaultTerms()
                                                                        .build())
                                                   .build();

        StepVerifier.create(clients.forms().select(QuerySpec.of(
                            form(), ConditionGroup.and().where("id", "=", new ExternalId("42")).build())))
                    .verifyComplete();

        assertEquals(List.of("EXT-42"), executor.request().parameters());
    }

    @Test
    void nativeEntityQueriesReuseTheClientEntityModelRegistry() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor(List.of(
                DynamicRow.copyOf(Map.of("id", 1L, "name", "Alice"))));
        FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                                                   .build();

        StepVerifier.create(clients.operator().unsafeNativeSql("select id, name from users")
                                   .query(UserRow.class))
                    .expectNext(new UserRow(1L, "Alice"))
                    .verifyComplete();
        long requestsAfterFirstQuery = clients.forms().entityModels().stats().requestCount();

        StepVerifier.create(clients.operator().unsafeNativeSql("select id, name from users")
                                   .query(UserRow.class))
                    .expectNext(new UserRow(1L, "Alice"))
                    .verifyComplete();

        assertTrue(clients.forms().entityModels().estimatedMappings() > 0L);
        assertTrue(requestsAfterFirstQuery > 0L);
        assertTrue(clients.forms().entityModels().stats().requestCount() > requestsAfterFirstQuery);
        assertTrue(clients.forms().entityModels().stats().hitCount() > 0L);
    }

    /** 只配置响应式执行器时模板可用，但不能再凭空生成同步桥。 */
    @Test
    void assemblesRegisteredSqlForBothOperatorStyles() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor(List.of(
                DynamicRow.copyOf(Map.of("id", "u-1"))));
        SqlTemplateRegistry templates = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query("tenant-user",
                                            "select id from users where tenant_id = :tenantId and id = :id",
                                            Set.of()),
                          Set.of("tenantId"))
                .build();
        FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                                                   .sqlTemplates(templates)
                                                   .sqlTemplateParameterProvider(
                                                           (templateId, names) -> Mono.just(
                                                                   Map.of("tenantId", "tenant-a")))
                                                   .build();

        StepVerifier.create(clients.operator().sqlTemplate("tenant-user").bind("id", "u-1").query())
                    .expectNext(DynamicRow.copyOf(Map.of("id", "u-1")))
                    .verifyComplete();
        assertEquals(List.of("tenant-a", "u-1"), executor.request().parameters());

        assertThrows(IllegalStateException.class, clients::syncOperator);
    }

    @Test
    void closeClearsTheOwnedCacheGraphAndIsIdempotent() {
        FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(new RecordingSqlExecutor(), RdbDialect.h2())
                                                   .build();
        ReactiveFormClient forms = clients.forms();
        forms.entityModels().metadata(UserRow.class);
        StepVerifier.create(forms.select(QuerySpec.of(form(), ConditionGroup.and().build())))
                    .verifyComplete();
        assertTrue(forms.entityModels().estimatedMappings() > 0L);
        assertTrue(clients.sqlPlanCacheSnapshot().estimatedSize() > 0L);

        clients.close();
        clients.close();

        assertEquals(0L, forms.entityModels().estimatedMappings());
        assertEquals(0L, clients.sqlPlanCacheSnapshot().estimatedSize());
        assertEquals(0L, clients.conditionPlanCacheSnapshot().estimatedSize());
    }

    /** Builder 只接收版本化主密钥环，客户端关闭时同时清零 ORM 持有的密钥副本。 */
    @Test
    void configuresAndClosesProtectedFieldKeysThroughTheUnifiedBuilder() {
        ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32]);
        FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(new RecordingSqlExecutor(), RdbDialect.h2())
                                                   .protectedFields(keys)
                                                   .build();

        assertEquals(Set.of("v1"), keys.readableVersions());
        clients.close();
        clients.close();

        assertThrows(IllegalStateException.class, keys::readableVersions);
    }

    /** Builder 内部应用默认策略时产生的中间视图必须立即释放，最终客户端关闭后不能残留共享资源引用。 */
    @Test
    void closesSharedResourcesAfterBuilderAppliesDerivedDefaults() {
        ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32]);
        FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(new RecordingSqlExecutor(), RdbDialect.h2())
                .protectedFields(keys)
                .batchWriteOptions(BatchWriteOptions.atomic(3))
                .migrationExecutionOptions(SchemaMigrationExecutionOptions.defaults())
                .migrationObserver(ignored -> {
                })
                .build();

        clients.close();

        assertThrows(IllegalStateException.class, keys::readableVersions);
    }

    /** 同一密钥环只能转移给一个客户端对象图，重复构建必须在创建第二个对象图前稳定失败。 */
    @Test
    void rejectsReusingABuilderAfterProtectedKeyOwnershipIsTransferred() {
        ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32]);
        FlyingOrmClientBuilder builder = FlyingOrmClientBuilder.reactive(
                new RecordingSqlExecutor(), RdbDialect.h2()).protectedFields(keys);
        FlyingOrmClients clients = builder.build();

        IllegalStateException failure = assertThrows(IllegalStateException.class, builder::build);

        assertEquals("protected field key ring is already owned by a client", failure.getMessage());
        clients.close();
        assertThrows(IllegalStateException.class, keys::readableVersions);
    }

    /** 派生客户端关闭时只能释放自身引用，不能提前清零仍被原客户端使用的共享密钥。 */
    @Test
    void keepsSharedProtectedKeysUntilEveryDerivedClientIsClosed() {
        ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32]);
        FlyingOrmClients original = FlyingOrmClientBuilder.reactive(new RecordingSqlExecutor(), RdbDialect.h2())
                .protectedFields(keys)
                .build();
        FlyingOrmClients derived = original.withDefaultDataScope(DataScope.none());

        derived.close();

        assertEquals(Set.of("v1"), keys.readableVersions());
        original.close();
        assertThrows(IllegalStateException.class, keys::readableVersions);
    }

    /** 原客户端先关闭时，仍存活的派生视图必须继续持有共享密钥，直至它自己关闭。 */
    @Test
    void keepsDerivedClientUsableAfterOriginalClientCloses() {
        ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32]);
        FlyingOrmClients original = FlyingOrmClientBuilder.reactive(new RecordingSqlExecutor(), RdbDialect.h2())
                .protectedFields(keys)
                .build();
        FlyingOrmClients derived = original.withDefaultDataScope(DataScope.none());

        original.close();

        assertEquals(Set.of("v1"), keys.readableVersions());
        derived.close();
        assertThrows(IllegalStateException.class, keys::readableVersions);
    }

    /** 关闭客户端后不能借助仍存活的派生客户端重新取得共享资源引用。 */
    @Test
    void rejectsDerivationAfterTheSourceClientIsClosed() {
        ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32]);
        FlyingOrmClients original = FlyingOrmClientBuilder.reactive(new RecordingSqlExecutor(), RdbDialect.h2())
                .protectedFields(keys)
                .build();
        FlyingOrmClients survivor = original.withDefaultDataScope(DataScope.none());

        original.close();

        assertThrows(IllegalStateException.class,
                     () -> original.withDefaultDataScope(DataScope.none()));
        assertThrows(IllegalStateException.class, original::forms);
        assertNotNull(survivor.forms());
        assertEquals(Set.of("v1"), keys.readableVersions());
        survivor.close();
        assertThrows(IllegalStateException.class, keys::readableVersions);
    }

    /** 公开的自定义脱敏策略必须能够经统一 Builder 进入真实查询结果链，不要求配置无关的加密密钥。 */
    @Test
    void configuresCustomMaskingPoliciesThroughTheUnifiedBuilder() {
        DynamicForm protectedForm = DynamicForm.builder("profiles", "profiles")
                                               .addField(DynamicField.of("contact", "VARCHAR"))
                                               .masked("contact", MaskedFieldDefinition.builder("constant").build())
                                               .build();
        RecordingSqlExecutor executor = new RecordingSqlExecutor(
                List.of(DynamicRow.copyOf(Map.of("contact", "secret-value"))));
        MaskingPolicyRegistry policies = MaskingPolicyRegistry.standard()
                                                                .with("constant", (value, ignored) -> "#");
        FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                                                   .protectedFieldPolicies(
                                                           ProtectedValueNormalizerRegistry.standard(), policies)
                                                   .build();

        StepVerifier.create(clients.forms().select(QuerySpec.of(
                            protectedForm, ConditionGroup.and().build())))
                    .assertNext(row -> assertEquals("#", row.get("contact")))
                    .verifyComplete();

        clients.close();
    }

    /** 自定义规范化器必须经统一 Builder 进入真实保护查询链，而不是停留在不可装配的扩展接口。 */
    @Test
    void configuresCustomProtectedValueNormalizerThroughTheUnifiedBuilder() {
        DynamicForm protectedForm = DynamicForm.builder("profiles", "profiles")
                                               .addField(DynamicField.of("contact", "VARCHAR"))
                                               .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                                             .normalizer("constant")
                                                                                             .build())
                                               .build();
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32]);
        ProtectedValueNormalizerRegistry normalizers = ProtectedValueNormalizerRegistry.standard()
                .with("constant", ignored -> "normalized");
        FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                                                   .protectedFields(keys)
                                                   .protectedFieldPolicies(
                                                           normalizers, MaskingPolicyRegistry.standard())
                                                   .build();

        StepVerifier.create(clients.forms().select(QuerySpec.of(
                            protectedForm,
                            ConditionGroup.and().add(ProtectedConditions.exact("contact", "first")).build())))
                    .verifyComplete();
        byte[] first = assertInstanceOf(byte[].class, executor.request().parameters().getFirst()).clone();
        StepVerifier.create(clients.forms().select(QuerySpec.of(
                            protectedForm,
                            ConditionGroup.and().add(ProtectedConditions.exact("contact", "second")).build())))
                    .verifyComplete();
        byte[] second = assertInstanceOf(byte[].class, executor.request().parameters().getFirst());

        assertArrayEquals(first, second);
        clients.close();
    }

    private static DynamicForm form() {
        return DynamicForm.builder("users", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .build();
    }

    private static ConnectionFactory connectionFactory(String metadataName) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.error(new UnsupportedOperationException("这个测试只看自动组装，不会真的连库"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> metadataName;
            }
        };
    }

    private static DataSource dataSource() {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                                                   new Class<?>[]{DataSource.class},
                                                   (proxy, method, arguments) -> null);
    }

    private static DataSource countingDataSource(AtomicInteger connectionAcquires) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                                                   new Class<?>[]{DataSource.class},
                                                   (proxy, method, arguments) -> {
                                                       if (method.getName().equals("getConnection")) {
                                                           connectionAcquires.incrementAndGet();
                                                           throw new AssertionError("connection must not be acquired");
                                                       }
                                                       if (method.getName().equals("toString")) {
                                                           return "counting data source";
                                                       }
                                                       throw new UnsupportedOperationException(method.getName());
                                                   });
    }

    private static DataSource closeFailingDataSource(DataSource delegate,
                                                     AtomicBoolean failClose,
                                                     AtomicInteger abortCalls) {
        return (DataSource) Proxy.newProxyInstance(
                FlyingOrmClientsTest.class.getClassLoader(), new Class[]{DataSource.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (method.getName().equals("getConnection")
                                && result instanceof java.sql.Connection connection) {
                            return closeFailingConnection(connection, failClose, abortCalls);
                        }
                        return result;
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static java.sql.Connection closeFailingConnection(java.sql.Connection delegate,
                                                               AtomicBoolean failClose,
                                                               AtomicInteger abortCalls) {
        return (java.sql.Connection) Proxy.newProxyInstance(
                FlyingOrmClientsTest.class.getClassLoader(), new Class[]{java.sql.Connection.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("abort")) {
                        abortCalls.incrementAndGet();
                        if (!delegate.isClosed()) {
                            delegate.close();
                        }
                        return null;
                    }
                    if (method.getName().equals("close") && failClose.get()) {
                        delegate.close();
                        throw new SQLException("simulated connection close failure", "08006");
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static final class RecordingSqlExecutor implements ReactiveSqlExecutor {

        private final List<DynamicRow> rows;

        private SqlExecutionOptions options;

        private SqlRequest request;

        private RecordingSqlExecutor() {
            this(List.of());
        }

        private RecordingSqlExecutor(List<DynamicRow> rows) {
            this.rows = List.copyOf(rows);
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            this.request = request;
            return Flux.fromIterable(rows);
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
            this.options = options;
            return query(request);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.just(1L);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
            this.options = options;
            return rowsUpdated(request);
        }

        @Override
        public Mono<BatchWriteResult> writeBatch(com.flying.orm.rdb.batch.BatchWriteRequest request) {
            return Mono.error(new UnsupportedOperationException("clients test must not use batch writer"));
        }

        private SqlExecutionOptions options() {
            return options;
        }

        private SqlRequest request() {
            return request;
        }

    }

    private record ExternalId(String value) {
    }

    private record UserRow(long id, String name) {
    }
}
