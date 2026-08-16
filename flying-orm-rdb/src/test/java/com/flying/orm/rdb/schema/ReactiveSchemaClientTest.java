package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.reactive.ConnectionScopedReactiveSqlExecutor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import io.r2dbc.spi.Connection;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证动态表结构客户端通过响应式 SQL 执行器维护表结构。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
class ReactiveSchemaClientTest {

    @Test
    void plannerRecognizesTheFixedMissingMetadataSignal() {
        DynamicForm form = DynamicForm.builder("users", "users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();
        ReactiveFormMetadataReader missing = new ReactiveFormMetadataReader() {
            @Override
            public Mono<DynamicForm> readForm(String formId, String table) {
                return Mono.error(new IllegalArgumentException("table metadata not found"));
            }

            @Override
            public Mono<DynamicForm> readForm(String formId, String schema, String table) {
                return Mono.error(new IllegalArgumentException("table metadata not found"));
            }
        };
        SchemaMigrationPlanner planner = new SchemaMigrationPlanner(FormSchemaSqlRenderer.create(RdbDialect.h2()));

        StepVerifier.create(planner.plan(form, List.of(), List.of(), missing, SchemaMigrationOptions.safe()))
                    .assertNext(plan -> assertTrue(!plan.tableExists()))
                    .verifyComplete();
    }

    @Test
    void plannerPreservesLegacyMissingMetadataSignalFromCustomReader() {
        DynamicForm form = DynamicForm.builder("users", "users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();
        ReactiveFormMetadataReader missing = new ReactiveFormMetadataReader() {
            @Override
            public Mono<DynamicForm> readForm(String formId, String table) {
                return Mono.error(new IllegalArgumentException("table metadata not found: users"));
            }

            @Override
            public Mono<DynamicForm> readForm(String formId, String schema, String table) {
                return Mono.error(new IllegalArgumentException("table metadata not found: users"));
            }
        };
        SchemaMigrationPlanner planner = new SchemaMigrationPlanner(FormSchemaSqlRenderer.create(RdbDialect.h2()));

        StepVerifier.create(planner.plan(form, List.of(), List.of(), missing, SchemaMigrationOptions.safe()))
                    .assertNext(plan -> assertTrue(!plan.tableExists()))
                    .verifyComplete();
    }

    /** 已有业务表首次启用 CONTAINS 时创建辅助表，后续重复规划不得再次 CREATE。 */
    @Test
    void plansContainsSideTableIdempotentlyForExistingBusinessTable() {
        DynamicForm form = containsProtectedForm();
        TableMetadata primary = ProtectedFormLayout.physical(form).toTableMetadata();
        ProtectedContainsLayout layout = ProtectedContainsLayout.resolve(form).orElseThrow();
        SchemaMigrationPlanner planner = new SchemaMigrationPlanner(
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()));

        StepVerifier.create(planner.plan(form, List.of(), List.of(),
                                         metadata(primary, null), SchemaMigrationOptions.safe()))
                    .assertNext(plan -> assertTrue(plan.sqlTexts().stream()
                                                       .anyMatch(sql -> sql.startsWith("create table ")
                                                               && sql.contains(layout.table().table()))))
                    .verifyComplete();

        StepVerifier.create(planner.plan(form, List.of(), List.of(),
                                         metadata(primary, layout.table().toTableMetadata()),
                                         SchemaMigrationOptions.safe()))
                    .assertNext(plan -> assertFalse(plan.sqlTexts().stream()
                                                        .anyMatch(sql -> sql.startsWith("create table ")
                                                                && sql.contains(layout.table().table()))))
                    .verifyComplete();
    }

    /** 审核计划必须为首次创建的辅助表生成自己的 DROP TABLE 回滚语句。 */
    @Test
    void reviewsContainsSideTableWithItsOwnRollbackSegment() {
        DynamicForm form = containsProtectedForm();
        TableMetadata primary = ProtectedFormLayout.physical(form).toTableMetadata();
        ProtectedContainsLayout layout = ProtectedContainsLayout.resolve(form).orElseThrow();
        SchemaMigrationPlanner planner = new SchemaMigrationPlanner(
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()));

        StepVerifier.create(planner.review(form, List.of(), List.of(), metadata(primary, null),
                                           SchemaMigrationOptions.safe(),
                                           SchemaMigrationReviewPolicy.allowBlocking()))
                    .assertNext(reviewed -> assertTrue(reviewed.rollback().requests().stream()
                                                              .map(SqlRequest::sql)
                                                              .anyMatch(sql -> sql.startsWith("drop table ")
                                                                      && sql.contains(layout.table().table()))))
                    .verifyComplete();
    }

    /** 主表与自动 CONTAINS 侧表在同一计划中执行 DDL 后，两个精确 metadata 键都必须失效。 */
    @Test
    void invalidatesPrimaryAndContainsSideMetadataAfterReactiveMigration() {
        DynamicForm form = containsProtectedForm();
        TableMetadata primary = ProtectedFormLayout.physical(form).toTableMetadata();
        String sideTable = ProtectedContainsLayout.resolve(form).orElseThrow().table().table();
        List<String> invalidated = new ArrayList<>();
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.postgresql());

        StepVerifier.create(client.createOrAlterDetailed(
                            form, List.of(), metadata(primary, null, invalidated)))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals(List.of(form.table(), sideTable), invalidated);
    }

    /** 已有 CONTAINS 侧表发生审核迁移后也必须失效，不能只覆盖首次建表路径。 */
    @Test
    void invalidatesExistingContainsSideMetadataAfterReviewedMigration() {
        DynamicForm form = containsProtectedForm();
        TableMetadata primary = ProtectedFormLayout.physical(form).toTableMetadata();
        ProtectedContainsLayout layout = ProtectedContainsLayout.resolve(form).orElseThrow();
        List<String> invalidated = new ArrayList<>();
        ReactiveFormMetadataReader reader = metadata(primary, layout.table().toTableMetadata(), invalidated);
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.postgresql());

        StepVerifier.create(client.reviewCreateOrAlter(
                                   form,
                                   List.of(),
                                   List.of(),
                                   reader,
                                   SchemaMigrationOptions.safe(),
                                   SchemaMigrationReviewPolicy.allowBlocking())
                           .doOnNext(reviewed -> {
                               assertTrue(reviewed.migration().sqlTexts().stream()
                                                  .anyMatch(sql -> sql.contains(layout.table().table())));
                               assertFalse(reviewed.rollback().requests().stream()
                                                   .map(SqlRequest::sql)
                                                   .anyMatch(sql -> sql.startsWith("drop table ")
                                                           && sql.contains(layout.table().table())));
                           })
                           .flatMap(reviewed -> client.executeReviewed(
                                   reviewed,
                                   reader,
                                   SchemaMigrationExecutionOptions.defaults().withApproval(
                                           SchemaMigrationApproval.approve(reviewed, "reviewed side-table test")))))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals(List.of(form.table(), layout.table().table()), invalidated);
    }

    private static DynamicForm containsProtectedForm() {
        return DynamicForm.builder("customer", "customer")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                        .searchModes(EncryptedSearchMode.CONTAINS)
                                                                        .build())
                          .build();
    }

    private static ReactiveFormMetadataReader metadata(TableMetadata primary, TableMetadata side) {
        return metadata(primary, side, new ArrayList<>());
    }

    private static ReactiveFormMetadataReader metadata(TableMetadata primary,
                                                       TableMetadata side,
                                                       List<String> invalidated) {
        return new ReactiveFormMetadataReader() {
            @Override
            public Mono<DynamicForm> readForm(String formId, String table) {
                return Mono.error(new UnsupportedOperationException("test reads table metadata directly"));
            }

            @Override
            public Mono<TableMetadata> readTable(String table) {
                if (table.equals(primary.name())) {
                    return Mono.just(primary);
                }
                if (side != null && table.equals(side.name())) {
                    return Mono.just(side);
                }
                return Mono.error(new IllegalArgumentException("table metadata not found"));
            }

            @Override
            public Mono<DynamicForm> readForm(String formId, String schema, String table) {
                return Mono.error(new UnsupportedOperationException("test reads table metadata directly"));
            }

            @Override
            public void invalidate(String table) {
                invalidated.add(table);
            }
        };
    }

    @Test
    void exposesConservativeOrdinarySqlDefaults() {
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults();

        assertEquals(Duration.ofSeconds(30), options.timeout());
        assertEquals(100_000L, options.maxRows());
        assertEquals(16L * 1024 * 1024, options.maxLargeObjectBytes());
        assertEquals(16_000_000L, options.maxLargeObjectChars());
    }

    @Test
    void keepsExplicitUnlimitedSqlEscapeHatch() {
        SqlExecutionOptions options = SqlExecutionOptions.unlimited();

        assertEquals(Duration.ZERO, options.timeout());
        assertEquals(0L, options.maxRows());
        assertEquals(0L, options.maxLargeObjectBytes());
        assertEquals(0L, options.maxLargeObjectChars());
    }

    @Test
    void exposesBoundedDdlDefaults() {
        SchemaMigrationExecutionOptions options = SchemaMigrationExecutionOptions.defaults();

        assertEquals(Duration.ofSeconds(60), options.sqlExecutionOptions().timeout());
        assertEquals(Duration.ofSeconds(10), options.lockTimeout());
    }

    /**
     * 验证建表请求会渲染为 DDL 并交给响应式执行器。
     */
    @Test
    void createsTableWithReactiveSqlExecutor() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, FormSchemaSqlRenderer.create(RdbDialect.h2()));

        StepVerifier.create(client.createTable(form()))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals(List.of("create table Users (id BIGINT primary key, name VARCHAR)"),
                     executor.sqlRequests());
    }

    /**
     * 验证响应式结构客户端可以直接接收 RDB 方言，并使用方言驱动结构 SQL 渲染。
     */
    @Test
    void createsTableWithRdbDialect() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.mysql());

        StepVerifier.create(client.createTable(form()))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals(List.of("create table `Users` (`id` BIGINT primary key, `name` VARCHAR(255))"),
                     executor.sqlRequests());
    }

    /** 直接建表和迁移也必须在实际发出 DDL 后精确失效目标表缓存。 */
    @Test
    void invalidatesConfiguredMetadataAfterDirectCreateAndMigrate() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        List<String> invalidated = new ArrayList<>();
        SchemaMigrationExecutionOptions options = SchemaMigrationExecutionOptions.defaults()
                                                                                 .withTimeout(Duration.ofSeconds(7));
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.h2())
                                                         .withDefaultMigrationExecutionOptions(options)
                                                         .withMetadataInvalidator(invalidated::add);
        DynamicForm source = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "VARCHAR"))
                                        .build();

        StepVerifier.create(client.createTable(source)).expectNext(1L).verifyComplete();
        StepVerifier.create(client.migrate(source.diffTo(target))).expectNext(1L).verifyComplete();

        assertEquals(List.of("Users", "Users"), invalidated);
        assertEquals(List.of(options.sqlExecutionOptions(), options.sqlExecutionOptions()),
                     executor.executionOptions());
    }

    /** 响应式 DDL 的缓存失效回调不得吞掉被 RuntimeException 包装的 JVM 致命错误。 */
    @Test
    void propagatesVirtualMachineErrorNestedInMetadataInvalidationFailure() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        OutOfMemoryError fatal = new OutOfMemoryError("reactive metadata invalidation fatal");
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.h2())
                                                         .withMetadataInvalidator(ignored -> {
                                                             throw new IllegalStateException(
                                                                     "invalidator wrapper", fatal);
                                                         });

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class, () -> client.createTable(form()).block());

        assertSame(fatal, observed);
        assertEquals(List.of("create table Users (id BIGINT primary key, name VARCHAR)"),
                     executor.sqlRequests());
    }

    /** 同一个冷 DDL Publisher 的每次订阅都独立执行并失效，不共享前一次订阅的终止状态。 */
    @Test
    void invalidatesConfiguredMetadataForEachDirectDdlSubscription() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        List<String> invalidated = new CopyOnWriteArrayList<>();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.h2())
                                                         .withMetadataInvalidator(invalidated::add);
        Mono<Long> createTable = client.createTable(form());

        StepVerifier.create(Mono.when(
                createTable.subscribeOn(reactor.core.scheduler.Schedulers.parallel()),
                createTable.subscribeOn(reactor.core.scheduler.Schedulers.parallel())))
                    .verifyComplete();

        assertEquals(List.of("Users", "Users"), List.copyOf(invalidated));
        assertEquals(2, executor.sqlRequests().size());
    }

    /** 事务门禁在发送 DDL 前拒绝时，直接入口也不能误清理元数据缓存。 */
    @Test
    void doesNotInvalidateConfiguredMetadataWhenDirectDdlIsRejected() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.bindExternalTransaction();
        List<String> invalidated = new ArrayList<>();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.mysql())
                                                         .withMetadataInvalidator(invalidated::add);

        StepVerifier.create(client.createTable(form()))
                    .expectError(SchemaMigrationRejectedException.class)
                    .verify();

        assertTrue(invalidated.isEmpty());
    }

    /** 自动迁移同时失效用于规划的 reader 和客户端装配的跨内核缓存回调。 */
    @Test
    void invalidatesPlanningReaderAndConfiguredMetadataAfterAutomaticDdl() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        InvalidatingMetadataReader reader = new InvalidatingMetadataReader(
                TableMetadata.builder("Users")
                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                             .build());
        List<String> configuredInvalidated = new ArrayList<>();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.h2())
                                                         .withMetadataInvalidator(configuredInvalidated::add);

        StepVerifier.create(client.createOrAlter(form(), List.of(), reader))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals(List.of("Users"), reader.invalidatedTables);
        assertEquals(List.of("Users"), configuredInvalidated);
    }

    /** 自动迁移的规划 reader 若包装 VME，不能被双缓存隔离逻辑静默吞掉。 */
    @Test
    void propagatesVirtualMachineErrorNestedInPlanningReaderInvalidationFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("planning reader invalidation fatal");
        InvalidatingMetadataReader reader = new InvalidatingMetadataReader(
                TableMetadata.builder("Users")
                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                             .build(),
                new IllegalStateException("reader invalidator wrapper", fatal));
        ReactiveSchemaClient client = ReactiveSchemaClient.create(new RecordingSqlExecutor(), RdbDialect.h2());

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class, () -> client.createOrAlter(form(), List.of(), reader).block());

        assertSame(fatal, observed);
    }

    /** 自动迁移装配的第二条缓存回调也必须保持嵌套 VME 的原对象身份。 */
    @Test
    void propagatesVirtualMachineErrorNestedInConfiguredAutomaticInvalidationFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("configured automatic invalidation fatal");
        InvalidatingMetadataReader reader = new InvalidatingMetadataReader(
                TableMetadata.builder("Users")
                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                             .build());
        ReactiveSchemaClient client = ReactiveSchemaClient.create(new RecordingSqlExecutor(), RdbDialect.h2())
                                                         .withMetadataInvalidator(ignored -> {
                                                             throw new IllegalStateException(
                                                                     "configured invalidator wrapper", fatal);
                                                         });

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class, () -> client.createOrAlter(form(), List.of(), reader).block());

        assertSame(fatal, observed);
        assertEquals(List.of("Users"), reader.invalidatedTables);
    }

    /** 审核通过的响应式迁移也必须通知规划 reader 和装配的跨内核缓存。 */
    @Test
    void invalidatesPlanningReaderAndConfiguredMetadataAfterReviewedDdl() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        InvalidatingMetadataReader reader = new InvalidatingMetadataReader(currentUsersTable());
        List<String> configuredInvalidated = new ArrayList<>();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.h2())
                                                         .withMetadataInvalidator(configuredInvalidated::add);

        StepVerifier.create(reviewAddName(client, reader)
                                    .flatMap(reviewed -> client.executeReviewed(
                                            reviewed,
                                            reader,
                                            SchemaMigrationExecutionOptions.defaults()
                                                                            .withLockTimeout(Duration.ZERO))))
                    .expectNextMatches(result -> result.rowsUpdated() == 1L)
                    .verifyComplete();

        assertEquals(List.of("users"), reader.invalidatedTables);
        assertEquals(List.of("users"), configuredInvalidated);
    }

    /** MySQL DDL 会隐式提交，外部业务事务中必须在第一条结构 SQL 前拒绝。 */
    @Test
    void rejectsImplicitCommitDdlInsideExternalTransaction() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.bindExternalTransaction();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.mysql());

        StepVerifier.create(client.createTable(form()))
                    .expectErrorSatisfies(error -> {
                        SchemaMigrationRejectedException rejected = assertInstanceOf(
                                SchemaMigrationRejectedException.class, error);
                        assertEquals(SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED,
                                     rejected.failureCode());
                    })
                    .verify();

        assertTrue(executor.sqlRequests().isEmpty());
    }

    /** 自动迁移在外部事务门禁拒绝时没有执行 DDL，不能错误地清空当前表的元数据缓存。 */
    @Test
    void doesNotInvalidateMetadataWhenExternalTransactionRejectsAutomaticDdl() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.bindExternalTransaction();
        InvalidatingMetadataReader reader = new InvalidatingMetadataReader(
                TableMetadata.builder("Users")
                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                             .build());
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.mysql());

        StepVerifier.create(client.createOrAlter(form(), List.of(), reader))
                    .expectErrorSatisfies(error -> {
                        SchemaMigrationRejectedException rejected = assertInstanceOf(
                                SchemaMigrationRejectedException.class, error);
                        assertEquals(SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED,
                                     rejected.failureCode());
                    })
                    .verify();

        assertTrue(executor.sqlRequests().isEmpty());
        assertTrue(reader.invalidatedTables.isEmpty());
    }

    /** PostgreSQL 普通 DDL 可以加入上层事务，最终提交或回滚仍由外部事务决定。 */
    @Test
    void allowsTransactionalDdlInsideExternalTransaction() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.bindExternalTransaction();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.postgresql());

        StepVerifier.create(client.createTable(form())).expectNext(1L).verifyComplete();

        assertEquals(1, executor.sqlRequests().size());
    }

    /** 即使方言支持普通事务 DDL，PostgreSQL 并发索引这类明确的非事务语句也不能混入外部事务。 */
    @Test
    void rejectsReviewedNonTransactionalDdlInsideExternalTransaction() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.bindExternalTransaction();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.postgresql());
        SqlRequest concurrentIndex = new SqlRequest(
                "create index concurrently idx_users_name on users (name)", List.of());
        SchemaMigrationPlan migration = new SchemaMigrationPlan(
                form(), List.of(), true, List.of(concurrentIndex), List.of());
        ReviewedSchemaMigrationPlan reviewed = new ReviewedSchemaMigrationPlan(
                migration,
                new SchemaRollbackPlan(List.of(), List.of()),
                new OnlineDdlReview(OnlineDdlMode.ALLOW_BLOCKING,
                                    SchemaOnlineDdlSupport.CONCURRENT_INDEX,
                                    List.of(),
                                    true));

        StepVerifier.create(client.executeReviewed(
                            reviewed,
                            new InvalidatingMetadataReader(currentUsersTable()),
                            SchemaMigrationExecutionOptions.defaults()))
                    .expectErrorSatisfies(error -> {
                        SchemaMigrationRejectedException rejected = assertInstanceOf(
                                SchemaMigrationRejectedException.class, error);
                        assertEquals(SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED,
                                     rejected.failureCode());
                    })
                    .verify();

        assertTrue(executor.sqlRequests().isEmpty());
    }

    /**
     * 验证迁移请求会按 add、alter、drop 的顺序串行执行并汇总影响结果。
     */
    @Test
    void migratesFormSchemaSequentiallyAndSumsResults() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, FormSchemaSqlRenderer.create(RdbDialect.h2()));
        DynamicForm source = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "VARCHAR"))
                                        .addField(DynamicField.of("age", "INTEGER"))
                                        .build();
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "TEXT"))
                                        .addField(DynamicField.of("email", "VARCHAR"))
                                        .build();

        StepVerifier.create(client.migrate(source.diffTo(target)))
                    .expectNext(3L)
                    .verifyComplete();

        assertEquals(List.of("alter table Users add column email VARCHAR",
                             "alter table Users alter column name type TEXT",
                             "alter table Users drop column age"),
                     executor.sqlRequests());
    }

    /** 多条已执行 DDL 的影响行数不能发生 long 回绕，溢出必须转换为稳定数据库错误。 */
    @Test
    void rejectsAffectedRowCountOverflowInsteadOfWrappingReactiveSchemaResult() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.withAffectedRows(Long.MAX_VALUE, 1L);
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.h2());
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("email", "VARCHAR"))
                                        .build();

        StepVerifier.create(client.migrate(form().diffTo(target)))
                    .expectErrorSatisfies(error -> {
                        RdbException rdb = assertInstanceOf(RdbException.class, error);
                        assertEquals(RdbErrorKind.UNKNOWN, rdb.kind());
                        assertInstanceOf(ArithmeticException.class, rdb.getCause());
                    })
                    .verify();
    }

    @Test
    void createOrAlterCreatesThenAddsMissingColumnsAndIndexesOnH2() {
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("schema_create_or_alter")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build()));
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.h2());
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.create(executor, RdbDialect.h2());
        DynamicForm initial = DynamicForm.builder("orders", "Orders")
                                         .addField(DynamicField.primaryKey("id", "BIGINT"))
                                         .addField(DynamicField.of("order_no", "VARCHAR")
                                                               .withLength(32))
                                         .build();
        DynamicForm target = DynamicForm.builder("orders", "Orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("order_no", "VARCHAR")
                                                              .withLength(32))
                                        .addField(DynamicField.of("status", "VARCHAR")
                                                              .withLength(16))
                                        .build();

        Mono<TableMetadata> scenario = client.createOrAlter(initial,
                                                            List.of(IndexMetadata.builder("uk_orders_order_no")
                                                                                 .unique()
                                                                                 .addColumn("order_no")
                                                                                 .build()),
                                                            reader)
                                             .then(client.createOrAlter(target,
                                                                        List.of(
                                                                                IndexMetadata.builder("uk_orders_order_no")
                                                                                             .unique()
                                                                                             .addColumn("order_no")
                                                                                             .build(),
                                                                                IndexMetadata.builder("idx_orders_status")
                                                                                             .addColumn("status")
                                                                                             .build()),
                                                                        reader))
                                             .then(reader.readTable("PUBLIC", "ORDERS"));

        StepVerifier.create(scenario)
                    .assertNext(table -> {
                        assertEquals("VARCHAR", table.column("STATUS").dataType());
                        assertEquals(List.of("ORDER_NO"), table.index("UK_ORDERS_ORDER_NO").columns());
                        assertTrue(table.index("UK_ORDERS_ORDER_NO").unique());
                        assertEquals(List.of("STATUS"), table.index("IDX_ORDERS_STATUS").columns());
                    })
                    .verifyComplete();
    }

    @Test
    void executesApprovedPlanWithProtectionAndInvalidatesMetadataAfterSuccess() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        List<SchemaMigrationObservation> observations = new ArrayList<>();
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("legacy", "VARCHAR"))
                                             .build();
        InvalidatingMetadataReader reader = new InvalidatingMetadataReader(current);
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.postgresql())
                                                          .withMigrationObserver(observations::add);
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();

        Mono<SchemaMigrationResult> execution = client.reviewCreateOrAlter(
                        target,
                        List.of(),
                        List.of(),
                        reader,
                        SchemaMigrationOptions.safe().allowDropColumn(),
                        SchemaMigrationReviewPolicy.preferOnline())
                .flatMap(reviewed -> client.executeReviewed(
                        reviewed,
                        reader,
                        SchemaMigrationExecutionOptions.defaults()
                                                       .withTimeout(Duration.ofSeconds(3))
                                                       .withLockTimeout(Duration.ofMillis(1500))
                                                       .withApproval(SchemaMigrationApproval.approve(
                                                               reviewed, "已确认备份"))));

        StepVerifier.create(execution)
                    .assertNext(result -> {
                        assertEquals(1L, result.rowsUpdated());
                        assertEquals(1, result.steps().size());
                    })
                    .verifyComplete();

        assertEquals(List.of("alter table \"users\" drop column \"legacy\""), executor.sqlRequests());
        assertEquals(Duration.ofSeconds(3), executor.executionOptions().getFirst().timeout());
        assertEquals(List.of("set lock_timeout = '1500ms'"), executor.sequence.setup().stream()
                                                                         .map(SqlRequest::sql).toList());
        assertEquals(List.of("reset lock_timeout"), executor.sequence.cleanup().stream()
                                                                  .map(SqlRequest::sql).toList());
        assertEquals(List.of("users"), reader.invalidatedTables);
        assertEquals(1, observations.size());
        assertEquals(SqlExecutionStatus.SUCCESS, observations.getFirst().status());
        assertEquals(SqlFailureCategory.NONE, observations.getFirst().failureCategory());
        assertEquals(1, observations.getFirst().completedSteps());
        assertEquals(1L, observations.getFirst().rowsUpdated());
    }

    @Test
    void refusesDangerousOptionsOnAutomaticEntry() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        InvalidatingMetadataReader reader = new InvalidatingMetadataReader(
                TableMetadata.builder("users")
                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                             .build());
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.postgresql());

        StepVerifier.create(client.createOrAlterDetailed(
                            form(),
                            List.of(),
                            reader,
                            SchemaMigrationOptions.safe().allowDropColumn()))
                    .expectErrorMatches(error -> error instanceof IllegalStateException
                            && error.getMessage().contains("reviewCreateOrAlter"))
                    .verify();

        assertTrue(executor.sqlRequests().isEmpty());
        assertTrue(reader.invalidatedTables.isEmpty());
    }

    @Test
    void reportsCancellationOnceWithoutTurningItIntoAnError() {
        List<SchemaMigrationObservation> observations = new ArrayList<>();
        ReactiveSqlExecutor executor = new NeverCompletingSqlExecutor();
        InvalidatingMetadataReader reader = new InvalidatingMetadataReader(currentUsersTable());
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.postgresql())
                                                          .withMigrationObserver(observations::add);

        StepVerifier.create(reviewAddName(client, reader)
                                    .flatMap(reviewed -> client.executeReviewed(
                                            reviewed,
                                            reader,
                                            SchemaMigrationExecutionOptions.defaults()
                                                                           .withLockTimeout(Duration.ZERO))))
                    .thenAwait(Duration.ofMillis(10))
                    .thenCancel()
                    .verify();

        assertEquals(1, observations.size());
        assertEquals(SqlExecutionStatus.CANCELLED, observations.getFirst().status());
        assertEquals(SqlFailureCategory.CANCELLED, observations.getFirst().failureCategory());
        assertEquals(SchemaMigrationFailureCode.CANCELLED, observations.getFirst().failureCode());
        assertEquals(List.of("users"), reader.invalidatedTables);
    }

    @Test
    void keepsCompletedProgressWhenSessionCleanupFails() {
        List<SchemaMigrationObservation> observations = new ArrayList<>();
        CleanupFailingSqlExecutor executor = new CleanupFailingSqlExecutor();
        InvalidatingMetadataReader reader = new InvalidatingMetadataReader(currentUsersTable());
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.postgresql())
                                                          .withMigrationObserver(observations::add);

        StepVerifier.create(reviewAddName(client, reader)
                                    .flatMap(reviewed -> client.executeReviewed(
                                            reviewed,
                                            reader,
                                            SchemaMigrationExecutionOptions.defaults()
                                                                           .withLockTimeout(Duration.ofSeconds(1)))))
                    .expectError(SqlExecutionSequenceException.class)
                    .verify();

        SchemaMigrationObservation observation = observations.getFirst();
        assertEquals(SqlExecutionStatus.ERROR, observation.status());
        assertEquals(SchemaMigrationFailureCode.CLEANUP_FAILED, observation.failureCode());
        assertEquals(SqlExecutionPhase.CLEANUP, observation.failedPhase());
        assertEquals(1, observation.completedSteps());
        assertEquals(7L, observation.rowsUpdated());
        assertEquals(List.of("users"), reader.invalidatedTables);
    }

    @Test
    void observerFailureDoesNotChangeSuccessfulMigrationResult() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        List<SchemaMigrationObservation> observations = new ArrayList<>();
        InvalidatingMetadataReader reader = new InvalidatingMetadataReader(currentUsersTable());
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.postgresql())
                                                          .withMigrationObserver(SchemaMigrationObservers.composite(
                                                                  ignored -> {
                                                                      throw new IllegalStateException(
                                                                              "metrics backend unavailable");
                                                                  },
                                                                  observations::add))
                                                          .withDefaultMigrationExecutionOptions(
                                                                  SchemaMigrationExecutionOptions.defaults()
                                                                                                 .withTimeout(
                                                                                                         Duration.ofSeconds(2)));

        StepVerifier.create(reviewAddName(client, reader)
                                    .flatMap(reviewed -> client.executeReviewed(reviewed, reader)))
                    .expectNextMatches(result -> result.rowsUpdated() == 1L)
                    .verifyComplete();

        assertEquals(List.of("users"), reader.invalidatedTables);
        assertEquals(1, observations.size());
        assertEquals(Duration.ofSeconds(2), executor.executionOptions().getFirst().timeout());
    }

    @Test
    void exposesStableFailureCodeWhenExactApprovalIsMissing() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("legacy", "VARCHAR"))
                                             .build();
        InvalidatingMetadataReader reader = new InvalidatingMetadataReader(current);
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, RdbDialect.postgresql());
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();

        StepVerifier.create(client.reviewCreateOrAlter(
                                   target,
                                   List.of(),
                                   List.of(),
                                   reader,
                                   SchemaMigrationOptions.safe().allowDropColumn(),
                                   SchemaMigrationReviewPolicy.preferOnline())
                           .flatMap(reviewed -> client.executeReviewed(
                                   reviewed, reader, SchemaMigrationExecutionOptions.defaults())))
                    .expectErrorMatches(error -> error instanceof SchemaMigrationRejectedException rejected
                            && rejected.failureCode() == SchemaMigrationFailureCode.APPROVAL_REQUIRED)
                    .verify();
    }

    private static Mono<ReviewedSchemaMigrationPlan> reviewAddName(ReactiveSchemaClient client,
                                                                    ReactiveFormMetadataReader reader) {
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "VARCHAR"))
                                        .build();
        return client.reviewCreateOrAlter(target,
                                          List.of(),
                                          List.of(),
                                          reader,
                                          SchemaMigrationOptions.safe(),
                                          SchemaMigrationReviewPolicy.preferOnline());
    }

    private static TableMetadata currentUsersTable() {
        return TableMetadata.builder("users")
                            .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                            .build();
    }

    @SuppressWarnings("unchecked")
    private static Connection connection() {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                                                    new Class<?>[]{Connection.class},
                                                    (proxy, method, args) -> defaultValue(method));
    }

    private static Object defaultValue(Method method) {
        if (Publisher.class.isAssignableFrom(method.getReturnType())) {
            return Mono.empty();
        }
        if (method.getReturnType() == boolean.class) {
            return false;
        }
        if (method.getReturnType() == int.class) {
            return 0;
        }
        if (method.getReturnType() == long.class) {
            return 0L;
        }
        return null;
    }

    private static DynamicForm form() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .build();
    }

    private static final class RecordingSqlExecutor
            implements ReactiveSqlExecutor, ConnectionScopedReactiveSqlExecutor {

        private final List<SqlRequest> requests = new CopyOnWriteArrayList<>();

        private final List<SqlExecutionOptions> executionOptions = new CopyOnWriteArrayList<>();

        private final List<Long> affectedRows = new ArrayList<>();

        private int affectedRowIndex;

        private SqlExecutionSequence sequence;

        private R2dbcTransactionContext transaction;

        @Override
        public Mono<R2dbcTransactionContext> currentTransaction() {
            return Mono.justOrEmpty(transaction);
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.error(new UnsupportedOperationException("schema client must not query rows"));
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            requests.add(request);
            return Mono.just(nextAffectedRows());
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
            executionOptions.add(options);
            return rowsUpdated(request);
        }

        @Override
        public Mono<SqlExecutionSequenceResult> executeInConnection(SqlExecutionSequence sequence,
                                                                    SqlExecutionOptions options) {
            this.sequence = sequence;
            this.executionOptions.add(options);
            this.requests.addAll(sequence.work());
            List<SqlExecutionStepResult> results = new ArrayList<>();
            for (int index = 0; index < sequence.work().size(); index++) {
                results.add(new SqlExecutionStepResult(index, sequence.work().get(index), 1L, 1L));
            }
            return Mono.just(new SqlExecutionSequenceResult(results));
        }

        private List<String> sqlRequests() {
            return requests.stream().map(SqlRequest::sql).toList();
        }

        private List<SqlExecutionOptions> executionOptions() {
            return List.copyOf(executionOptions);
        }

        private void withAffectedRows(long... rows) {
            affectedRows.clear();
            for (long rowsUpdated : rows) {
                affectedRows.add(rowsUpdated);
            }
            affectedRowIndex = 0;
        }

        private long nextAffectedRows() {
            return affectedRowIndex < affectedRows.size() ? affectedRows.get(affectedRowIndex++) : 1L;
        }

        private void bindExternalTransaction() {
            transaction = R2dbcTransactionContext.external(connection(), "primary");
        }
    }

    private static final class NeverCompletingSqlExecutor implements ReactiveSqlExecutor {

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.error(new UnsupportedOperationException("schema client must not query rows"));
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.never();
        }
    }

    private static final class CleanupFailingSqlExecutor
            implements ReactiveSqlExecutor, ConnectionScopedReactiveSqlExecutor {

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.error(new UnsupportedOperationException("schema client must not query rows"));
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.error(new UnsupportedOperationException("lock timeout path must use one connection"));
        }

        @Override
        public Mono<SqlExecutionSequenceResult> executeInConnection(SqlExecutionSequence sequence,
                                                                    SqlExecutionOptions options) {
            SqlExecutionStepResult completed = new SqlExecutionStepResult(
                    0, sequence.work().getFirst(), 7L, 1L);
            return Mono.error(new SqlExecutionSequenceException(
                    SqlExecutionPhase.CLEANUP,
                    0,
                    List.of(completed),
                    new IllegalStateException("failed to reset session setting")));
        }
    }

    private static final class InvalidatingMetadataReader implements ReactiveFormMetadataReader {

        private final List<String> invalidatedTables = new ArrayList<>();

        private final TableMetadata current;

        private final RuntimeException invalidationFailure;

        private InvalidatingMetadataReader(TableMetadata current) {
            this(current, null);
        }

        private InvalidatingMetadataReader(TableMetadata current, RuntimeException invalidationFailure) {
            this.current = current;
            this.invalidationFailure = invalidationFailure;
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            return Mono.error(new UnsupportedOperationException("this test only verifies cache invalidation"));
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            return Mono.error(new UnsupportedOperationException("this test only verifies cache invalidation"));
        }

        @Override
        public Mono<TableMetadata> readTable(String table) {
            return Mono.just(current);
        }

        @Override
        public void invalidate(String table) {
            if (invalidationFailure != null) {
                throw invalidationFailure;
            }
            invalidatedTables.add(table);
        }
    }
}
