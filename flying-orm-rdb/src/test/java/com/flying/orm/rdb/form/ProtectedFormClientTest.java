package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.FormSchemaSqlRenderer;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证显式受保护字段贯穿动态表单写入、查询、解密和展示控制的公共契约。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
class ProtectedFormClientTest {

    /** 从 DDL、原生 JDBC 写事务到 CONTAINS 两阶段查询必须形成一条完整可用链路。 */
    @Test
    void writesAndSearchesContainsIndexThroughTheNativeSyncClient() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:protected_form_contains;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        DynamicForm form = protectedContainsForm();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(5))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);
            FormSchemaSqlRenderer.create(RdbDialect.h2()).createTable(form)
                                 .forEach(executor::rowsUpdated);
            SyncFormClient client = SyncFormClient.create(
                    executor, SyncBatchExecutor.jdbc(dataSource), renderer);

            assertEquals(1L, client.insert(WriteSpec.insert(
                    form, Map.of("id", 1L, "contact", "AlphaBeta"))));
            List<DynamicRow> rows = client.select(QuerySpec.of(
                    form,
                    ConditionGroup.and().add(ProtectedConditions.contains("contact", "PHAB")).build())
                                                           .showSensitive());

            assertEquals(1, rows.size());
            assertEquals("AlphaBeta", rows.getFirst().get("contact"));
        }
    }

    /**
     * CONTAINS 批量写入必须让业务行和侧索引共享现有批量事务；ATOMIC 与 INDEPENDENT 都不能退化为提交后的补写。
     */
    @Test
    void maintainsContainsIndexInsideNativeSyncBatchTransactions() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:protected_form_contains_batch;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        DynamicForm form = protectedContainsForm();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(6))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);
            FormSchemaSqlRenderer.create(RdbDialect.h2()).createTable(form)
                                 .forEach(executor::rowsUpdated);
            SyncFormClient client = SyncFormClient.create(
                    executor, SyncBatchExecutor.jdbc(dataSource), renderer);

            BatchWriteResult atomic = client.writeBatch(BatchSpec.insert(
                    form,
                    Flux.just(Map.<String, Object>of("id", 1L, "contact", "AlphaBeta"),
                              Map.<String, Object>of("id", 2L, "contact", "GammaDelta")))
                                                                 .withOptions(BatchWriteOptions.atomic(2)));
            BatchWriteResult independent = client.writeBatch(BatchSpec.insert(
                    form,
                    Flux.just(Map.<String, Object>of("id", 3L, "contact", "ThetaSigma")))
                                                                      .withOptions(
                                                                              BatchWriteOptions.independent(1)));

            assertEquals(BatchWriteResult.Status.COMMITTED, atomic.status());
            assertEquals(2L, atomic.affectedRows());
            assertEquals(BatchWriteResult.Status.COMMITTED, independent.status());
            assertEquals(1L, independent.affectedRows());
            assertEquals(List.of(1L), matchingIds(client, form, "PHAB"));
            assertEquals(List.of(2L), matchingIds(client, form, "MADE"));
            assertEquals(List.of(3L), matchingIds(client, form, "TASI"));
        }
    }

    /** R2DBC 批量必须在原事务连接内维护 CONTAINS 侧索引，不能在业务提交后补写。 */
    @Test
    void maintainsContainsIndexInsideNativeReactiveBatchTransactions() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("protected_form_contains_reactive_batch")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build());
        DynamicForm form = protectedContainsForm();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(8))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            ReactiveSqlExecutor executor = com.flying.orm.rdb.reactive.R2dbcSqlExecutor.create(connectionFactory)
                    .withObserver(com.flying.orm.rdb.observation.SqlExecutionObserver.noop());
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer);
            Mono<List<Long>> scenario = Flux.fromIterable(FormSchemaSqlRenderer.create(RdbDialect.h2())
                                                                                .createTable(form))
                    .concatMap(executor::rowsUpdated)
                    .then(client.writeBatch(BatchSpec.insert(
                            form,
                            Flux.just(Map.<String, Object>of("id", 1L, "contact", "AlphaBeta"),
                                      Map.<String, Object>of("id", 2L, "contact", "GammaDelta")))
                                                       .withOptions(BatchWriteOptions.atomic(2))))
                    .thenMany(client.select(QuerySpec.of(
                            form,
                            ConditionGroup.and()
                                          .add(ProtectedConditions.contains("contact", "PHAB"))
                                          .build()).withProjection(List.of("id"), List.of())))
                    .map(row -> ((Number) row.get("id")).longValue())
                    .collectList();

            StepVerifier.create(scenario)
                        .expectNext(List.of(1L))
                        .verifyComplete();
        }
    }

    /** JDBC 侧索引维护失败时，ATOMIC 批量必须回滚已经写入的业务行。 */
    @Test
    void rollsBackNativeSyncBatchWhenContainsIndexMaintenanceFails() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:protected_form_contains_sync_rollback;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        DynamicForm form = protectedContainsForm();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(16))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);
            executor.rowsUpdated(FormSchemaSqlRenderer.create(RdbDialect.h2()).createTable(form).getFirst());
            SyncFormClient client = SyncFormClient.create(
                    executor, SyncBatchExecutor.jdbc(dataSource), renderer);

            BatchWriteException failure = assertThrows(BatchWriteException.class, () -> client.writeBatch(
                    BatchSpec.insert(form, Flux.just(Map.<String, Object>of(
                            "id", 1L, "contact", "AlphaBeta")))
                             .withOptions(BatchWriteOptions.atomic(1))));

            assertEquals(BatchWriteResult.Status.ROLLED_BACK, failure.result().status());
            assertTrue(client.select(QuerySpec.of(form, ConditionGroup.and().build())).isEmpty());
        }
    }

    /** R2DBC 侧索引维护失败时，ATOMIC 批量必须在同一连接上回滚业务行。 */
    @Test
    void rollsBackNativeReactiveBatchWhenContainsIndexMaintenanceFails() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("protected_form_contains_reactive_rollback")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build());
        DynamicForm form = protectedContainsForm();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(18))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            ReactiveSqlExecutor executor = com.flying.orm.rdb.reactive.R2dbcSqlExecutor.create(connectionFactory);
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer);
            Mono<Long> scenario = executor.rowsUpdated(
                    FormSchemaSqlRenderer.create(RdbDialect.h2()).createTable(form).getFirst())
                    .then(client.writeBatch(BatchSpec.insert(
                            form, Flux.just(Map.<String, Object>of("id", 1L, "contact", "AlphaBeta")))
                                                      .withOptions(BatchWriteOptions.atomic(1))))
                    .thenReturn(-1L)
                    .onErrorResume(BatchWriteException.class, failure -> {
                        assertEquals(BatchWriteResult.Status.ROLLED_BACK, failure.result().status());
                        return client.select(QuerySpec.of(form, ConditionGroup.and().build())).count();
                    });

            StepVerifier.create(scenario)
                        .expectNext(0L)
                        .verifyComplete();
        }
    }

    /** 批量 UPSERT 与乐观更新必须替换旧令牌，并在 JDBC/R2DBC 共享规划中保持相同语义。 */
    @Test
    void replacesContainsTokensForNativeReactiveBatchUpsertAndUpdate() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("protected_form_contains_reactive_update")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build());
        DynamicForm form = protectedContainsVersionedForm();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(10))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            ReactiveSqlExecutor executor = com.flying.orm.rdb.reactive.R2dbcSqlExecutor.create(connectionFactory);
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer);
            BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                    Map.of("contact", "OmegaBeta"),
                    ConditionGroup.and().where("id", "=", 1L).build(),
                    OptimisticLockOptions.increment("version", 1));
            Mono<List<List<Long>>> scenario = Flux.fromIterable(FormSchemaSqlRenderer.create(RdbDialect.h2())
                                                                                     .createTable(form))
                    .concatMap(executor::rowsUpdated)
                    .then(client.writeBatch(BatchSpec.upsert(
                            form,
                            Flux.just(Map.<String, Object>of(
                                    "id", 1L, "contact", "AlphaBeta", "version", 1)))))
                    .then(client.writeBatch(BatchSpec.update(form, Flux.just(update))))
                    .then(Mono.zip(matchingIds(client, form, "PHAB"),
                                   matchingIds(client, form, "MEGA")))
                    .map(tuple -> List.of(tuple.getT1(), tuple.getT2()));

            StepVerifier.create(scenario)
                        .expectNext(List.of(List.of(), List.of(1L)))
                        .verifyComplete();
        }
    }

    /** JDBC 批量使用同一保护规划，并在乐观更新前读取 owner、成功后替换令牌。 */
    @Test
    void replacesContainsTokensForNativeSyncBatchUpsertAndUpdate() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:protected_form_contains_sync_update;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        DynamicForm form = protectedContainsVersionedForm();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(12))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);
            FormSchemaSqlRenderer.create(RdbDialect.h2()).createTable(form)
                                 .forEach(executor::rowsUpdated);
            SyncFormClient client = SyncFormClient.create(
                    executor, SyncBatchExecutor.jdbc(dataSource), renderer);
            client.writeBatch(BatchSpec.upsert(
                    form,
                    Flux.just(Map.<String, Object>of(
                            "id", 1L, "contact", "AlphaBeta", "version", 1))));
            client.writeBatch(BatchSpec.update(form, Flux.just(new BatchOptimisticUpdate(
                    Map.of("contact", "OmegaBeta"),
                    ConditionGroup.and().where("id", "=", 1L).build(),
                    OptimisticLockOptions.increment("version", 1)))));

            assertEquals(List.of(), matchingIds(client, form, "PHAB"));
            assertEquals(List.of(1L), matchingIds(client, form, "MEGA"));
        }
    }

    /** 数据库生成主键必须立即成为同事务侧索引 owner，不能等批量完成后猜测或补写。 */
    @Test
    void usesReactiveBatchGeneratedKeysAsContainsIndexOwners() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("protected_form_contains_generated_key")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build());
        DynamicForm form = protectedContainsIdentityForm();
        Map<Long, Long> generated = new ConcurrentHashMap<>();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(14))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            ReactiveSqlExecutor executor = com.flying.orm.rdb.reactive.R2dbcSqlExecutor.create(connectionFactory);
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer);
            BatchSpec insert = BatchSpec.insert(
                    form, Flux.just(Map.<String, Object>of("contact", "AlphaBeta")))
                                        .withGeneratedKeys(BatchGeneratedKeys.required(
                                                "id", (offset, row) -> generated.put(
                                                        offset, ((Number) row.value(0)).longValue())));
            Mono<List<Long>> scenario = Flux.fromIterable(FormSchemaSqlRenderer.create(RdbDialect.h2())
                                                                                .createTable(form))
                    .concatMap(executor::rowsUpdated)
                    .then(client.writeBatch(insert))
                    .then(matchingIds(client, form, "PHAB"));

            StepVerifier.create(scenario)
                        .assertNext(ids -> {
                            assertEquals(1, ids.size());
                            assertEquals(generated.get(0L), ids.getFirst());
                        })
                        .verifyComplete();
        }
    }

    /**
     * 写入只把密文和声明的盲索引交给执行器；读取默认按字段声明脱敏，可信查询可显式取完整值。
     */
    @Test
    void protectsWritesAndSupportsExactSearchWithDeclaredOrFullDisplay() {
        RecordingExecutor executor = new RecordingExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(7))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                    .withProtectedFields(ProtectedFieldRuntime.create(keys));
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer)
                                                          .withDefaultDataScope(
                                                                  DataScope.tenant("tenant_id", "tenant-a"));
            DynamicForm form = protectedForm();

            StepVerifier.create(client.insert(WriteSpec.insert(form, Map.of("contact", "13800138000"))))
                        .expectNext(1L)
                        .verifyComplete();

            SqlRequest insert = executor.request;
            assertTrue(insert.sql().startsWith("insert into customer"));
            assertFalse(insert.sql().contains("13800138000"));
            assertEquals(4, insert.parameters().size());
            byte[] ciphertext = assertInstanceOf(byte[].class, insert.parameters().get(0));
            byte[] exactToken = assertInstanceOf(byte[].class, insert.parameters().get(1));
            byte[] suffixToken = assertInstanceOf(byte[].class, insert.parameters().get(2));
            assertNotEquals("13800138000", new String(ciphertext, java.nio.charset.StandardCharsets.UTF_8));
            assertEquals(32, exactToken.length);
            assertEquals(32, suffixToken.length);
            assertEquals("tenant-a", insert.parameters().get(3));

            executor.rows = List.of(DynamicRow.copyOf(new LinkedHashMap<>(Map.of(
                    "contact", ciphertext,
                    "tenant_id", "tenant-a"))));
            QuerySpec exact = QuerySpec.of(
                    form,
                    ConditionGroup.and().add(ProtectedConditions.exact("contact", "13800138000")).build());
            StepVerifier.create(client.select(exact))
                        .assertNext(row -> assertEquals("13*******00", row.get("contact")))
                        .verifyComplete();
            assertArrayEquals(exactToken, assertInstanceOf(byte[].class, executor.request.parameters().get(0)));
            assertFalse(executor.request.sql().contains("contact = ?"));

            StepVerifier.create(client.select(exact.showSensitive()))
                        .assertNext(row -> assertEquals("13800138000", row.get("contact")))
                        .verifyComplete();
        }
    }

    /** 没有显式配置密钥环时，受保护表单必须在 SQL 交给执行器之前失败。 */
    @Test
    void rejectsProtectedFormBeforeSqlWhenKeysAreNotConfigured() {
        RecordingExecutor executor = new RecordingExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(
                executor,
                FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2()))
                                                      .withDefaultDataScope(
                                                              DataScope.tenant("tenant_id", "tenant-a"));

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> client.insert(WriteSpec.insert(protectedForm(), Map.of("contact", "13800138000"))).block());

        assertEquals("protected field key ring is not configured", error.getMessage());
        assertEquals(0, executor.writeCalls);
    }

    /** CONTAINS 索引必须与业务写入同事务；普通自定义执行器不能降级成只写密文。 */
    @Test
    void rejectsContainsWriteBeforeSqlWhenExecutorCannotMaintainTheSideIndex() {
        RecordingExecutor executor = new RecordingExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(9))) {
            ReactiveFormClient client = ReactiveFormClient.create(
                    executor,
                    FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                               .withProtectedFields(ProtectedFieldRuntime.create(keys)));

            StepVerifier.create(client.insert(WriteSpec.insert(
                                protectedContainsForm(), Map.of("id", 1L, "contact", "AlphaBeta"))))
                        .expectErrorSatisfies(error -> assertEquals(
                                "reactive sql executor does not support atomic protected writes",
                                error.getMessage()))
                        .verify();

            assertEquals(0, executor.writeCalls);
        }
    }

    /** CONTAINS 侧索引以主键定位业务行，更新主键必须在任何 SQL 执行前拒绝。 */
    @Test
    void rejectsPrimaryKeyChangesWhenContainsSideIndexIsEnabled() {
        RecordingExecutor executor = new RecordingExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(10))) {
            ReactiveFormClient client = ReactiveFormClient.create(
                    executor,
                    FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                               .withProtectedFields(ProtectedFieldRuntime.create(keys)));

            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> client.update(WriteSpec.update(
                            protectedContainsForm(),
                            Map.of("id", 2L),
                            ConditionGroup.and().where("id", "=", 1L).build())));

            assertEquals("protected contains update must not change primary key", error.getMessage());
            assertEquals(0, executor.writeCalls);
        }
    }

    /** 更新与删除条件必须和读取使用同一盲索引改写，格式化后缀应按规范化后的长度选择隐藏列。 */
    @Test
    void rewritesProtectedWriteConditionsAndNormalizedSuffixSearch() {
        RecordingExecutor executor = new RecordingExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(11))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                    .withProtectedFields(ProtectedFieldRuntime.create(keys));
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer)
                                                          .withDefaultDataScope(
                                                                  DataScope.tenant("tenant_id", "tenant-a"));
            DynamicForm form = protectedForm();

            StepVerifier.create(client.update(WriteSpec.update(
                                form,
                                Map.of("contact", "13900139000"),
                                ConditionGroup.and()
                                              .add(ProtectedConditions.exact("contact", "13800138000"))
                                              .build())))
                        .expectNext(1L)
                        .verifyComplete();

            assertTrue(executor.request.sql().contains("__fop_e_"));
            assertFalse(executor.request.sql().contains("protected-exact"));

            QuerySpec suffix = QuerySpec.of(
                    form,
                    ConditionGroup.and().add(ProtectedConditions.suffix("contact", "8-0-0-0")).build());
            StepVerifier.create(client.select(suffix))
                        .verifyComplete();

            assertTrue(executor.request.sql().contains("__fop_s4_"));
            assertEquals(32, assertInstanceOf(byte[].class, executor.request.parameters().get(0)).length);
        }
    }

    /** 批量输入逐行加密并生成盲索引，不能把首行或后续行的明文交给批执行器。 */
    @Test
    void protectsEveryReactiveBatchInsertRow() {
        RecordingExecutor executor = new RecordingExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(13))) {
            ReactiveFormClient client = ReactiveFormClient.create(
                    executor,
                    FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                               .withProtectedFields(ProtectedFieldRuntime.create(keys)))
                                                          .withDefaultDataScope(
                                                                  DataScope.tenant("tenant_id", "tenant-a"));

            StepVerifier.create(client.writeBatch(BatchSpec.insert(
                                protectedForm(),
                                Flux.just(Map.<String, Object>of("contact", "13800138000"),
                                          Map.<String, Object>of("contact", "13900139000")))))
                        .assertNext(result -> assertEquals(2L, result.affectedRows()))
                        .verifyComplete();

            assertTrue(executor.batchRequest.sql().contains("__fop_e_"));
            assertTrue(executor.batchRequest.sql().contains("__fop_s4_"));
            assertEquals(2, executor.batchRows.size());
            for (Object[] row : executor.batchRows) {
                assertInstanceOf(byte[].class, row[0]);
                assertEquals(32, assertInstanceOf(byte[].class, row[1]).length);
                assertEquals(32, assertInstanceOf(byte[].class, row[2]).length);
                assertEquals("tenant-a", row[3]);
            }
        }
    }

    /**
     * 同一受保护批量的冷重放会重新生成随机密文，但回执必须按逻辑载荷命中，不能误报 payload mismatch。
     */
    @Test
    void replaysProtectedBatchReceiptWithoutDependingOnRandomCiphertext() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("protected_receipt_replay")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .username("sa")
                                         .build());
        ReactiveSqlExecutor executor = com.flying.orm.rdb.reactive.R2dbcSqlExecutor.create(connectionFactory);
        DynamicForm form = protectedForm();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(15))) {
            ReactiveFormClient client = ReactiveFormClient.create(
                    executor,
                    FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                       .withProtectedFields(ProtectedFieldRuntime.create(keys)))
                                                          .withDefaultDataScope(
                                                                  DataScope.tenant("tenant_id", "tenant-a"));
            Mono<BatchWriteResult> operation = client.writeBatch(
                    BatchSpec.insert(form, Flux.just(Map.<String, Object>of("contact", "13800138000")))
                             .withOptions(BatchWriteOptions.atomic(1).withReceipt("protected-receipt-replay")));
            Mono<Long> scenario = Flux.fromIterable(FormSchemaSqlRenderer.create(RdbDialect.h2())
                                                                          .createTable(form))
                                      .concatMap(executor::rowsUpdated)
                                      .then(executor.rowsUpdated(SqlRequest.nativeSql(receiptTableSql(), List.of())))
                                      .then(operation)
                                      .then(operation)
                                      .then(client.select(QuerySpec.of(form, ConditionGroup.and().build())
                                                                   .showSensitive()).count());

            StepVerifier.create(scenario)
                        .expectNext(1L)
                        .verifyComplete();
        }
    }

    /**
     * 密钥轮换会同时改变密文、EXACT 和 SUFFIX 内部列；回执必须只比较稳定逻辑载荷，不能把这些内部派生值当成新请求。
     */
    @Test
    void replaysProtectedBatchReceiptAcrossEncryptionKeyRotation() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("protected_receipt_rotation")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .username("sa")
                                         .build());
        ReactiveSqlExecutor executor = com.flying.orm.rdb.reactive.R2dbcSqlExecutor.create(connectionFactory);
        DynamicForm form = protectedForm();
        byte[] stableSearchKey = key(31);
        try (ProtectedFieldKeyRing firstKeys = ProtectedFieldKeyRing.builder()
                                                                 .current("v1", key(15))
                                                                 .uniqueSearchKey(stableSearchKey)
                                                                 .build();
             ProtectedFieldKeyRing rotatedKeys = ProtectedFieldKeyRing.builder()
                                                                   .current("v2", key(16))
                                                                   .readable("v1", key(15))
                                                                   .uniqueSearchKey(stableSearchKey)
                                                                   .build()) {
            ReactiveFormClient first = protectedClient(executor, firstKeys);
            ReactiveFormClient rotated = protectedClient(executor, rotatedKeys);
            BatchSpec request = BatchSpec.insert(
                    form, Flux.just(Map.<String, Object>of("contact", "13800138000")))
                    .withOptions(BatchWriteOptions.atomic(1).withReceipt("protected-receipt-rotation"));
            Mono<Long> scenario = Flux.fromIterable(FormSchemaSqlRenderer.create(RdbDialect.h2())
                                                                          .createTable(form))
                                      .concatMap(executor::rowsUpdated)
                                      .then(executor.rowsUpdated(SqlRequest.nativeSql(receiptTableSql(), List.of())))
                                      .then(first.writeBatch(request))
                                      .then(rotated.writeBatch(request))
                                      .then(rotated.select(QuerySpec.of(form, ConditionGroup.and().build())
                                                                        .showSensitive()).count());

            StepVerifier.create(scenario)
                        .expectNext(1L)
                        .verifyComplete();
        }
    }

    /** 批量更新的 SET 与 WHERE 必须同时转换为密文列和盲索引，不能把新旧明文交给执行器。 */
    @Test
    void protectsReactiveBatchUpdateValuesAndConditions() {
        RecordingExecutor executor = new RecordingExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(17))) {
            ReactiveFormClient client = ReactiveFormClient.create(
                    executor,
                    FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                               .withProtectedFields(ProtectedFieldRuntime.create(keys)))
                                                          .withDefaultDataScope(
                                                                  DataScope.tenant("tenant_id", "tenant-a"));
            BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                    Map.of("contact", "13900139000"),
                    ConditionGroup.and().add(ProtectedConditions.exact("contact", "13800138000")).build(),
                    OptimisticLockOptions.increment("version", 1));

            StepVerifier.create(client.writeBatch(BatchSpec.update(protectedVersionedForm(), Flux.just(update))))
                        .assertNext(result -> assertEquals(1L, result.affectedRows()))
                        .verifyComplete();

            assertTrue(executor.batchRequest.sql().contains("__fop_e_"));
            assertTrue(executor.batchRequest.sql().contains("__fop_s4_"));
            for (Object value : executor.batchRows.getFirst()) {
                if (value instanceof String text) {
                    assertEquals("tenant-a", text);
                }
            }
        }
    }

    /** 受保护物理列必须映射为各数据库真实二进制类型，不能把内部逻辑类型输出到 DDL。 */
    @Test
    void mapsProtectedPhysicalColumnsForEverySupportedDialect() {
        Map<RdbDialect, List<String>> expectedTypes = Map.of(
                RdbDialect.h2(), List.of("BLOB", "BINARY(32)"),
                RdbDialect.mysql(), List.of("LONGBLOB", "BINARY(32)"),
                RdbDialect.postgresql(), List.of("BYTEA"),
                RdbDialect.oracle(), List.of("BLOB", "RAW(32)"),
                RdbDialect.sqlServer(), List.of("VARBINARY(max)", "BINARY(32)"));

        expectedTypes.forEach((dialect, types) -> {
            String sql = FormSchemaSqlRenderer.create(dialect)
                                              .createTable(protectedForm())
                                              .stream()
                                              .map(SqlRequest::sql)
                                              .collect(java.util.stream.Collectors.joining("; "));
            assertFalse(sql.contains("PROTECTED_BINARY"));
            assertFalse(sql.contains("PROTECTED_HASH"));
            types.forEach(type -> assertTrue(sql.toUpperCase(java.util.Locale.ROOT)
                                                .contains(type.toUpperCase(java.util.Locale.ROOT))));
        });
    }

    /** Oracle 密文列必须显式按 BLOB 绑定，固定长度盲索引仍保留普通二进制参数。 */
    @Test
    void bindsOracleCiphertextAsBlobWithoutTreatingBlindIndexesAsLobs() {
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(19))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.oracle())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            ProtectedFieldRuntime.PreparedWrite write = renderer.protection().prepareWrite(
                    protectedForm(), Map.of("contact", "13800138000", "tenant_id", "tenant-a"),
                    DataScope.tenant("tenant_id", "tenant-a"));
            SqlRequest request = renderer.protection().insert(write);

            List<SqlTypedValue> typed = request.parameters().stream()
                                                 .filter(SqlTypedValue.class::isInstance)
                                                 .map(SqlTypedValue.class::cast)
                                                 .toList();
            assertEquals(1, typed.size());
            SqlTypedValue ciphertext = typed.getFirst();
            assertEquals(SqlTypedValue.Kind.BLOB, ciphertext.kind());
            assertEquals(2L, request.parameters().stream().filter(byte[].class::isInstance).count());
            assertTrue(request.parameters().contains("tenant-a"));
        }
    }

    /**
     * CONTAINS 的数据库候选必须在应用内解密复核；投影未请求的明文只可用于复核，不能进入最终结果。
     */
    @Test
    void verifiesContainsCandidatesAndKeepsInternalPlaintextOutOfProjection() {
        RecordingExecutor executor = new RecordingExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(23))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer);
            DynamicForm form = protectedContainsForm();
            byte[] matching = ciphertext(renderer, form, 1L, "AlphaBeta");
            byte[] falsePositive = ciphertext(renderer, form, 2L, "AlphaGamma");
            executor.rows = List.of(
                    DynamicRow.copyOf(new LinkedHashMap<>(Map.of("id", 1L, "contact", matching))),
                    DynamicRow.copyOf(new LinkedHashMap<>(Map.of("id", 2L, "contact", falsePositive))));
            QuerySpec query = QuerySpec.of(
                    form,
                    ConditionGroup.and().add(ProtectedConditions.contains("contact", "PHAB")).build())
                                       .withProjection(List.of("id"), List.of())
                                       .withSorts(List.of(PageSort.asc("id")))
                                       .showSensitive();

            StepVerifier.create(client.select(query))
                        .assertNext(row -> assertEquals(Map.of("id", 1L), row.toMap()))
                        .verifyComplete();

            assertTrue(executor.request.sql().contains(" union ") || executor.request.sql().contains(" join ("));
            assertFalse(executor.request.parameters().contains("PHAB"));
        }
    }

    /** CONTAINS 的 total 和页内容都必须以解密复核后的真实命中为准，不能另做未复核 count。 */
    @Test
    void pagesContainsResultsAfterPlaintextVerification() {
        RecordingExecutor executor = new RecordingExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(29))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer);
            DynamicForm form = protectedContainsForm();
            executor.rows = List.of(
                    DynamicRow.copyOf(Map.of("id", 1L, "contact",
                                                      ciphertext(renderer, form, 1L, "AlphaBeta"))),
                    DynamicRow.copyOf(Map.of("id", 2L, "contact",
                                                      ciphertext(renderer, form, 2L, "AlphaGamma"))));
            QuerySpec query = QuerySpec.of(
                    form,
                    ConditionGroup.and().add(ProtectedConditions.contains("contact", "PHAB")).build())
                                       .showSensitive();

            StepVerifier.create(client.page(query, PageQuery.of(1, 1, PageSort.asc("id"))))
                        .assertNext(page -> {
                            assertEquals(1L, page.total());
                            assertEquals(1, page.rows().size());
                            assertEquals("AlphaBeta", page.rows().getFirst().get("contact"));
                        })
                        .verifyComplete();

            assertEquals(1, executor.queryCalls);
        }
    }

    /** CONTAINS 必须支持稳定游标分页，并在解密复核后再决定下一游标。 */
    @Test
    void cursorPagesContainsResultsAfterPlaintextVerification() {
        RecordingExecutor executor = new RecordingExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(30))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer);
            DynamicForm form = protectedContainsForm();
            QuerySpec query = QuerySpec.of(
                    form,
                    ConditionGroup.and().add(ProtectedConditions.contains("contact", "ALPHA")).build())
                                       .showSensitive();
            executor.rows = List.of(
                    DynamicRow.copyOf(Map.of("id", 1L, "contact",
                                                      ciphertext(renderer, form, 1L, "AlphaOne"))),
                    DynamicRow.copyOf(Map.of("id", 2L, "contact",
                                                      ciphertext(renderer, form, 2L, "AlphaTwo"))));

            StepVerifier.create(client.cursorPage(query, CursorPageQuery.first(
                                1, CursorSort.asc("id"))))
                        .assertNext(page -> {
                            assertEquals(1L, page.rows().getFirst().get("id"));
                            assertTrue(page.hasMore());
                            assertEquals(List.of(1L), page.nextCursor());
                        })
                        .verifyComplete();

            executor.rows = List.of(DynamicRow.copyOf(Map.of(
                    "id", 2L, "contact", ciphertext(renderer, form, 2L, "AlphaTwo"))));
            StepVerifier.create(client.cursorPage(query, CursorPageQuery.after(
                                1, List.of(1L), CursorSort.asc("id"))))
                        .assertNext(page -> {
                            assertEquals(2L, page.rows().getFirst().get("id"));
                            assertFalse(page.hasMore());
                        })
                        .verifyComplete();

            assertTrue(executor.request.sql().contains(" > ?"));
        }
    }

    /** 超过硬候选上限必须稳定失败，不能把前一千条截断后伪装成完整结果。 */
    @Test
    void rejectsContainsCandidateOverflowWithStableErrorCode() {
        RecordingExecutor executor = new RecordingExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(31))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            DynamicForm form = protectedContainsForm();
            byte[] encrypted = ciphertext(renderer, form, 1L, "AlphaBeta");
            java.util.ArrayList<DynamicRow> candidates = new java.util.ArrayList<>(1001);
            for (long id = 1L; id <= 1001L; id++) {
                candidates.add(DynamicRow.copyOf(Map.of("id", id, "contact", encrypted)));
            }
            executor.rows = List.copyOf(candidates);
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer);
            QuerySpec query = QuerySpec.of(
                    form,
                    ConditionGroup.and().add(ProtectedConditions.contains("contact", "PHAB")).build());

            StepVerifier.create(client.select(query))
                        .expectErrorSatisfies(error -> {
                            ProtectedSearchCandidateLimitExceededException limit = assertInstanceOf(
                                    ProtectedSearchCandidateLimitExceededException.class, error);
                            assertEquals(1000, limit.limit());
                            assertEquals(1001, limit.actual());
                            assertEquals("PROTECTED_SEARCH_CANDIDATE_LIMIT",
                                         limit.toErrorReport().code());
                        })
                        .verify();
        }
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("customer", "customer")
                          .addField(DynamicField.of("tenant_id", "VARCHAR").withNullable(false))
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .tenant("tenant_id", TenantStrategy.AUTO)
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                                 EncryptedSearchMode.EXACT,
                                                                                 EncryptedSearchMode.SUFFIX)
                                                                         .normalizer("digits")
                                                                         .suffixLengths(4)
                                                                         .build())
                          .masked("contact", MaskedFieldDefinition.builder("partial")
                                                                   .prefix(2)
                                                                   .suffix(2)
                                                                   .build())
                          .build();
    }

    private static ReactiveFormClient protectedClient(ReactiveSqlExecutor executor,
                                                        ProtectedFieldKeyRing keys) {
        return ReactiveFormClient.create(
                executor,
                FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                   .withProtectedFields(ProtectedFieldRuntime.create(keys)))
                                 .withDefaultDataScope(DataScope.tenant("tenant_id", "tenant-a"));
    }

    private static DynamicForm protectedVersionedForm() {
        return DynamicForm.builder("customer", "customer")
                          .addField(DynamicField.of("tenant_id", "VARCHAR").withNullable(false))
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .addField(DynamicField.of("version", "INTEGER"))
                          .tenant("tenant_id", TenantStrategy.AUTO)
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                                 EncryptedSearchMode.EXACT,
                                                                                 EncryptedSearchMode.SUFFIX)
                                                                         .normalizer("digits")
                                                                         .suffixLengths(4)
                                                                         .build())
                          .build();
    }

    private static DynamicForm protectedContainsForm() {
        return DynamicForm.builder("protected-customer", "protected_customer")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                                 EncryptedSearchMode.CONTAINS)
                                                                         .normalizer("case-fold")
                                                                         .build())
                          .build();
    }

    private static DynamicForm protectedContainsVersionedForm() {
        return DynamicForm.builder("protected-customer-versioned", "protected_customer_versioned")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .addField(DynamicField.of("version", "INTEGER"))
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                                 EncryptedSearchMode.CONTAINS)
                                                                         .normalizer("case-fold")
                                                                         .build())
                          .build();
    }

    private static DynamicForm protectedContainsIdentityForm() {
        return DynamicForm.builder("protected-customer-identity", "protected_customer_identity")
                          .addField(DynamicField.primaryKey("id", "BIGINT").generatedByIdentity())
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                                 EncryptedSearchMode.CONTAINS)
                                                                         .normalizer("case-fold")
                                                                         .build())
                          .build();
    }

    private static byte[] ciphertext(FormDataSqlRenderer renderer,
                                     DynamicForm form,
                                     long id,
                                     String contact) {
        return (byte[]) renderer.protection().prepareWrite(
                form, Map.of("id", id, "contact", contact), DataScope.none()).values().get("contact");
    }

    private static List<Long> matchingIds(SyncFormClient client, DynamicForm form, String contains) {
        return client.select(QuerySpec.of(
                             form,
                             ConditionGroup.and().add(ProtectedConditions.contains("contact", contains)).build())
                                      .withProjection(List.of("id"), List.of()))
                     .stream()
                     .map(row -> ((Number) row.get("id")).longValue())
                     .toList();
    }

    private static Mono<List<Long>> matchingIds(ReactiveFormClient client,
                                                 DynamicForm form,
                                                 String contains) {
        return client.select(QuerySpec.of(
                             form,
                             ConditionGroup.and().add(ProtectedConditions.contains("contact", contains)).build())
                                      .withProjection(List.of("id"), List.of()))
                     .map(row -> ((Number) row.get("id")).longValue())
                     .collectList();
    }

    private static byte[] key(int seed) {
        byte[] key = new byte[32];
        java.util.Arrays.fill(key, (byte) seed);
        return key;
    }

    private static String receiptTableSql() {
        return """
                create table flying_orm_batch_receipt (
                    operation_id varchar(128) not null,
                    chunk_index integer not null,
                    plan_hash varchar(64) not null,
                    payload_hash varchar(64) not null,
                    row_count bigint not null,
                    affected_rows bigint not null,
                    status varchar(32) not null,
                    created_at timestamp not null default current_timestamp,
                    primary key (operation_id, chunk_index)
                )
                """;
    }

    /** 只记录 FormClient 交给执行内核的安全请求。 */
    private static final class RecordingExecutor implements ReactiveSqlExecutor {
        private SqlRequest request;
        private List<DynamicRow> rows = List.of();
        private int writeCalls;
        private BatchWriteRequest batchRequest;
        private int queryCalls;
        private final java.util.ArrayList<Object[]> batchRows = new java.util.ArrayList<>();

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            this.request = request;
            queryCalls++;
            return Flux.fromIterable(rows);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            this.request = request;
            writeCalls++;
            return Mono.just(1L);
        }

        @Override
        public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
            this.batchRequest = request;
            return Flux.from(request.rows())
                       .doOnNext(row -> batchRows.add(row.clone()))
                       .count()
                       .map(count -> BatchWriteResult.from(
                               request.options().mode(),
                               List.of(BatchChunkResult.committed(0, 0L, count.intValue(), count))));
        }
    }
}
