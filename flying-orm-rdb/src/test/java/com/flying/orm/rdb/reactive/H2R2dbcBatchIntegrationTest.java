package com.flying.orm.rdb.reactive;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.observation.BatchExecutionEventType;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用真实 H2 R2DBC 驱动验证动态表单批量写入主链路。
 *
 * @author wangr
 * @date 2026-07-23
 * @version v1.0
 */
class H2R2dbcBatchIntegrationTest {

    @Test
    void concurrentFormBatchesExposeTheWinnerAndOptimisticConflict() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("form_batch_race")
                                                                                                   .property("DB_CLOSE_DELAY", "-1")
                                                                                                   .build());
        List<BatchExecutionObservation> observations = new CopyOnWriteArrayList<>();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory)
                                                       .withBatchObserver(observations::add);
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(renderer(), RdbDialect.h2()));
        BatchOptimisticUpdate first = optimisticUpdate("Alice-2");
        BatchOptimisticUpdate second = optimisticUpdate("Alice-3");

        Mono<List<BatchWriteResult.Status>> race = executor.rowsUpdated(SqlRequest.nativeSql(
                                                                "create table Users (id varchar(32) primary key, "
                                                                        + "name varchar(64), version bigint)",
                                                                List.of()))
                                                           .then(executor.rowsUpdated(SqlRequest.nativeSql(
                                                                   "insert into Users (id, name, version) values (?, ?, ?)",
                                                                   List.of("u1", "Alice", 1L))))
                                                           .thenMany(Flux.merge(batchOutcome(client, first),
                                                                             batchOutcome(client, second)))
                                                           .collectList();

        StepVerifier.create(race)
                    .assertNext(statuses -> {
                        assertEquals(1L, statuses.stream()
                                                 .filter(BatchWriteResult.Status.COMMITTED::equals)
                                                 .count());
                        assertEquals(1L, statuses.stream()
                                                 .filter(BatchWriteResult.Status.ROLLED_BACK::equals)
                                                 .count());
                    })
                    .verifyComplete();

        StepVerifier.create(executor.query(SqlRequest.nativeSql(
                                            "select name, version from Users where id = ?",
                                            List.of("u1")))
                                    .next())
                    .assertNext(row -> {
                        assertTrue(List.of("Alice-2", "Alice-3").contains(row.get("NAME")));
                        assertEquals(2L, ((Number) row.get("VERSION")).longValue());
                    })
                    .verifyComplete();

        assertEquals(1L, observations.stream()
                                     .filter(event -> event.eventType() == BatchExecutionEventType.CHUNK)
                                     .filter(event -> event.chunkStatus() == BatchChunkResult.Status.CONFLICTED)
                                     .filter(event -> event.failureCategory() == SqlFailureCategory.OPTIMISTIC_LOCK)
                                     .count());
        assertEquals(1L, observations.stream()
                                     .filter(event -> event.eventType() == BatchExecutionEventType.SUMMARY)
                                     .filter(event -> event.summaryStatus() == BatchWriteResult.Status.ROLLED_BACK)
                                     .filter(event -> event.failureCategory() == SqlFailureCategory.OPTIMISTIC_LOCK)
                                     .count());
    }

    @Test
    void atomicExactRowCountReportsConflictAndRollsBackEveryUpdate() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("batch_lock_atomic")
                                                                                                   .property("DB_CLOSE_DELAY", "-1")
                                                                                                   .build());
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        BatchWriteRequest request = optimisticUpdateRequest(
                Flux.just(new Object[]{"Alice-2", "u1", 1L}, new Object[]{"Bob-2", "u2", 9L}),
                BatchWriteOptions.atomic(2));

        Mono<List<DynamicRow>> scenario = createVersionedUsers(executor)
                .then(executor.writeBatch(request))
                .then(Mono.<List<DynamicRow>>error(new AssertionError("batch should conflict")))
                .onErrorResume(BatchWriteException.class, error -> {
                    BatchWriteResult result = error.result();
                    assertEquals(BatchWriteResult.Status.ROLLED_BACK, result.status());
                    assertEquals(1L, result.conflictCount());
                    assertEquals(1L, result.chunks().getFirst().conflicts().getFirst().inputOffset());
                    return queryVersionedUsers(executor);
                });

        StepVerifier.create(scenario)
                    .assertNext(rows -> {
                        assertEquals("Alice", rowById(rows, "u1").get("NAME"));
                        assertEquals(1L, ((Number) rowById(rows, "u1").get("VERSION")).longValue());
                    })
                    .verifyComplete();
    }

    @Test
    void independentExactRowCountCommitsHealthyChunksAndReportsConflictChunk() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("batch_lock_independent")
                                                                                                   .property("DB_CLOSE_DELAY", "-1")
                                                                                                   .build());
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        BatchWriteRequest request = optimisticUpdateRequest(
                Flux.just(new Object[]{"Alice-2", "u1", 1L}, new Object[]{"Bob-2", "u2", 9L}),
                BatchWriteOptions.independent(1, 1));

        Mono<List<DynamicRow>> scenario = createVersionedUsers(executor)
                .then(executor.writeBatch(request))
                .flatMap(result -> {
                    assertEquals(BatchWriteResult.Status.PARTIAL, result.status());
                    assertEquals(1L, result.affectedRows());
                    assertEquals(1L, result.conflictCount());
                    assertEquals(BatchChunkResult.Status.CONFLICTED, result.chunks().get(1).status());
                    assertEquals(1L, result.chunks().get(1).conflicts().getFirst().inputOffset());
                    return queryVersionedUsers(executor);
                });

        StepVerifier.create(scenario)
                    .assertNext(rows -> {
                        assertEquals("Alice-2", rowById(rows, "u1").get("NAME"));
                        assertEquals(2L, ((Number) rowById(rows, "u1").get("VERSION")).longValue());
                        assertEquals("Bob", rowById(rows, "u2").get("NAME"));
                    })
                    .verifyComplete();
    }

    /**
     * 验证建表、批量插入和查询都经过真实 Publisher 与数据库执行。
     */
    @Test
    void insertsAndQueriesDynamicFormBatchWithRealH2Driver() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("batch_it")
                                                                                                   .property("DB_CLOSE_DELAY",
                                                                                                             "-1")
                                                                                                   .build());
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(renderer(), RdbDialect.h2()));

        Mono<List<DynamicRow>> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                                                                  "create table Users (id varchar(32) primary key, "
                                                                          + "name varchar(64), age integer)",
                                                                  List.of()))
                                                           .then(client.writeBatch(BatchSpec.insert(
                                                                                    form(),
                                                                                    Flux.fromIterable(List.of(row("id", "u1",
                                                                                                "name", "王",
                                                                                                "age", 18),
                                                                                            row("name", "李",
                                                                                                "age", null,
                                                                                                "id", "u2"))))))
                                                           .flatMap(result -> {
                                                               assertEquals(BatchWriteResult.Status.COMMITTED,
                                                                            result.status());
                                                               assertEquals(2L, result.inputCount());
                                                               assertEquals(2L, result.affectedRows());
                                                               return client.select(QuerySpec.of(
                                                                                    form(),
                                                                                    ConditionGroup.and().build()))
                                                                            .collectList();
                                                           });

        StepVerifier.create(scenario)
                    .assertNext(rows -> {
                        assertEquals(2, rows.size());
                        Map<String, Object> first = rowById(rows, "u1");
                        Map<String, Object> second = rowById(rows, "u2");
                        assertEquals("王", first.get("NAME"));
                        assertEquals(18, ((Number) first.get("AGE")).intValue());
                        assertEquals("李", second.get("NAME"));
                        assertNull(second.get("AGE"));
                    })
                    .verifyComplete();
    }

    /**
     * 验证 H2 真实执行 upsert：同一主键再次写入时更新非主键字段。
     */
    @Test
    void upsertsDynamicFormBatchWithRealH2Driver() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("batch_upsert_h2")
                                                                                                   .property("DB_CLOSE_DELAY",
                                                                                                             "-1")
                                                                                                   .build());
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(renderer(), RdbDialect.h2()));

        Mono<List<DynamicRow>> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                                                                  "create table Users (id varchar(32) primary key, "
                                                                          + "name varchar(64), age integer)",
                                                                  List.of()))
                                                           .then(client.writeBatch(BatchSpec.insert(
                                                                                    form(),
                                                                                    Flux.fromIterable(List.of(row("id", "u1",
                                                                                                "name", "王",
                                                                                                "age", 18))))))
                                                           .then(client.writeBatch(BatchSpec.upsert(
                                                                                    form(),
                                                                                    Flux.just(row("id", "u1",
                                                                                                  "name", "王二",
                                                                                                  "age", 19),
                                                                                              row("id", "u2",
                                                                                                  "name", "李",
                                                                                                  "age", 20)))))
                                                           .flatMap(result -> {
                                                               assertEquals(BatchWriteResult.Status.COMMITTED,
                                                                            result.status());
                                                               assertEquals(2L, result.inputCount());
                                                               return client.select(QuerySpec.of(
                                                                                    form(),
                                                                                    ConditionGroup.and().build()))
                                                                            .collectList();
                                                           });

        StepVerifier.create(scenario)
                    .assertNext(rows -> {
                        assertEquals(2, rows.size());
                        Map<String, Object> first = rowById(rows, "u1");
                        Map<String, Object> second = rowById(rows, "u2");
                        assertEquals("王二", first.get("NAME"));
                        assertEquals(19, ((Number) first.get("AGE")).intValue());
                        assertEquals("李", second.get("NAME"));
                        assertEquals(20, ((Number) second.get("AGE")).intValue());
                    })
                    .verifyComplete();
    }

    /**
     * 整批列值都是 null 时，也要按动态字段类型 bindNull，不能退回模糊的 Object。
     */
    @Test
    void insertsAllNullTypedColumnsWithRealH2Driver() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("batch_all_null_types")
                                                                                                   .property("DB_CLOSE_DELAY",
                                                                                                             "-1")
                                                                                                   .build());
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(renderer(), RdbDialect.h2()));

        Mono<List<DynamicRow>> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                                                                  "create table Attachments (id varchar(32) primary key, "
                                                                          + "payload blob, amount decimal(10,2), "
                                                                          + "enabled boolean, created_at timestamp)",
                                                                  List.of()))
                                                           .then(client.writeBatch(BatchSpec.insert(
                                                                                    typedNullForm(),
                                                                                    Flux.just(row("id", "a1",
                                                                                                  "payload", null,
                                                                                                  "amount", null,
                                                                                                  "enabled", null,
                                                                                                  "created_at", null),
                                                                                              row("id", "a2",
                                                                                                  "payload", null,
                                                                                                  "amount", null,
                                                                                                  "enabled", null,
                                                                                                  "created_at", null)))
                                                                                              .withOptions(BatchWriteOptions.atomic(1))))
                                                           .flatMap(result -> {
                                                               assertEquals(BatchWriteResult.Status.COMMITTED,
                                                                            result.status());
                                                               assertEquals(2L, result.inputCount());
                                                               return client.select(QuerySpec.of(
                                                                                    typedNullForm(),
                                                                                    ConditionGroup.and().build()))
                                                                            .collectList();
                                                           });

        StepVerifier.create(scenario)
                    .assertNext(rows -> {
                        assertEquals(2, rows.size());
                        Map<String, Object> first = rowById(rows, "a1");
                        assertNull(first.get("PAYLOAD"));
                        assertNull(first.get("AMOUNT"));
                        assertNull(first.get("ENABLED"));
                        assertNull(first.get("CREATED_AT"));
                    })
                    .verifyComplete();
    }

    /**
     * ATOMIC 是默认写法：所有分片都成功，才算整批成功。
     */
    @Test
    void writesAtomicBatchAndSummarizesCommittedChunks() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("batch_atomic_success")
                                                                                                   .property("DB_CLOSE_DELAY",
                                                                                                             "-1")
                                                                                                   .build());
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);

        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id, name) values (?, ?)",
                2,
                List.of(String.class, String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"u1", "Alice"}, new Object[]{"u2", "Bob"}),
                BatchWriteOptions.atomic(1));

        Mono<BatchWriteResult> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                                                       "create table Users (id varchar(32) primary key, name varchar(64))",
                                                       List.of()))
                                                  .then(executor.writeBatch(request));

        StepVerifier.create(scenario)
                    .assertNext(result -> {
                        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
                        assertEquals(2L, result.affectedRows());
                        assertEquals(2L, result.inputCount());
                        assertEquals(2, result.chunks().size());
                    })
                    .verifyComplete();
    }

    /**
     * 任一分片失败，ATOMIC 要把前面已经写进去的分片也回滚掉。
     */
    @Test
    void rollsBackAtomicBatchWhenLaterChunkFails() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("batch_atomic_rollback")
                                                                                                   .property("DB_CLOSE_DELAY",
                                                                                                             "-1")
                                                                                                   .build());
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);

        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id, name) values (?, ?)",
                2,
                List.of(String.class, String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"u1", "Alice"}, new Object[]{"u2", "Bob"}, new Object[]{"u2", "Duplicate"}),
                BatchWriteOptions.atomic(2));

        Mono<Void> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                                           "create table Users (id varchar(32) primary key, name varchar(64))",
                                           List.of()))
                                      .then(executor.writeBatch(request))
                                      .then();

        StepVerifier.create(scenario)
                    .expectErrorSatisfies(error -> {
                        BatchWriteException batchError = assertInstanceOf(BatchWriteException.class, error);
                        assertEquals(BatchWriteResult.Status.ROLLED_BACK, batchError.result().status());
                        assertEquals(0L, batchError.result().affectedRows());
                        assertEquals(3L, batchError.result().inputCount());
                    })
                    .verify();

        StepVerifier.create(executor.query(SqlRequest.nativeSql("select count(*) as total from Users", List.of()))
                                    .next())
                    .assertNext(row -> assertEquals(0L, ((Number) row.get("TOTAL")).longValue()))
                    .verifyComplete();
    }

    /**
     * ATOMIC 被全局行数限制拦停时，已经执行过的分片也必须回滚。
     */
    @Test
    void atomicBatchRollsBackWhenRowLimitIsExceeded() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("batch_atomic_limit")
                                                                                                   .property("DB_CLOSE_DELAY",
                                                                                                             "-1")
                                                                                                   .build());
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);

        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id, name) values (?, ?)",
                2,
                List.of(String.class, String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"u1", "Alice"},
                          new Object[]{"u2", "Bob"},
                          new Object[]{"u3", "Cindy"}),
                BatchWriteOptions.atomic(2).withMaxRows(2));

        Mono<Void> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                                           "create table Users (id varchar(32) primary key, name varchar(64))",
                                           List.of()))
                                      .then(executor.writeBatch(request))
                                      .then();

        StepVerifier.create(scenario)
                    .expectErrorSatisfies(error -> {
                        BatchWriteException batchError = assertInstanceOf(BatchWriteException.class, error);
                        BatchWriteResult result = batchError.result();
                        assertEquals(BatchWriteResult.Status.ROLLED_BACK, result.status());
                        assertEquals(0L, result.affectedRows());
                        assertEquals(3L, result.inputCount());
                        assertEquals(2, result.chunks().size());
                        assertEquals(BatchChunkResult.Status.ROLLED_BACK, result.chunks().get(0).status());
                        assertEquals(BatchChunkResult.Status.FAILED, result.chunks().get(1).status());
                    })
                    .verify();

        StepVerifier.create(executor.query(SqlRequest.nativeSql("select count(*) as total from Users", List.of()))
                                    .next())
                    .assertNext(row -> assertEquals(0L, ((Number) row.get("TOTAL")).longValue()))
                    .verifyComplete();
    }

    /**
     * INDEPENDENT 明确允许部分成功：失败分片回滚，后续分片继续提交。
     */
    @Test
    void independentChunksContinueAfterFailedChunk() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("batch_independent_chunks")
                                                                                                   .property("DB_CLOSE_DELAY",
                                                                                                             "-1")
                                                                                                   .build());
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);

        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id, name) values (?, ?)",
                2,
                List.of(String.class, String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"u1", "Alice"},
                          new Object[]{"u2", "Bob"},
                          new Object[]{"u2", "Duplicate"},
                          new Object[]{"u3", "Cindy"}),
                BatchWriteOptions.independent(1));

        Mono<List<BatchChunkResult>> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                                                                      "create table Users (id varchar(32) primary key, name varchar(64))",
                                                                      List.of()))
                                                                 .thenMany(executor.writeBatchChunks(request))
                                                                 .collectList();

        StepVerifier.create(scenario)
                    .assertNext(chunks -> {
                        assertEquals(4, chunks.size());
                        assertEquals(BatchChunkResult.Status.COMMITTED, chunks.get(0).status());
                        assertEquals(BatchChunkResult.Status.COMMITTED, chunks.get(1).status());
                        assertEquals(BatchChunkResult.Status.FAILED, chunks.get(2).status());
                        assertEquals(BatchChunkResult.Status.COMMITTED, chunks.get(3).status());
                    })
                    .verifyComplete();

        StepVerifier.create(executor.query(SqlRequest.nativeSql("select count(*) as total from Users", List.of()))
                                    .next())
                    .assertNext(row -> assertEquals(3L, ((Number) row.get("TOTAL")).longValue()))
                    .verifyComplete();
    }

    /**
     * INDEPENDENT 如果被全局限制拦停，也要把已经提交的分片带给上层。
     */
    @Test
    void independentBatchLimitErrorCarriesCommittedChunks() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("batch_independent_limit")
                                                                                                   .property("DB_CLOSE_DELAY",
                                                                                                             "-1")
                                                                                                   .build());
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);

        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id, name) values (?, ?)",
                2,
                List.of(String.class, String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"u1", "Alice"},
                          new Object[]{"u2", "Bob"},
                          new Object[]{"u3", "Cindy"}),
                BatchWriteOptions.independent(1).withMaxRows(2));

        Mono<Void> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                                           "create table Users (id varchar(32) primary key, name varchar(64))",
                                           List.of()))
                                      .then(executor.writeBatch(request))
                                      .then();

        StepVerifier.create(scenario)
                    .expectErrorSatisfies(error -> {
                        BatchWriteException batchError = assertInstanceOf(BatchWriteException.class, error);
                        BatchWriteResult result = batchError.result();
                        assertEquals(BatchWriteResult.Status.PARTIAL, result.status());
                        assertEquals(2L, result.affectedRows());
                        assertEquals(3L, result.inputCount());
                        assertEquals(3, result.chunks().size());
                        assertEquals(BatchChunkResult.Status.COMMITTED, result.chunks().get(0).status());
                        assertEquals(BatchChunkResult.Status.COMMITTED, result.chunks().get(1).status());
                        assertEquals(BatchChunkResult.Status.FAILED, result.chunks().get(2).status());
                    })
                    .verify();

        StepVerifier.create(executor.query(SqlRequest.nativeSql("select count(*) as total from Users", List.of()))
                                    .next())
                    .assertNext(row -> assertEquals(2L, ((Number) row.get("TOTAL")).longValue()))
                    .verifyComplete();
    }

    /** 批量输入等待由 Publisher 或上层控制，连接可用后的 SQL 兜底时间不能提前截断输入。 */
    @Test
    void atomicBatchInputWaitingIsNotCutOffBySqlFallback() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("batch_atomic_timeout")
                                                                                                   .property("DB_CLOSE_DELAY",
                                                                                                             "-1")
                                                                                                   .build());
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);

        StepVerifier.create(executor.rowsUpdated(SqlRequest.nativeSql(
                            "create table Users (id varchar(32) primary key, name varchar(64))", List.of())))
                    .expectNext(0L)
                    .verifyComplete();

        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id, name) values (?, ?)",
                2,
                List.of(String.class, String.class),
                SqlBindMarkerStyle.CANONICAL,
                Mono.delay(Duration.ofMillis(450)).map(ignored -> new Object[]{"u1", "Alice"}),
                BatchWriteOptions.atomic(2).withTimeout(Duration.ofMillis(300)));

        StepVerifier.create(executor.writeBatch(request))
                    .assertNext(result -> {
                        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
                        assertEquals(1L, result.inputCount());
                        assertEquals(1L, result.affectedRows());
                    })
                    .verifyComplete();
    }

    /**
     * 回执模式下，提交已经到达数据库但原连接确认丢失时，确认窗口内应主动查证并安全重放。
     */
    @Test
    void receiptModeConfirmsUnknownCommitAndReplaysWithoutDuplicates() {
        H2ConnectionFactory rawFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                           .inMemory("batch_receipt_replay")
                                                                                           .property("DB_CLOSE_DELAY",
                                                                                                     "-1")
                                                                                           .build());
        ReactiveSqlExecutor flakyExecutor = R2dbcSqlExecutor.create(failFirstCommit(rawFactory));
        ReactiveSqlExecutor stableExecutor = R2dbcSqlExecutor.create(rawFactory);
        Mono<BatchWriteResult> scenario = flakyExecutor.rowsUpdated(SqlRequest.nativeSql(
                                                       "create table Users (id varchar(32) primary key, name varchar(64))",
                                                       List.of()))
                                                  .then(flakyExecutor.rowsUpdated(SqlRequest.nativeSql(
                                                          receiptTableSql(),
                                                          List.of())))
                                                  .then(flakyExecutor.writeBatch(receiptRequest()));

        StepVerifier.create(scenario)
                    .assertNext(result -> {
                        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
                        assertEquals(2L, result.inputCount());
                        assertEquals(2L, result.affectedRows());
                        assertEquals(BatchChunkResult.Status.COMMITTED, result.chunks().getFirst().status());
                    })
                    .verifyComplete();

        StepVerifier.create(stableExecutor.writeBatch(receiptRequest()))
                    .assertNext(result -> {
                        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
                        assertEquals(2L, result.inputCount());
                        assertEquals(2L, result.affectedRows());
                    })
                    .verifyComplete();

        StepVerifier.create(stableExecutor.query(SqlRequest.nativeSql("select count(*) as total from Users", List.of()))
                                          .next())
                    .assertNext(row -> assertEquals(2L, ((Number) row.get("TOTAL")).longValue()))
                    .verifyComplete();
    }

    /**
     * 独立分片的提交确认丢失时，每片都应在自己的回执证据确认后报告 COMMITTED。
     */
    @Test
    void independentReceiptModeConfirmsUnknownChunk() {
        H2ConnectionFactory rawFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                           .inMemory("batch_independent_receipt")
                                                                                           .property("DB_CLOSE_DELAY",
                                                                                                     "-1")
                                                                                           .build());
        ReactiveSqlExecutor flakyExecutor = R2dbcSqlExecutor.create(failFirstCommit(rawFactory));
        ReactiveSqlExecutor stableExecutor = R2dbcSqlExecutor.create(rawFactory);
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id, name) values (?, ?)",
                2,
                List.of(String.class, String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"u1", "Alice"}, new Object[]{"u2", "Bob"}),
                BatchWriteOptions.independent(1).withReceipt("independent-receipt-operation"));

        Mono<List<BatchChunkResult>> scenario = flakyExecutor.rowsUpdated(SqlRequest.nativeSql(
                                                                  "create table Users (id varchar(32) primary key, name varchar(64))",
                                                                  List.of()))
                                                             .then(flakyExecutor.rowsUpdated(SqlRequest.nativeSql(
                                                                     receiptTableSql(),
                                                                     List.of())))
                                                             .thenMany(flakyExecutor.writeBatchChunks(request))
                                                             .collectList();

        StepVerifier.create(scenario)
                    .assertNext(chunks -> {
                        assertEquals(2, chunks.size());
                        assertEquals(BatchChunkResult.Status.COMMITTED, chunks.get(0).status());
                        assertEquals(BatchChunkResult.Status.COMMITTED, chunks.get(1).status());
                        assertEquals(1, chunks.get(0).inputCount());
                        assertEquals(1, chunks.get(1).inputCount());
                    })
                    .verifyComplete();

        StepVerifier.create(stableExecutor.query(SqlRequest.nativeSql("select count(*) as total from Users", List.of()))
                                          .next())
                    .assertNext(row -> assertEquals(2L, ((Number) row.get("TOTAL")).longValue()))
                    .verifyComplete();
    }

    /**
     * INDEPENDENT 汇总入口要正常返回 PARTIAL，方便上层不用订阅分片流也能处理业务结果。
     */
    @Test
    void independentBatchSummaryReturnsPartialResult() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                                                                                                   .inMemory("batch_independent_summary")
                                                                                                   .property("DB_CLOSE_DELAY",
                                                                                                             "-1")
                                                                                                   .build());
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);

        BatchWriteRequest request = new BatchWriteRequest(
                "insert into Users (id, name) values (?, ?)",
                2,
                List.of(String.class, String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"u1", "Alice"}, new Object[]{"u1", "Duplicate"}),
                BatchWriteOptions.independent(1));

        Mono<BatchWriteResult> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                                                       "create table Users (id varchar(32) primary key, name varchar(64))",
                                                       List.of()))
                                                  .then(executor.writeBatch(request));

        StepVerifier.create(scenario)
                    .assertNext(result -> {
                        assertEquals(BatchWriteResult.Status.PARTIAL, result.status());
                        assertEquals(1L, result.affectedRows());
                        assertEquals(2L, result.inputCount());
                    })
                    .verifyComplete();
    }

    private static SqlRenderer renderer() {
        return SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build();
    }

    private static DynamicForm form() {
        return DynamicForm.builder("users", "Users")
                          .addField(DynamicField.primaryKey("id", "VARCHAR"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("age", "INTEGER"))
                          .build();
    }

    private static DynamicForm versionedForm() {
        return DynamicForm.builder("users", "Users")
                          .addField(DynamicField.primaryKey("id", "VARCHAR"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("version", "BIGINT"))
                          .build();
    }

    private static BatchOptimisticUpdate optimisticUpdate(String name) {
        return new BatchOptimisticUpdate(row("name", name),
                                         ConditionGroup.and().where("id", "=", "u1").build(),
                                         OptimisticLockOptions.increment("version", 1L));
    }

    private static Mono<BatchWriteResult.Status> batchOutcome(ReactiveFormClient client,
                                                              BatchOptimisticUpdate update) {
        return client.writeBatch(BatchSpec.update(versionedForm(), Flux.just(update)))
                     .map(BatchWriteResult::status)
                     .onErrorResume(BatchWriteException.class, error -> Mono.just(error.result().status()));
    }

    private static DynamicForm typedNullForm() {
        return DynamicForm.builder("attachments", "Attachments")
                          .addField(DynamicField.primaryKey("id", "VARCHAR"))
                          .addField(DynamicField.of("payload", "BLOB"))
                          .addField(DynamicField.of("amount", "DECIMAL"))
                          .addField(DynamicField.of("enabled", "BOOLEAN"))
                          .addField(DynamicField.of("created_at", "TIMESTAMP"))
                          .build();
    }

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            values.put((String) pairs[i], pairs[i + 1]);
        }
        return values;
    }

    private static Map<String, Object> rowById(List<? extends Map<String, Object>> rows, String id) {
        return rows.stream()
                   .filter(row -> id.equals(row.get("ID")))
                   .findFirst()
                   .orElseThrow();
    }

    private static BatchWriteRequest receiptRequest() {
        return new BatchWriteRequest(
                "insert into Users (id, name) values (?, ?)",
                2,
                List.of(String.class, String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"u1", "Alice"}, new Object[]{"u2", "Bob"}),
                BatchWriteOptions.atomic(2).withReceipt("receipt-replay-operation"));
    }

    private static BatchWriteRequest optimisticUpdateRequest(Publisher<Object[]> rows, BatchWriteOptions options) {
        return new BatchWriteRequest(
                "update Users set name = ?, version = version + 1 where id = ? and version = ?",
                3,
                List.of(String.class, String.class, Long.class),
                SqlBindMarkerStyle.CANONICAL,
                rows,
                options,
                BatchRowCountPolicy.EXACTLY_ONE);
    }

    private static Mono<Long> createVersionedUsers(ReactiveSqlExecutor executor) {
        return executor.rowsUpdated(SqlRequest.nativeSql(
                               "create table Users (id varchar(32) primary key, name varchar(64), version bigint)",
                               List.of()))
                       .then(executor.rowsUpdated(SqlRequest.nativeSql(
                               "insert into Users(id, name, version) values ('u1', 'Alice', 1), ('u2', 'Bob', 1)",
                               List.of())));
    }

    private static Mono<List<DynamicRow>> queryVersionedUsers(ReactiveSqlExecutor executor) {
        return executor.query(SqlRequest.nativeSql("select id, name, version from Users order by id", List.of()))
                       .collectList();
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

    private static ConnectionFactory failFirstCommit(ConnectionFactory delegate) {
        AtomicBoolean failed = new AtomicBoolean();
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.from(delegate.create()).map(connection -> proxy(Connection.class, (ignored, method, args) -> {
                    if ("commitTransaction".equals(method.getName()) && failed.compareAndSet(false, true)) {
                        return Mono.from(connection.commitTransaction())
                                   .then(Mono.error(new IllegalStateException("lost commit acknowledgement")));
                    }
                    return invoke(connection, method, args);
                }));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return delegate.getMetadata();
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException error) {
            throw error.getCause();
        }
    }
}
