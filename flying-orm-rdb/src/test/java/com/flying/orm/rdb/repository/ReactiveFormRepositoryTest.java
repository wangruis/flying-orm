package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lifecycle.CommittedEntityLifecycleException;
import com.flying.orm.rdb.mapping.FlyingLogicDelete;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 覆盖实体 Repository 的映射、逻辑删除、乐观锁、scope 和批量写入组合契约。 */
class ReactiveFormRepositoryTest {

    @Test
    void writesEntityValuesThroughFormValueCodecs() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<UserEntity> repository = ReactiveFormRepository.create(client,
                                                                                      form(),
                                                                                      UserEntity.class);

        StepVerifier.create(repository.insert(new UserEntity(1L, Status.ACTIVE)))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("ACTIVE", executor.request().parameters().get(statusParameterIndex(executor.request().sql())));
        assertFalse(executor.request().parameters().contains(Status.ACTIVE));
    }

    @Test
    void repositoryWritesJpaColumnNames() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<AnnotatedUserEntity> repository = ReactiveFormRepository.create(client,
                                                                                               annotatedForm(),
                                                                                               AnnotatedUserEntity.class);

        StepVerifier.create(repository.insert(new AnnotatedUserEntity(1L, "Alice")))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("insert into Users (id, user_name) values (?, ?)", executor.request().sql());
        assertEquals(List.of(1L, "Alice"), executor.request().parameters());
    }

    @Test
    void repositoryAppliesColumnWriteRulesToInsertAndUpdate() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormRepository<WritableRulesEntity> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()),
                writableRulesForm(),
                WritableRulesEntity.class);
        WritableRulesEntity entity = new WritableRulesEntity(1L, "Alice", "ALICE");

        StepVerifier.create(repository.insert(entity)).expectNext(1L).verifyComplete();
        assertEquals("insert into Users (id, name) values (?, ?)", executor.request().sql());

        StepVerifier.create(repository.update(entity,
                                              ConditionGroup.and().where("id", "=", 1L).build()))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("update Users set name = ? where id = ?", executor.request().sql());
        assertEquals(List.of("Alice", 1L), executor.request().parameters());
    }

    @Test
    void insertRunsConfiguredLifecycleAfterSubscription() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        List<EntityLifecyclePhase> phases = new ArrayList<>();
        ReactiveFormRepository<LifecycleEntity> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()),
                writableRulesForm(),
                LifecycleEntity.class)
                .withListener(event -> {
                    phases.add(event.phase());
                    if (event.phase() == EntityLifecyclePhase.PRE_PERSIST) {
                        event.entity().prePersistCalls++;
                        event.entity().name = event.entity().name.trim();
                    } else if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        event.entity().postPersistCalls++;
                    }
                    return Mono.empty();
                });
        LifecycleEntity entity = new LifecycleEntity(1L, "  Alice  ");

        Mono<Long> insertion = repository.insert(entity);
        assertEquals(0, entity.prePersistCalls);
        assertEquals(List.of(), phases);

        StepVerifier.create(insertion).expectNext(1L).verifyComplete();

        assertEquals(1, entity.prePersistCalls);
        assertEquals(1, entity.postPersistCalls);
        assertEquals(List.of(EntityLifecyclePhase.PRE_PERSIST, EntityLifecyclePhase.POST_PERSIST), phases);
        assertEquals(List.of(1L, "Alice"), executor.request().parameters());
    }

    @Test
    void updateLifecycleFailureStopsBeforeSqlExecution() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        LifecycleEntity entity = new LifecycleEntity(1L, "Alice");
        ReactiveFormRepository<LifecycleEntity> repository = ReactiveFormRepository.create(
                        ReactiveFormClient.create(executor, renderer()),
                writableRulesForm(),
                LifecycleEntity.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.PRE_UPDATE) {
                        event.entity().preUpdateCalls++;
                        return Mono.error(new IllegalStateException("update rejected"));
                    }
                    event.entity().postUpdateCalls++;
                    return Mono.empty();
                });

        StepVerifier.create(repository.update(entity, ConditionGroup.and().where("id", "=", 1L).build()))
                    .expectErrorMessage("update rejected")
                    .verify();

        assertEquals(1, entity.preUpdateCalls);
        assertEquals(0, entity.postUpdateCalls);
        assertNull(executor.request());
    }

    @Test
    void postWriteFailureExplicitlyReportsThatTheDatabaseChangeCommitted() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        LifecycleEntity entity = new LifecycleEntity(1L, "Alice");
        ReactiveFormRepository<LifecycleEntity> repository = ReactiveFormRepository.create(
                        ReactiveFormClient.create(executor, renderer()), writableRulesForm(), LifecycleEntity.class)
                .withListener(event -> event.phase() == EntityLifecyclePhase.POST_PERSIST
                        ? Mono.error(new IllegalStateException("audit sink unavailable"))
                        : Mono.empty());

        StepVerifier.create(repository.insert(entity))
                    .expectErrorSatisfies(error -> {
                        CommittedEntityLifecycleException committed =
                                (CommittedEntityLifecycleException) error;
                        assertTrue(committed.committed());
                        assertEquals(EntityLifecyclePhase.POST_PERSIST, committed.phase());
                        assertEquals(1L, committed.result());
                        assertEquals("audit sink unavailable", committed.getCause().getMessage());
                    })
                    .verify();

        assertNotNull(executor.request());
    }

    /** POST 回调发生 VME 时，已提交状态不改变，但致命错误不能被已提交说明包装。 */
    @Test
    void postWriteVirtualMachineErrorPropagatesWithoutCommittedLifecycleWrapper() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        OutOfMemoryError failure = new OutOfMemoryError("post-persist callback failure");
        ReactiveFormRepository<LifecycleEntity> repository = ReactiveFormRepository.create(
                        ReactiveFormClient.create(executor, renderer()), writableRulesForm(), LifecycleEntity.class)
                .withListener(event -> event.phase() == EntityLifecyclePhase.POST_PERSIST
                        ? Mono.error(failure)
                        : Mono.empty());

        StepVerifier.create(repository.insert(new LifecycleEntity(1L, "Alice")))
                    .expectErrorSatisfies(error -> assertSame(failure, error))
                    .verify();

        assertNotNull(executor.request());
    }

    @Test
    void selectCompletesPostLoadBeforePublishingEntity() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.queryRows = List.of(Map.of("id", 1L, "name", "Alice"));
        ReactiveFormRepository<LifecycleEntity> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()),
                writableRulesForm(),
                LifecycleEntity.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_LOAD) {
                        event.entity().postLoadCalls++;
                    }
                    return Mono.empty();
                });

        StepVerifier.create(repository.select(ConditionGroup.and().build()))
                    .assertNext(entity -> assertEquals(1, entity.postLoadCalls))
                    .verifyComplete();
    }


    @Test
    void syncRepositoryOwnsNoReactiveRepositoryDelegate() {
        boolean holdsReactiveDelegate = java.util.Arrays.stream(SyncFormRepository.class.getDeclaredFields())
                                                         .anyMatch(field -> field.getType()
                                                                                 == ReactiveFormRepository.class);

        assertFalse(holdsReactiveDelegate);
    }


    /** 同步 Repository 的监听器也必须受客户端等待上限保护，不能因为回调不结束而永久占住调用线程。 */

    @Test
    void batchLifecycleOnlyPublishesAfterForCommittedChunks() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        List<EntityLifecyclePhase> phases = new ArrayList<>();
        ReactiveFormRepository<LifecycleEntity> repository = ReactiveFormRepository.create(
                        ReactiveFormClient.create(executor, renderer()),
                        writableRulesForm(),
                        LifecycleEntity.class)
                .withListener(event -> {
                    phases.add(event.phase());
                    return Mono.empty();
                });

        StepVerifier.create(repository.insertBatch(List.of(
                            new LifecycleEntity(1L, "Alice"),
                            new LifecycleEntity(2L, "Bob"))))
                    .expectNextMatches(result -> result.status() == BatchWriteResult.Status.COMMITTED)
                    .verifyComplete();

        assertEquals(2, phases.stream().filter(phase -> phase == EntityLifecyclePhase.PRE_PERSIST).count());
        assertEquals(2, phases.stream().filter(phase -> phase == EntityLifecyclePhase.POST_PERSIST).count());

        phases.clear();
        executor.forcedBatchResult = BatchWriteResult.from(
                BatchWriteOptions.Mode.INDEPENDENT,
                List.of(BatchChunkResult.unknown(0, 0, 2, new IllegalStateException("commit result lost"))));

        StepVerifier.create(repository.insertBatch(List.of(
                            new LifecycleEntity(3L, "Carol"),
                            new LifecycleEntity(4L, "David"))))
                    .expectNextMatches(result -> result.status() == BatchWriteResult.Status.UNKNOWN)
                    .verifyComplete();

        assertEquals(2, phases.stream().filter(phase -> phase == EntityLifecyclePhase.PRE_PERSIST).count());
        assertEquals(0, phases.stream().filter(phase -> phase == EntityLifecyclePhase.POST_PERSIST).count());

        phases.clear();
        executor.forcedBatchResult = BatchWriteResult.from(
                BatchWriteOptions.Mode.ATOMIC,
                List.of(BatchChunkResult.committed(0, 0, 1, 1),
                        BatchChunkResult.failed(1, 1, 1, new IllegalStateException("second chunk failed"))));

        StepVerifier.create(repository.insertBatch(List.of(
                            new LifecycleEntity(5L, "Eve"),
                            new LifecycleEntity(6L, "Frank"))))
                    .expectNextMatches(result -> result.status() == BatchWriteResult.Status.ROLLED_BACK)
                    .verifyComplete();

        assertEquals(0, phases.stream().filter(phase -> phase == EntityLifecyclePhase.POST_PERSIST).count());
    }

    /** 外部 ATOMIC 返回 ENLISTED 时保留有界实体引用，只有外层真正提交后才执行 POST 生命周期。 */
    @Test
    void batchLifecycleWaitsForExternalTransactionCompletion() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        List<EntityLifecyclePhase> phases = new ArrayList<>();
        ReactiveFormRepository<LifecycleEntity> repository = ReactiveFormRepository.create(
                        ReactiveFormClient.create(executor, renderer()),
                        writableRulesForm(),
                        LifecycleEntity.class)
                .withListener(event -> {
                    phases.add(event.phase());
                    return Mono.empty();
                });
        BatchChunkResult enlisted = new BatchChunkResult(0, 0, 2, 0,
                                                         BatchChunkResult.Status.ENLISTED,
                                                         null, null, List.of());
        executor.forcedBatchResult = BatchWriteResult.from(
                BatchWriteOptions.Mode.ATOMIC, List.of(enlisted));

        StepVerifier.create(repository.insertBatch(List.of(
                            new LifecycleEntity(1L, "Alice"),
                            new LifecycleEntity(2L, "Bob"))))
                    .expectNextMatches(result -> result.status() == BatchWriteResult.Status.ENLISTED)
                    .verifyComplete();

        assertEquals(2, phases.stream().filter(phase -> phase == EntityLifecyclePhase.PRE_PERSIST).count());
        assertEquals(0, phases.stream().filter(phase -> phase == EntityLifecyclePhase.POST_PERSIST).count());

        BatchWriteResult committed = BatchWriteResult.from(
                BatchWriteOptions.Mode.ATOMIC, List.of(BatchChunkResult.committed(0, 0, 2, 2)));
        StepVerifier.create(Mono.from(executor.writeRequest().completion().afterCompletion(committed)))
                    .verifyComplete();
        assertEquals(2, phases.stream().filter(phase -> phase == EntityLifecyclePhase.POST_PERSIST).count());

        phases.clear();
        StepVerifier.create(repository.insertBatch(List.of(
                            new LifecycleEntity(3L, "Carol"),
                            new LifecycleEntity(4L, "David"))))
                    .expectNextMatches(result -> result.status() == BatchWriteResult.Status.ENLISTED)
                    .verifyComplete();
        BatchWriteResult rolledBack = BatchWriteResult.from(
                BatchWriteOptions.Mode.ATOMIC, List.of(BatchChunkResult.rolledBack(0, 0, 2)));
        StepVerifier.create(Mono.from(executor.writeRequest().completion().afterCompletion(rolledBack)))
                    .verifyComplete();
        assertEquals(0, phases.stream().filter(phase -> phase == EntityLifecyclePhase.POST_PERSIST).count());
    }

    @Test
    void batchLifecycleRetentionRespectsTheBatchMemoryBudget() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        BatchWriteOptions options = BatchWriteOptions.atomic(10).withMemoryLimits(10, 128, 10);
        ReactiveFormRepository<LifecycleEntity> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()).withDefaultBatchWriteOptions(options),
                writableRulesForm(),
                LifecycleEntity.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.PRE_PERSIST) {
                        event.entity().prePersistCalls++;
                    } else if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        event.entity().postPersistCalls++;
                    }
                    return Mono.empty();
                });
        LifecycleEntity entity = new LifecycleEntity(1L, "x".repeat(1_024));

        StepVerifier.create(repository.insertBatch(List.of(entity)))
                    .expectErrorSatisfies(error -> {
                        assertTrue(error instanceof BatchMemoryLimitExceededException);
                        assertTrue(error.getMessage().contains("lifecycleRetainedBytes"));
                    })
                    .verify();

        assertNull(executor.writeRequest());
        assertEquals(1, entity.prePersistCalls);
        assertEquals(0, entity.postPersistCalls);
    }

    @Test
    void batchLifecycleAndMappedRowsShareOneMemoryBudget() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        BatchWriteOptions options = BatchWriteOptions.atomic(10).withMemoryLimits(10, 200, 10);
        ReactiveFormRepository<LifecycleEntity> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()).withDefaultBatchWriteOptions(options),
                writableRulesForm(),
                LifecycleEntity.class)
                .withListener(event -> Mono.empty());

        StepVerifier.create(repository.insertBatch(List.of(new LifecycleEntity(1L, "x"))))
                    .expectErrorSatisfies(error -> {
                        assertTrue(error instanceof BatchMemoryLimitExceededException);
                        assertTrue(error.getMessage().contains("combinedRetainedBytes"));
                    })
                    .verify();

        assertNull(executor.writeRequest());
    }

    @Test
    void passesExecutionOptionsThroughReactiveRepositoryReadsAndWrites() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<UserEntity> repository = ReactiveFormRepository.create(client,
                                                                                      form(),
                                                                                      UserEntity.class);
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(20).withTimeout(Duration.ofSeconds(2));
        ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();

        StepVerifier.create(repository.select(where, options))
                    .verifyComplete();
        assertEquals(options, executor.options());

        StepVerifier.create(repository.update(new UserEntity(1L, Status.ACTIVE), where, options))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals(options, executor.options());
    }

    @Test
    void reactiveRepositoryPassesOptimisticLockToFormClient() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<UserEntity> repository = ReactiveFormRepository.create(client,
                                                                                      versionedForm(),
                                                                                      UserEntity.class);
        ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();

        StepVerifier.create(repository.update(new UserEntity(1L, Status.ACTIVE),
                                              where,
                                              OptimisticLockOptions.increment("version", 3)))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals(true, executor.request().sql().startsWith("update Users set "));
        assertEquals(true, executor.request().sql().contains("version = version + 1 where id = ? and version = ?"));
        assertEquals(3, executor.request().parameters().getLast());
    }

    @Test
    void reactiveRepositoryUsesVersionAnnotationAsOptimisticLock() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<VersionedUserEntity> repository = ReactiveFormRepository.create(client,
                                                                                               versionedForm(),
                                                                                               VersionedUserEntity.class);
        ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();

        StepVerifier.create(repository.update(new VersionedUserEntity(1L, Status.ACTIVE, 3), where))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("update Users set status = ?, version = version + 1 where id = ? and version = ?",
                     executor.request().sql());
        assertEquals(List.of("ACTIVE", 1L, 3), executor.request().parameters());
    }

    @Test
    void repositoryBatchesEntityUpdatesByIdAndVersion() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormRepository<VersionedUserEntity> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()),
                versionedForm(),
                VersionedUserEntity.class);

        StepVerifier.create(repository.updateBatch(List.of(
                            new VersionedUserEntity(1L, Status.ACTIVE, 3),
                            new VersionedUserEntity(2L, Status.ACTIVE, 4))))
                    .assertNext(result -> assertEquals(BatchWriteResult.Status.COMMITTED, result.status()))
                    .verifyComplete();

        assertEquals(BatchRowCountPolicy.EXACTLY_ONE, executor.writeRequest().rowCountPolicy());
        assertEquals("update Users set status = ?, version = version + 1 where id = ? and version = ?",
                     executor.writeRequest().sql());
        assertEquals(List.of("ACTIVE", 1L, 3), List.of(executor.parameterRows().get(0)));
        assertEquals(List.of("ACTIVE", 2L, 4), List.of(executor.parameterRows().get(1)));
    }


    @Test
    void repositorySelectAddsLogicDeleteFilter() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<LogicDeletedUserEntity> repository = ReactiveFormRepository.create(client,
                                                                                                  logicDeletedForm(),
                                                                                                  LogicDeletedUserEntity.class);
        ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();

        StepVerifier.create(repository.select(where))
                    .verifyComplete();

        assertEquals("select id, status, deleted from Users where id = ? and deleted = ?", executor.request().sql());
        assertEquals(List.of(1L, 0), executor.request().parameters());
    }

    @Test
    void repositorySelectCanApplyServerDataScope() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<TenantUserEntity> repository = ReactiveFormRepository.create(client,
                                                                                            tenantForm(),
                                                                                            TenantUserEntity.class);
        ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();

        StepVerifier.create(repository.select(where, DataScope.tenant("tenant_id", "t1")))
                    .verifyComplete();

        assertEquals("select id, tenant_id, status from Users where id = ? and tenant_id = ?",
                     executor.request().sql());
        assertEquals(List.of(1L, "t1"), executor.request().parameters());
    }

    @Test
    void repositoryTypedSelectKeepsExplicitFieldScope() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormRepository<UserEntity> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()),
                form(),
                UserEntity.class);
        DataScope scope = DataScope.none().withFields(FieldScope.readable("id"));

        StepVerifier.create(repository.select(ConditionGroup.and().build(), scope))
                    .verifyComplete();

        assertEquals("select id from Users", executor.request().sql());
    }

    @Test
    void repositoryUpdateKeepsExplicitWriteFieldScope() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormRepository<UserEntity> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()),
                form(),
                UserEntity.class);
        DataScope scope = DataScope.none().withFields(new FieldScope(java.util.Set.of("id", "status"),
                                                                     java.util.Set.of("id")));

        assertThrows(IllegalArgumentException.class,
                     () -> repository.update(new UserEntity(1L, Status.ACTIVE),
                                             ConditionGroup.and().where("id", "=", 1L).build(),
                                             scope)
                                     .block());
    }

    @Test
    void repositoryDeleteUsesLogicDeleteMarkerWhenConfigured() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<LogicDeletedUserEntity> repository = ReactiveFormRepository.create(client,
                                                                                                  logicDeletedForm(),
                                                                                                  LogicDeletedUserEntity.class);
        ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();

        StepVerifier.create(repository.delete(where))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("update Users set deleted = ? where id = ? and deleted = ?", executor.request().sql());
        assertEquals(List.of(1, 1L, 0), executor.request().parameters());
    }

    @Test
    void repositoryCanStillUsePhysicalDeleteExplicitly() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<LogicDeletedUserEntity> repository = ReactiveFormRepository.create(client,
                                                                                                  logicDeletedForm(),
                                                                                                  LogicDeletedUserEntity.class);
        ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();

        StepVerifier.create(repository.physicalDelete(where))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("delete from Users where id = ?", executor.request().sql());
        assertEquals(List.of(1L), executor.request().parameters());
    }

    @Test
    void repositoryScopedPhysicalDeleteBypassesDeclaredLogicDelete() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormRepository<TenantUserEntity> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()),
                tenantLogicDeletedForm(),
                TenantUserEntity.class);
        ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();

        StepVerifier.create(repository.physicalDelete(where, DataScope.tenant("tenant_id", "t1")))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("delete from Users where id = ? and tenant_id = ?", executor.request().sql());
        assertEquals(List.of(1L, "t1"), executor.request().parameters());
    }




    @Test
    void reactiveRepositoryUsesClientDefaultExecutionOptions() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(12).withTimeout(Duration.ofSeconds(2));
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                      .withDefaultExecutionOptions(options);
        ReactiveFormRepository<UserEntity> repository = ReactiveFormRepository.create(client,
                                                                                      form(),
                                                                                      UserEntity.class);
        ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();

        StepVerifier.create(repository.select(where))
                    .verifyComplete();

        assertEquals(options, executor.options());
    }



    /** Repository 带 Scope 的删除便捷入口不能用 unlimited 覆盖 FormClient 的统一保护。 */
    @Test
    void scopedRepositoryDeletesKeepClientDefaultExecutionOptions() {
        SqlExecutionOptions defaults = SqlExecutionOptions.maxRows(1).withTimeout(Duration.ofSeconds(2));
        ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();
        VersionedUserEntity entity = new VersionedUserEntity(1L, Status.ACTIVE, 3);

        RecordingSqlExecutor reactiveExecutor = new RecordingSqlExecutor();
        ReactiveFormClient reactiveClient = ReactiveFormClient.create(reactiveExecutor, renderer())
                                                              .withDefaultExecutionOptions(defaults);
        ReactiveFormRepository<VersionedUserEntity> reactiveRepository = ReactiveFormRepository.create(
                reactiveClient, versionedForm(), VersionedUserEntity.class);
        StepVerifier.create(reactiveRepository.delete(entity, where, DataScope.none()))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals(defaults, reactiveExecutor.options());

    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder()
                                                     .addTerm(SqlTermHandler.equalsTo())
                                                     .build(),
                                          RdbDialect.h2());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("status", "VARCHAR"))
                          .build();
    }

    private static DynamicForm versionedForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("status", "VARCHAR"))
                          .addField(DynamicField.of("version", "INTEGER"))
                          .build();
    }

    private static DynamicForm logicDeletedForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("status", "VARCHAR"))
                          .addField(DynamicField.of("deleted", "INTEGER"))
                          .build();
    }

    private static DynamicForm tenantForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("tenant_id", "VARCHAR"))
                          .addField(DynamicField.of("status", "VARCHAR"))
                          .tenant("tenant_id", TenantStrategy.AUTO)
                          .build();
    }

    private static DynamicForm tenantLogicDeletedForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("tenant_id", "VARCHAR"))
                          .addField(DynamicField.of("status", "VARCHAR"))
                          .addField(DynamicField.of("deleted", "INTEGER"))
                          .tenant("tenant_id", TenantStrategy.AUTO)
                          .logicDelete("deleted", 0, 1)
                          .build();
    }

    private static DynamicForm annotatedForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("user_name", "VARCHAR"))
                          .build();
    }

    private static DynamicForm writableRulesForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("computed_label", "VARCHAR"))
                          .build();
    }

    private static int statusParameterIndex(String sql) {
        String columns = sql.substring(sql.indexOf('(') + 1, sql.indexOf(')'));
        return List.of(columns.split(", ")).indexOf("status");
    }

    private enum Status {
        ACTIVE
    }

    private record UserEntity(Long id, Status status) {
    }

    private record AnnotatedUserEntity(Long id, @TableField("user_name") String name) {
    }

    private record WritableRulesEntity(@TableId Long id,
                                       String name,
                                       @TableField(exist = false)
                                       String computedLabel) {
    }

    private static final class LifecycleEntity {

        @TableId
        private Long id;

        private String name;

        private transient int prePersistCalls;

        private transient int postPersistCalls;

        private transient int preUpdateCalls;

        private transient int postUpdateCalls;

        private transient int postLoadCalls;

        private LifecycleEntity() {
        }

        private LifecycleEntity(Long id, String name) {
            this.id = id;
            this.name = name;
        }

    }

    private record VersionedUserEntity(@TableId Long id, Status status, @Version Integer version) {
    }

    private record LogicDeletedUserEntity(Long id,
                                          Status status,
                                          @FlyingLogicDelete(notDeletedValue = "0", deletedValue = "1")
                                          Integer deleted) {
    }

    private record TenantUserEntity(Long id, String tenantId, Status status) {
    }

    private static final class RecordingSqlExecutor implements ReactiveSqlExecutor {

        private SqlRequest request;

        private SqlExecutionOptions options;

        private BatchWriteRequest writeRequest;

        private final List<Object[]> parameterRows = new ArrayList<>();

        private List<Map<String, Object>> queryRows = List.of();

        private BatchWriteResult forcedBatchResult;

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            this.request = request;
            return Flux.fromIterable(queryRows).map(DynamicRow::copyOf);
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
            this.options = options;
            return query(request);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            this.request = request;
            return Mono.just(1L);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
            this.options = options;
            return rowsUpdated(request);
        }

        @Override
        public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
            this.writeRequest = request;
            return Flux.from(request.rows())
                       .doOnNext(parameterRows::add)
                       .count()
                       .map(count -> forcedBatchResult != null
                               ? forcedBatchResult
                               : BatchWriteResult.from(
                                       request.options().mode(),
                                       List.of(BatchChunkResult.committed(0, 0, count.intValue(), count))));
        }

        private SqlRequest request() {
            return request;
        }

        private SqlExecutionOptions options() {
            return options;
        }

        private BatchWriteRequest writeRequest() {
            return writeRequest;
        }

        private List<Object[]> parameterRows() {
            return parameterRows;
        }
    }
}
