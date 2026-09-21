package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityDefaultProjectionContractTest {

    @Test
    void syncEntityTerminalsHonorDefaultProjection() {
        Fixture fixture = new Fixture();
        assertSyncTerminals(fixture, () -> fixture.sync.entity(Account.class).query());
    }

    @Test
    void reactiveEntityTerminalsHonorDefaultProjection() {
        Fixture fixture = new Fixture();
        assertReactiveTerminals(fixture, () -> fixture.reactive.entity(Account.class).query());
    }

    @Test
    void syncRepositoryCreateQueryHonorsDefaultProjection() {
        Fixture fixture = new Fixture();
        SyncFormRepository<Account> repository = SyncFormRepository.create(
                fixture.sync, fixture.sync.entityModels().metadata(Account.class).toDynamicForm(), Account.class);
        assertSyncTerminals(fixture, repository::createQuery);
    }

    @Test
    void reactiveRepositoryCreateQueryHonorsDefaultProjection() {
        Fixture fixture = new Fixture();
        ReactiveFormRepository<Account> repository = ReactiveFormRepository.create(
                fixture.reactive, fixture.reactive.entityModels().metadata(Account.class).toDynamicForm(), Account.class);
        assertReactiveTerminals(fixture, repository::createQuery);
    }

    @Test
    void entityDefaultProjectionIntersectsReadableFieldScope() {
        Fixture fixture = new Fixture();
        DataScope idOnly = DataScope.none().withFields(FieldScope.readable("id"));

        Account sync = fixture.sync.entity(Account.class).query().scope(idOnly).one();
        assertEquals(7L, sync.id);
        assertNull(sync.name);
        assertEquals("select \"id\" from \"accounts\"", fixture.dataRequest.sql());

        Account reactive = fixture.reactive.entity(Account.class).query().scope(idOnly).one().block();
        assertEquals(7L, reactive.id);
        assertNull(reactive.name);
        assertEquals("select \"id\" from \"accounts\"", fixture.dataRequest.sql());
    }

    @Test
    void repositoryDefaultProjectionIntersectsReadableFieldScope() {
        Fixture fixture = new Fixture();
        DataScope idOnly = DataScope.none().withFields(FieldScope.readable("id"));
        SyncFormRepository<Account> syncRepository = SyncFormRepository.create(
                fixture.sync, fixture.sync.entityModels().metadata(Account.class).toDynamicForm(), Account.class);
        ReactiveFormRepository<Account> reactiveRepository = ReactiveFormRepository.create(
                fixture.reactive, fixture.reactive.entityModels().metadata(Account.class).toDynamicForm(), Account.class);

        assertEquals(7L, syncRepository.createQuery().scope(idOnly).one().id);
        assertEquals("select \"id\" from \"accounts\"", fixture.dataRequest.sql());
        assertEquals(7L, reactiveRepository.createQuery().scope(idOnly).one().block().id);
        assertEquals("select \"id\" from \"accounts\"", fixture.dataRequest.sql());
    }

    @Test
    void syncClientDefaultFieldScopeNarrowsEntityDefaultProjection() {
        Fixture fixture = new Fixture();
        SyncFormClient client = fixture.sync.withDefaultDataScope(
                DataScope.none().withFields(FieldScope.readable("id")));

        assertAll(
                () -> assertIdOnlyProjection(fixture, client.entity(Account.class).query().execute().getFirst()),
                () -> assertIdOnlyProjection(fixture, client.entity(Account.class).query().one()),
                () -> assertIdOnlyProjection(fixture, client.entity(Account.class).query()
                        .page(1, 10).rows().getFirst()));
    }

    @Test
    void reactiveClientDefaultFieldScopeNarrowsEntityDefaultProjection() {
        Fixture fixture = new Fixture();
        ReactiveFormClient client = fixture.reactive.withDefaultDataScope(
                DataScope.none().withFields(FieldScope.readable("id")));

        assertAll(
                () -> assertIdOnlyProjection(fixture, client.entity(Account.class).query().execute().single().block()),
                () -> assertIdOnlyProjection(fixture, client.entity(Account.class).query().one().block()),
                () -> assertIdOnlyProjection(fixture, client.entity(Account.class).query()
                        .page(1, 10).block().rows().getFirst()));
    }

    @Test
    void clientDefaultFieldScopeNarrowsBothRepositoryReadPaths() {
        Fixture fixture = new Fixture();
        DataScope idOnly = DataScope.none().withFields(FieldScope.readable("id"));
        SyncFormClient sync = fixture.sync.withDefaultDataScope(idOnly);
        ReactiveFormClient reactive = fixture.reactive.withDefaultDataScope(idOnly);
        SyncFormRepository<Account> syncRepository = SyncFormRepository.create(
                sync, sync.entityModels().metadata(Account.class).toDynamicForm(), Account.class);
        ReactiveFormRepository<Account> reactiveRepository = ReactiveFormRepository.create(
                reactive, reactive.entityModels().metadata(Account.class).toDynamicForm(), Account.class);
        ConditionGroup where = ConditionGroup.and().build();

        assertAll(
                () -> assertIdOnlyProjection(fixture, syncRepository.select(where).getFirst()),
                () -> assertIdOnlyProjection(fixture, syncRepository.page(where, PageQuery.of(1, 10))
                        .rows().getFirst()),
                () -> assertIdOnlyProjection(fixture, syncRepository.createQuery().one()),
                () -> assertIdOnlyProjection(fixture, reactiveRepository.select(where).single().block()),
                () -> assertIdOnlyProjection(fixture, reactiveRepository.page(where, PageQuery.of(1, 10))
                        .block().rows().getFirst()),
                () -> assertIdOnlyProjection(fixture, reactiveRepository.createQuery().one().block()));
    }

    @Test
    void explicitProjectionStillRejectsFieldsOutsideClientDefaultScope() {
        Fixture fixture = new Fixture();
        DataScope idOnly = DataScope.none().withFields(FieldScope.readable("id"));

        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> fixture.sync.withDefaultDataScope(idOnly)
                        .entity(Account.class).query().select(Account::getSecret).executeRows()),
                () -> assertThrows(IllegalArgumentException.class, () -> fixture.reactive.withDefaultDataScope(idOnly)
                        .entity(Account.class).query().select(Account::getSecret).executeRows().collectList().block()));
        assertNull(fixture.dataRequest, "denied explicit projections must fail before execution");
    }

    @Test
    void explicitProjectionCanStillSelectADefaultExcludedField() {
        Fixture fixture = new Fixture();
        DynamicRow sync = fixture.sync.entity(Account.class).query()
                .select(Account::getSecret).executeRows().getFirst();
        assertEquals("classified", sync.get("secret"));
        assertTrue(fixture.dataRequest.sql().contains("\"secret\""));
        DynamicRow reactive = fixture.reactive.entity(Account.class).query()
                .select(Account::getSecret).executeRows().single().block();
        assertEquals("classified", reactive.get("secret"));
        assertTrue(fixture.dataRequest.sql().contains("\"secret\""));
    }

    @Test
    void plainDynamicFormQueriesDoNotInheritEntityDefaultProjection() {
        Fixture fixture = new Fixture();
        QuerySpec spec = QuerySpec.of(fixture.sync.entityModels().metadata(Account.class).toDynamicForm(),
                ConditionGroup.and().build());
        assertEquals("classified", fixture.sync.select(spec).getFirst().get("secret"));
        assertEquals("classified", fixture.reactive.select(spec).single().block().get("secret"));
    }

    @Test
    void noSelectableFieldsRejectOnlyDefaultQueriesNotTheWriteModel() {
        Fixture fixture = new Fixture();
        var metadata = fixture.sync.entityModels().metadata(WriteOnlyAccount.class);
        assertEquals(List.of("secret"), metadata.toDynamicForm().fields().stream()
                .map(field -> field.name()).toList());
        assertDoesNotThrow(() -> fixture.sync.entity(WriteOnlyAccount.class).update());
        assertDoesNotThrow(() -> fixture.reactive.entity(WriteOnlyAccount.class).update());

        assertAll(
                () -> assertThrows(MappingException.class,
                        () -> fixture.sync.entity(WriteOnlyAccount.class).query().execute()),
                () -> assertThrows(MappingException.class,
                        () -> fixture.reactive.entity(WriteOnlyAccount.class).query().execute().collectList().block()));
        assertNull(fixture.dataRequest, "invalid default queries must fail before execution");
    }

    private static void assertSyncTerminals(
            Fixture fixture, Supplier<SyncEntityDmlQueryOperator<Account>> query) {
        assertAll(
                () -> assertDefaultProjection(fixture, query.get().execute().getFirst()),
                () -> assertDefaultProjection(fixture, query.get().one()),
                () -> assertDefaultProjection(fixture, query.get().page(1, 10).rows().getFirst()));
    }

    private static void assertReactiveTerminals(
            Fixture fixture, Supplier<EntityDmlQueryOperator<Account>> query) {
        assertAll(
                () -> assertDefaultProjection(fixture, query.get().execute().single().block()),
                () -> assertDefaultProjection(fixture, query.get().one().block()),
                () -> assertDefaultProjection(fixture, query.get().page(1, 10).block().rows().getFirst()));
    }

    private static void assertDefaultProjection(Fixture fixture, Account result) {
        assertAll(
                () -> assertFalse(fixture.dataRequest.sql().contains("\"secret\""),
                        fixture.dataRequest.sql()),
                () -> assertEquals(7L, result.id),
                () -> assertEquals("Ada", result.name),
                () -> assertNull(result.secret));
    }

    private static void assertIdOnlyProjection(Fixture fixture, Account result) {
        assertAll(
                () -> assertEquals("select \"id\"", fixture.dataRequest.sql()
                        .substring(0, fixture.dataRequest.sql().indexOf(" from "))),
                () -> assertEquals(7L, result.id),
                () -> assertNull(result.name),
                () -> assertNull(result.secret));
    }

    @TableName("accounts")
    private static final class Account {
        @TableId
        private Long id;
        private String name;
        @TableField(select = false)
        private String secret;

        public String getSecret() {
            return secret;
        }
    }

    @TableName("write_only_accounts")
    private static final class WriteOnlyAccount {
        @TableField(select = false)
        private String secret;
    }

    private static final class Fixture {
        private SqlRequest dataRequest;
        private final SyncFormClient sync;
        private final ReactiveFormClient reactive;

        private Fixture() {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
            sync = SyncFormClient.create(new SyncSqlExecutor() {
                @Override
                public List<DynamicRow> query(SqlRequest request) {
                    return List.of(row(request));
                }

                @Override
                public long rowsUpdated(SqlRequest request) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                    throw new UnsupportedOperationException();
                }
            }, new UnusedBatchExecutor(), renderer);
            reactive = ReactiveFormClient.create(new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.just(row(request));
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new UnsupportedOperationException());
                }
            }, renderer);
        }

        private DynamicRow row(SqlRequest request) {
            if (request.sql().startsWith("select count(*)")) {
                return DynamicRow.copyOf(Map.of("total", 1L));
            }
            dataRequest = request;
            // 模拟数据库只返回实际 SELECT 投影，保留真实 SQL 编译和实体结果映射。
            String projection = request.sql().substring(0, request.sql().indexOf(" from "));
            Map<String, Object> values = new LinkedHashMap<>();
            if (projection.contains("\"id\"")) {
                values.put("id", 7L);
            }
            if (projection.contains("\"name\"")) {
                values.put("name", "Ada");
            }
            if (projection.contains("\"secret\"")) {
                values.put("secret", "classified");
            }
            return DynamicRow.copyOf(values);
        }
    }

    private static final class UnusedBatchExecutor implements SyncBatchExecutor {
        @Override
        public BatchExecutionEvidence writeBatch(BatchWriteRequest request, java.util.function.LongConsumer rowCompleted) {
            throw new UnsupportedOperationException();
        }

    }
}
