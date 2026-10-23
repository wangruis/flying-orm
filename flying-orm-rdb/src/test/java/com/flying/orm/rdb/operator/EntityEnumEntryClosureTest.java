package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class EntityEnumEntryClosureTest {
    @Test
    void expressionSetResolvesGetterColumnsAndEnumLiteralsOnBothExecutionPaths() {
        Capture capture = new Capture();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        ReactiveFormClient async = ReactiveFormClient.create(capture.reactive(), renderer);
        SyncFormClient sync = SyncFormClient.create(capture, (request, callback) -> {
            throw new AssertionError("unexpected batch");
        }, renderer);
        sync.entity(Account.class).update().setExpression(Account::getStatus,
                expression -> expression.field(Account::getStatus)
                        .coalesce(expression.value(Account::getStatus, Status.DISABLED)))
                .where(Account::id, 1L).execute();
        Mono<Long> update = async.entity(Account.class).update().setExpression(Account::getStatus,
                expression -> expression.field(Account::getStatus)
                        .coalesce(expression.value(Account::getStatus, Status.ACTIVE)))
                .where(Account::id, 2L).execute();
        assertEquals(1, capture.requests.size());
        assertEquals(1L, update.block());
        assertEquals(List.of("D", 1L), capture.requests.get(0).parameters());
        assertEquals(List.of("A", 2L), capture.requests.get(1).parameters());
        assertTrue(capture.requests.get(0).sql().contains("\"status_code\" = coalesce(\"status_code\", ?)"));
    }

    @TestFactory
    List<DynamicTest> publicPropertyValuesReachQueryPageUpdateAndDelete() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean reactive : List.of(false, true)) {
            for (String operation : List.of("QUERY", "PAGE", "UPDATE", "DELETE")) {
                tests.add(DynamicTest.dynamicTest("public-values/" + reactive + "/" + operation,
                        () -> verifyPublicValues(reactive, operation)));
            }
        }
        return tests;
    }

    private static void verifyPublicValues(boolean reactive, String operation) {
        Capture capture = new Capture();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        ReactiveFormClient async = ReactiveFormClient.create(capture.reactive(), renderer);
        SyncFormClient sync = SyncFormClient.create(capture, (request, callback) -> {
            throw new AssertionError("unexpected batch");
        }, renderer);
        EntityModelRegistry models = reactive ? async.entityModels() : sync.entityModels();
        String column = models.metadata(Account.class).field("status").columnName();
        ConditionGroup where = ConditionGroup.and()
                .add(models.condition(Account.class, "status", "=", Status.ACTIVE)).build();
        QuerySpec query = QuerySpec.of(models.metadata(Account.class).toDynamicForm(), where);
        switch (operation) {
            case "QUERY" -> {
                if (reactive) async.select(query).collectList().block();
                else sync.select(query);
            }
            case "PAGE" -> {
                if (reactive) async.page(query, PageQuery.of(1, 10)).block();
                else sync.page(query, PageQuery.of(1, 10));
            }
            case "UPDATE" -> {
                WriteSpec update = WriteSpec.update(models.metadata(Account.class).toDynamicForm(),
                        Map.of(column, models.writeValue(Account.class, "status", Status.DISABLED)), where);
                if (reactive) async.update(update).block();
                else sync.update(update);
            }
            case "DELETE" -> {
                WriteSpec delete = WriteSpec.delete(models.metadata(Account.class).toDynamicForm(), where);
                if (reactive) async.delete(delete).block();
                else sync.delete(delete);
            }
            default -> throw new AssertionError(operation);
        }
        assertEquals(operation.equals("PAGE") ? 2 : 1, capture.requests.size());
        for (SqlRequest request : capture.requests) {
            assertTrue(request.sql().contains("status_code"), request.sql());
            if (operation.equals("UPDATE")) {
                assertEquals(List.of("D", "A"), request.parameters());
            } else {
                assertEquals("A", request.parameters().getFirst());
                assertTrue(request.parameters().stream().noneMatch(Enum.class::isInstance));
            }
        }
    }

    @Test
    void publicConditionValuesPreserveShapesAndCustomTermSemantics() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            Status[] values = {Status.ACTIVE, Status.DISABLED};
            Object in = models.condition(Account.class, "status", "in", values).value();
            values[0] = Status.DISABLED;
            assertEquals(List.of("A", "D"), in);
            assertEquals(List.of("A", "D"), models.condition(Account.class, "status", "between",
                    List.of(Status.ACTIVE, Status.DISABLED)).value());
            assertNull(models.condition(Account.class, "status", "is-null", null).value());
            assertThrows(IllegalArgumentException.class,
                    () -> models.condition(Account.class, "status", "=", null));
            assertEquals("D", models.condition(Account.class, "status", "=", "D").value());
            TermRegistry custom = TermRegistry.builder()
                    .add(TermHandler.simple("custom-status", ConditionValueShape.SCALAR))
                    .add(TermRegistry.standard().handler("=")).build();
            assertSame(Status.ACTIVE,
                    models.condition(Account.class, "status", "custom-status", Status.ACTIVE, custom).value());
            assertEquals("A",
                    models.condition(Account.class, "status", "=", Status.ACTIVE, custom).value());
            assertThrows(IllegalArgumentException.class,
                    () -> models.condition(Account.class, "missing", "=", Status.ACTIVE));
        }
    }

    @Test
    void publicConditionsKeepMappedScalarValuesWithoutNormalizingThemAgain() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            TermCondition empty = models.condition(Account.class, "status", "=", Status.EMPTY);
            assertEquals("status_code", empty.field());
            assertEquals("", empty.value());
            TermCondition structured = models.condition(StructuredAccount.class, "status", "=", StructuredStatus.ACTIVE);
            assertEquals(Map.of("code", "A"), structured.value());
            assertEquals(List.of(empty, structured), ConditionGroup.and().add(empty).add(structured).build().children());
        }
    }

    @Test
    void publicWriteValuesAcceptOrdinaryAndBoundGetters() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            Account account = new Account(1L, Status.DISABLED);
            EntityProperty<Account, Status> getter = Account::getStatus;
            Supplier<Status> boundGetter = account::getStatus;
            assertEquals("D", models.writeValue(Account.class, "status", getter.apply(account)));
            assertEquals("D", models.writeValue(Account.class, "status", boundGetter.get()));
            assertNull(models.writeValue(Account.class, "status", null));
            assertEquals("D", models.writeValue(Account.class, "status", "D"));
            assertEquals("D", models.writeValue(Account.class, "status_code", Status.DISABLED));
            assertEquals(1L, models.writeValue(Account.class, "id", 1L));
            assertThrows(IllegalArgumentException.class,
                    () -> models.writeValue(Account.class, "missing", Status.DISABLED));
        }
    }

    @TestFactory
    List<DynamicTest> declaredStorageReachesBothExecutionBoundaries() {
        List<DynamicTest> tests = new ArrayList<>();
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.postgresql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            for (boolean reactive : List.of(false, true)) {
                for (String operation : List.of("SET", "=", "in", "between", "is-null")) {
                    tests.add(DynamicTest.dynamicTest(dialect.name() + "/" + reactive + "/" + operation,
                            () -> verify(dialect, reactive, operation)));
                }
            }
        }
        return tests;
    }

    private static void verify(RdbDialect dialect, boolean reactive, String operation) {
        Capture capture = new Capture();
        SqlRenderer sql = SqlRenderer.builder().addDefaultTerms().build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(sql, dialect);
        ReactiveFormClient async = ReactiveFormClient.create(capture.reactive(), renderer);
        SyncFormClient sync = SyncFormClient.create(capture, (request, callback) -> {
            throw new AssertionError("unexpected batch");
        }, renderer);
        if (operation.equals("SET")) {
            if (reactive) async.entity(Account.class).update().set(Account::status, Status.ACTIVE)
                    .where(Account::id, 1L).execute().block();
            else sync.entity(Account.class).update().set(Account::status, Status.ACTIVE)
                    .where(Account::id, 1L).execute();
            assertEquals(List.of("A", 1L), capture.request.parameters());
        } else {
            Object value = operation.equals("in") || operation.equals("between")
                    ? new Status[]{Status.ACTIVE, Status.DISABLED}
                    : operation.equals("is-null") ? null : Status.ACTIVE;
            if (reactive) new EntityJoinQueryOperator<>(async, sql, Account.class)
                    .join(Team.class, Account::id, Team::id).select(Account.class, Account::id)
                    .where(Account.class, Account::status, operation, value).executeRows().collectList().block();
            else new SyncEntityJoinQueryOperator<>(sync, sql, Account.class)
                    .join(Team.class, Account::id, Team::id).select(Account.class, Account::id)
                    .where(Account.class, Account::status, operation, value).executeRows();
            assertEquals(operation.equals("is-null") ? List.of() : operation.equals("=")
                    ? List.of("A") : List.of("A", "D"), capture.request.parameters());
        }
    }

    @Test
    void customTermsKeepTheirDeclaredDomainValue() {
        var values = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()).entityValues(Account.class);
        TermRegistry terms = TermRegistry.builder()
                .add(TermHandler.simple("custom-status", ConditionValueShape.SCALAR)).build();
        assertSame(Status.ACTIVE, values.normalizeConditionValue("status", "custom-status", Status.ACTIVE, terms));
        assertSame(Status.ACTIVE, values.normalizeConditionScalar("status", "custom-status", Status.ACTIVE, terms));
        assertSame(Status.ACTIVE, values.normalizeConditionScalar("status", "=", Status.ACTIVE, terms));
        assertEquals("A", values.normalizeConditionScalar("status", "in", Status.ACTIVE, TermRegistry.standard()));
        assertNull(values.normalizeWriteValue("status", null));
        assertEquals("A", values.normalizeWriteValue("status", "A"));
    }

    enum Status {
        ACTIVE("A"), DISABLED("D"), EMPTY("");
        @EnumValue private final String code;
        Status(String code) { this.code = code; }
    }
    @TableName("enum_accounts")
    record Account(@TableId(type = IdType.INPUT) Long id, @TableField("status_code") Status status) {
        public Status getStatus() { return status; }
    }
    @TableName("enum_teams")
    record Team(@TableId(type = IdType.INPUT) Long id) { }

    record StructuredAccount(StructuredStatus status) { }

    enum StructuredStatus {
        ACTIVE;
        @EnumValue private final Map<String, String> code = Map.of("code", "A");
    }

    private static final class Capture implements com.flying.orm.rdb.support.ListQueryFixture {
        private SqlRequest request;
        private final List<SqlRequest> requests = new ArrayList<>();
        public List<DynamicRow> query(SqlRequest value) {
            request = value;
            requests.add(value);
            return value.sql().contains(" as total from ")
                    ? List.of(DynamicRow.copyOf(Map.of("total", 1L))) : List.of();
        }
        public long rowsUpdated(SqlRequest value) { request = value; requests.add(value); return 1; }
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest value, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new AssertionError("unexpected generated keys");
        }
        private ReactiveSqlExecutor reactive() {
            return new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest value) {
                    return Flux.defer(() -> Flux.fromIterable(Capture.this.query(value)));
                }
                public Mono<Long> rowsUpdated(SqlRequest value) {
                    return Mono.fromSupplier(() -> Capture.this.rowsUpdated(value));
                }
            };
        }
    }
}
