package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SelfJoinScopeAndGovernanceTest {

    @Test
    void sameFormAndDifferentFormsOfOneTableRetainSourceIdentityScopeAndSqlAliases() {
        for (boolean sameForm : List.of(true, false)) {
            Fixture fixture = fixture(sameForm, FieldScope.unrestricted());
            for (RdbDialect dialect : List.of(RdbDialect.h2(), RdbDialect.mysql(), RdbDialect.postgresql(),
                    RdbDialect.oracle(), RdbDialect.sqlServer())) {
                List<SqlRequest> syncRequests = new ArrayList<>();
                List<SqlRequest> reactiveRequests = new ArrayList<>();
                var sync = sync(syncRequests, FieldUsePolicy.unrestricted(), dialect);
                var reactive = reactive(reactiveRequests, FieldUsePolicy.unrestricted(), dialect);

                assertEquals("Alice", sync.selectJoin(fixture.spec()).getFirst().get("employee"));
                assertEquals("Boss", reactive.selectJoin(fixture.spec()).single().block().get("manager"));
                assertEquals(List.of(0, 1), fixture.spec().sources().stream().map(JoinSource::ordinal).toList());
                assertNotEquals(fixture.employee(), fixture.manager());
                if (sameForm) assertSame(fixture.employee().form(), fixture.manager().form());
                assertNotEquals(fixture.spec().scope(fixture.employee()), fixture.spec().scope(fixture.manager()));
                assertEquals(syncRequests.getFirst().sql(), reactiveRequests.getFirst().sql());
                assertEquals(List.of(7L, 2L, 7L, "enabled"), syncRequests.getFirst().parameters());
                String sql = syncRequests.getFirst().sql();
                var identifiers = dialect.schema();
                assertTrue(sql.contains(identifiers.identifier("t0") + "." + identifiers.identifier("name")
                        + " as " + (dialect.name().equals("h2") ? "\"employee\"" : identifiers.identifier("employee"))
                        + ", " + identifiers.identifier("t1") + "." + identifiers.identifier("name") + " as "
                        + (dialect.name().equals("h2") ? "\"manager\"" : identifiers.identifier("manager"))), sql);
                assertTrue(sql.contains(") " + identifiers.identifier("t0") + " left outer join (select * from "
                        + identifiers.identifier("employees") + " where"), sql);
                assertTrue(sql.contains("on " + identifiers.identifier("t0") + "." + identifiers.identifier("manager_id")
                        + " = " + identifiers.identifier("t1") + "." + identifiers.identifier("id")), sql);
            }
        }
    }

    @Test
    void publicJdbcSelfJoinKeepsTenantIsolationWithoutCollapsingTheOuterJoin() throws Exception {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:self_join_" + UUID.randomUUID() + ";DATABASE_TO_LOWER=TRUE");
        try (var keeper = source.getConnection(); var statement = keeper.createStatement()) {
            statement.execute("create table employees(id bigint primary key, manager_id bigint, tenant_id bigint, "
                    + "name varchar(40), state varchar(20))");
            statement.execute("insert into employees values "
                    + "(1,null,7,'Boss','enabled'),(2,1,7,'Alice','enabled'),"
                    + "(3,1,8,'Eve','enabled'),(4,9,7,'Bob','enabled'),(9,null,8,'ForeignBoss','enabled')");
            SyncFormClient client = SyncFormClient.create(
                    SyncSqlExecutor.jdbc(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(source), RdbDialect.h2()),
                    SyncBatchExecutor.jdbc(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(source), RdbDialect.h2()),
                    renderer(RdbDialect.h2())).withDefaultDataScope(tenantScope());
            for (boolean sameForm : List.of(true, false)) {
                List<DynamicRow> rows = client.selectJoin(fixture(sameForm, FieldScope.unrestricted()).spec());
                assertEquals(List.of("Alice", "Bob"), rows.stream().map(row -> row.get("employee")).toList());
                assertEquals("Boss", rows.getFirst().get("manager"));
                assertNull(rows.getLast().get("manager"), "another tenant's manager must remain invisible");
            }
        }
    }

    @Test
    void qualifiedFullMaskedAndHiddenVisibilityStayIndependentOnBothPublicClients() {
        for (boolean sameForm : List.of(true, false)) {
            Fixture fixture = fixture(sameForm, FieldScope.unrestricted());
            for (FieldVisibility employee : List.of(FieldVisibility.FULL, FieldVisibility.HIDDEN)) {
                FieldVisibility manager = employee == FieldVisibility.FULL ? FieldVisibility.MASKED : FieldVisibility.FULL;
                FieldUsePolicy policy = policy(fixture, employee, manager, true);
                var sync = sync(new ArrayList<>(), policy);
                var reactive = reactive(new ArrayList<>(), policy);
                for (FieldUseSnapshot snapshot : List.of(sync.previewFieldUse(fixture.spec()),
                        reactive.previewFieldUse(fixture.spec()))) {
                    assertEquals(employee, snapshot.joinVisibility(new JoinFieldRef(fixture.employee(), "name")));
                    assertEquals(manager, snapshot.joinVisibility(new JoinFieldRef(fixture.manager(), "name")));
                    assertTrue(snapshot.decisions().isEmpty(), "self JOIN must keep the source-qualified namespace");
                }
                for (DynamicRow row : List.of(sync.selectJoin(fixture.spec()).getFirst(),
                        reactive.selectJoin(fixture.spec()).single().block())) {
                    assertEquals(employee != FieldVisibility.HIDDEN, row.containsKey("employee"));
                    if (employee == FieldVisibility.FULL) assertEquals("Alice", row.get("employee"));
                    assertEquals(manager == FieldVisibility.MASKED ? "****" : "Boss", row.get("manager"));
                }
            }
        }
    }

    @Test
    void grantingTheRootFieldDoesNotAuthorizeTheRepeatedSourceBeforeExecutorAccess() {
        Fixture fixture = fixture(true, FieldScope.unrestricted());
        FieldUsePolicy policy = policy(fixture, FieldVisibility.FULL, FieldVisibility.FULL, false);
        List<SqlRequest> syncRequests = new ArrayList<>();
        List<SqlRequest> reactiveRequests = new ArrayList<>();
        assertThrows(ScopeAccessException.class, () -> sync(syncRequests, policy).selectJoin(fixture.spec()));
        assertThrows(ScopeAccessException.class,
                () -> reactive(reactiveRequests, policy).selectJoin(fixture.spec()).collectList().block());
        assertTrue(syncRequests.isEmpty());
        assertTrue(reactiveRequests.isEmpty());
    }

    @Test
    void sourceFieldScopeCannotBorrowReadabilityFromTheSameFormAtAnotherOrdinal() {
        Fixture fixture = fixture(true, FieldScope.readable("id", "manager_id", "tenant_id", "state"));
        List<SqlRequest> syncRequests = new ArrayList<>();
        List<SqlRequest> reactiveRequests = new ArrayList<>();
        assertThrows(IllegalArgumentException.class,
                () -> sync(syncRequests, FieldUsePolicy.unrestricted()).selectJoin(fixture.spec()));
        assertThrows(IllegalArgumentException.class, () -> reactive(reactiveRequests, FieldUsePolicy.unrestricted())
                .selectJoin(fixture.spec()).collectList().block());
        assertTrue(syncRequests.isEmpty());
        assertTrue(reactiveRequests.isEmpty());
    }

    @Test
    void businessUsesCannotBorrowTheSameNamedFieldGrantFromAnotherSource() {
        DynamicForm form = form("employees");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource employee = builder.root();
        JoinSource manager = builder.join(JoinType.LEFT, form, employee, "id", "id");
        JoinQuerySpec spec = builder.select(employee, "id").select(manager, "id")
                .where(manager, ConditionGroup.and().where("id", ">", 0L).build())
                .orderBy(manager, "id", PageSort.Direction.ASC).build();
        List<FieldUse> uses = List.of(FieldUse.PROJECT, FieldUse.FILTER, FieldUse.SORT, FieldUse.JOIN);
        for (FieldUse omitted : uses) {
            var policy = FieldUsePolicy.builder();
            for (FieldUse use : uses) {
                policy.allowJoin(new JoinFieldRef(employee, "id"), use);
                if (use != omitted) policy.allowJoin(new JoinFieldRef(manager, "id"), use);
            }
            FieldUsePolicy incomplete = policy.build();
            List<SqlRequest> syncRequests = new ArrayList<>();
            List<SqlRequest> reactiveRequests = new ArrayList<>();
            ScopeAccessException syncError = assertThrows(ScopeAccessException.class,
                    () -> sync(syncRequests, incomplete).selectJoin(spec));
            ScopeAccessException reactiveError = assertThrows(ScopeAccessException.class,
                    () -> reactive(reactiveRequests, incomplete).selectJoin(spec).collectList().block());
            for (ScopeAccessException error : List.of(syncError, reactiveError)) {
                assertTrue(error.getMessage().contains("source[1:employees].id"), error.getMessage());
                assertTrue(error.getMessage().contains("for " + omitted), error.getMessage());
            }
            assertTrue(syncRequests.isEmpty());
            assertTrue(reactiveRequests.isEmpty());
        }
    }

    @Test
    void changingOnlyProjectionAliasesCannotChangeSourceFieldDecisions() {
        DynamicForm form = form("employees");
        Fixture original = fixture(form, form, FieldScope.unrestricted(), "employee", "manager");
        Fixture renamed = fixture(form, form, FieldScope.unrestricted(), "manager_label", "employee_label");
        FieldUsePolicy policy = policy(original, FieldVisibility.FULL, FieldVisibility.MASKED, true);
        var sync = sync(new ArrayList<>(), policy);
        var reactive = reactive(new ArrayList<>(), policy);
        FieldUseSnapshot expected = sync.previewFieldUse(original.spec());
        for (FieldUseSnapshot snapshot : List.of(sync.previewFieldUse(renamed.spec()),
                reactive.previewFieldUse(original.spec()), reactive.previewFieldUse(renamed.spec()))) {
            assertEquals(expected.joinDecisions(), snapshot.joinDecisions());
            assertEquals(FieldVisibility.FULL, snapshot.joinVisibility(new JoinFieldRef(renamed.employee(), "name")));
            assertEquals(FieldVisibility.MASKED, snapshot.joinVisibility(new JoinFieldRef(renamed.manager(), "name")));
        }
    }

    private static Fixture fixture(boolean sameForm, FieldScope managerFields) {
        DynamicForm form = form("employees");
        return fixture(form, sameForm ? form : form("managers"), managerFields, "employee", "manager");
    }

    private static Fixture fixture(DynamicForm form, DynamicForm joinedForm, FieldScope managerFields,
                                   String employeeAlias, String managerAlias) {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource employee = builder.root();
        JoinSource manager = builder.join(JoinType.LEFT, joinedForm, employee, "manager_id", "id");
        JoinQuerySpec spec = builder.scope(employee, scope("id", ">=", 2L))
                .scope(manager, scope("state", "=", "enabled").withFields(managerFields))
                .selectAs(employee, "name", employeeAlias).selectAs(manager, "name", managerAlias)
                .orderBy(employee, "id", PageSort.Direction.ASC).showSensitive().build();
        return new Fixture(spec, employee, manager);
    }

    private static DynamicForm form(String id) {
        return DynamicForm.builder(id, "employees").addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("manager_id", "BIGINT")).addField(DynamicField.of("tenant_id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR")).addField(DynamicField.of("state", "VARCHAR"))
                .masked("name", MaskedFieldDefinition.builder("full").build()).build();
    }

    private static FieldUsePolicy policy(Fixture fixture, FieldVisibility employee, FieldVisibility manager,
                                          boolean authorizeManagerName) {
        JoinFieldRef employeeName = new JoinFieldRef(fixture.employee(), "name");
        JoinFieldRef managerName = new JoinFieldRef(fixture.manager(), "name");
        var builder = FieldUsePolicy.builder()
                .allowJoin(new JoinFieldRef(fixture.employee(), "manager_id"), FieldUse.JOIN)
                .allowJoin(new JoinFieldRef(fixture.manager(), "id"), FieldUse.JOIN)
                .allowJoin(new JoinFieldRef(fixture.employee(), "id"), FieldUse.SORT)
                .joinVisibility(employeeName, employee);
        if (employee == FieldVisibility.HIDDEN) builder.allowJoin(employeeName, FieldUse.PROJECT);
        if (authorizeManagerName) builder.joinVisibility(managerName, manager);
        return builder.build();
    }

    private static SyncFormClient sync(List<SqlRequest> requests, FieldUsePolicy policy) {
        return sync(requests, policy, RdbDialect.postgresql());
    }

    private static SyncFormClient sync(List<SqlRequest> requests, FieldUsePolicy policy, RdbDialect dialect) {
        SyncSqlExecutor executor = (SyncSqlExecutor) Proxy.newProxyInstance(SyncSqlExecutor.class.getClassLoader(),
                new Class<?>[]{SyncSqlExecutor.class}, (proxy, method, args) -> {
                    if (method.getName().equals("queryMapped")) {
                        requests.add((SqlRequest) args[0]);
                        @SuppressWarnings("unchecked") RowMapper<Object> mapper = (RowMapper<Object>) args[2];
                        return List.of(mapper.map(raw()));
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
        SyncBatchExecutor batch = (SyncBatchExecutor) Proxy.newProxyInstance(SyncBatchExecutor.class.getClassLoader(),
                new Class<?>[]{SyncBatchExecutor.class}, (proxy, method, args) -> { throw new UnsupportedOperationException(); });
        return SyncFormClient.create(executor, batch, renderer(dialect))
                .withDefaultDataScope(tenantScope()).withFieldUsePolicy(policy);
    }

    private static ReactiveFormClient reactive(List<SqlRequest> requests, FieldUsePolicy policy) {
        return reactive(requests, policy, RdbDialect.postgresql());
    }

    private static ReactiveFormClient reactive(List<SqlRequest> requests, FieldUsePolicy policy, RdbDialect dialect) {
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override public Flux<DynamicRow> query(SqlRequest request) { requests.add(request); return Flux.just(raw()); }
            @Override public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new UnsupportedOperationException()); }
        };
        return ReactiveFormClient.create(executor, renderer(dialect))
                .withDefaultDataScope(tenantScope()).withFieldUsePolicy(policy);
    }

    private static DynamicRow raw() { return DynamicRow.copyOf(Map.of("employee", "Alice", "manager", "Boss")); }
    private static DataScope tenantScope() { return scope("tenant_id", "=", 7L); }
    private static DataScope scope(String field, String operator, Object value) {
        return DataScope.where(ConditionGroup.and().where(field, operator, value).build());
    }
    private static FormDataSqlRenderer renderer(RdbDialect dialect) {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
    }
    private record Fixture(JoinQuerySpec spec, JoinSource employee, JoinSource manager) { }
}
