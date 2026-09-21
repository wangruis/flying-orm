package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GovernedJoinSourceIsolationTest {

    @Test
    void previewKeepsProjectFilterSortAndJoinRequirementsDistinctForEverySourceAndAliasLayout() {
        Fixture stableAliases = fixture(false, "account_name", "audit_name");
        Fixture changedAliases = fixture(true, "renamed_audit_name", "renamed_account_name");

        assertAll(
                () -> assertIndependentUses(stableAliases),
                () -> assertIndependentUses(changedAliases));
    }

    @Test
    void sourceOrderAndProjectionAliasesCannotTransferQualifiedAuthority() {
        Fixture granted = fixture(false, "account_name", "audit_name");
        Fixture changed = fixture(true, "renamed_audit_name", "renamed_account_name");
        AtomicInteger executions = new AtomicInteger();
        FieldUsePolicy policy = qualifiedPolicy(
                granted, FieldVisibility.FULL, FieldVisibility.FULL);

        ScopeAccessException error = assertThrows(
                ScopeAccessException.class,
                () -> client(executions, Map.of(), policy).selectJoin(changed.spec()));

        assertAll(
                () -> assertTrue(error.getMessage().contains("source[0:account-audits].name")),
                () -> assertEquals(0, executions.get()));
    }

    @Test
    void bareFieldGrantsDoNotSatisfyMissingQualifiedJoinAuthorizationBeforeExecution() {
        Fixture fixture = fixture(false, "account_name", "audit_name");
        AtomicInteger executions = new AtomicInteger();
        SyncFormClient client = client(executions, Map.of(), bareFieldPolicy());

        assertAll(
                () -> assertThrows(ScopeAccessException.class,
                        () -> client.selectJoin(fixture.spec())),
                () -> assertEquals(0, executions.get(),
                        "missing source-qualified authorization must fail before executor access"));
    }

    @Test
    void fullMaskedAndHiddenPublicationRemainIndependentBySource() {
        Fixture fixture = fixture(false, "account_name", "audit_name");
        Map<String, Object> values = Map.of(
                fixture.rootAlias(), "Alice",
                fixture.joinedAlias(), "security-event");
        DynamicRow fullAndMasked = client(new AtomicInteger(), values,
                qualifiedPolicy(fixture, FieldVisibility.FULL, FieldVisibility.MASKED))
                .selectJoin(fixture.spec()).getFirst();
        DynamicRow hiddenAndFull = client(new AtomicInteger(), values,
                qualifiedPolicy(fixture, FieldVisibility.HIDDEN, FieldVisibility.FULL))
                .selectJoin(fixture.spec()).getFirst();

        assertAll(
                () -> assertEquals("Alice", fullAndMasked.get(fixture.rootAlias())),
                () -> assertEquals("**************", fullAndMasked.get(fixture.joinedAlias())),
                () -> assertFalse(hiddenAndFull.containsKey(fixture.rootAlias())),
                () -> assertEquals("security-event", hiddenAndFull.get(fixture.joinedAlias())));
    }

    @Test
    void missingQualifiedGrantFailsBeforeSyncAndReactiveExecutorAccess() {
        Fixture fixture = fixture(false, "account_name", "audit_name");
        FieldUsePolicy incomplete = qualifiedPolicyBuilder(fixture)
                .joinVisibility(fixture.rootName(), FieldVisibility.FULL)
                .build();
        AtomicInteger syncExecutions = new AtomicInteger();
        AtomicInteger reactiveExecutions = new AtomicInteger();

        assertAll(
                () -> assertThrows(ScopeAccessException.class,
                        () -> client(syncExecutions, Map.of(), incomplete).selectJoin(fixture.spec())),
                () -> assertThrows(ScopeAccessException.class,
                        () -> ReactiveFormClient.create(
                                        reactiveExecutor(reactiveExecutions, DynamicRow.copyOf(Map.of())),
                                        renderer())
                                .withFieldUsePolicy(incomplete)
                                .selectJoin(fixture.spec()).collectList().block()),
                () -> assertEquals(0, syncExecutions.get()),
                () -> assertEquals(0, reactiveExecutions.get()));
    }

    @Test
    void unrestrictedGovernedJoinKeepsTheFastPathAndExecutesSymmetrically() {
        Fixture fixture = fixture(false, "account_name", "audit_name");
        Map<String, Object> values = Map.of(
                fixture.rootAlias(), "Alice", fixture.joinedAlias(), "security-event");
        AtomicInteger syncExecutions = new AtomicInteger();
        AtomicInteger reactiveExecutions = new AtomicInteger();

        DynamicRow sync = client(syncExecutions, values, FieldUsePolicy.unrestricted())
                .withQueryShapeLimits(QueryShapeLimits.defaults().withMaxProjectionCount(16))
                .selectJoin(fixture.spec()).getFirst();
        DynamicRow reactive = ReactiveFormClient.create(
                        reactiveExecutor(reactiveExecutions, DynamicRow.copyOf(values)), renderer())
                .withQueryShapeLimits(QueryShapeLimits.defaults().withMaxProjectionCount(16))
                .selectJoin(fixture.spec()).single().block();

        assertAll(
                () -> assertEquals("*****", sync.get(fixture.rootAlias())),
                () -> assertEquals("**************", sync.get(fixture.joinedAlias())),
                () -> assertEquals("*****", reactive.get(fixture.rootAlias())),
                () -> assertEquals("**************", reactive.get(fixture.joinedAlias())),
                () -> assertEquals(1, syncExecutions.get()),
                () -> assertEquals(1, reactiveExecutions.get()));
    }

    private static void assertIndependentUses(Fixture fixture) {
        FieldUseSnapshot snapshot = client(new AtomicInteger(), Map.of(), qualifiedPolicy(
                        fixture, FieldVisibility.FULL, FieldVisibility.FULL))
                .previewFieldUse(fixture.spec());

        assertAll(
                () -> assertDecision(snapshot, fixture.rootName(), FieldUse.PROJECT),
                () -> assertDecision(snapshot, fixture.joinedName(), FieldUse.PROJECT),
                () -> assertDecision(snapshot, new JoinFieldRef(fixture.root(), "state"), FieldUse.FILTER),
                () -> assertDecision(snapshot, new JoinFieldRef(fixture.joined(), "state"), FieldUse.FILTER),
                () -> assertDecision(snapshot, new JoinFieldRef(fixture.root(), "created_at"), FieldUse.SORT),
                () -> assertDecision(snapshot, new JoinFieldRef(fixture.joined(), "created_at"), FieldUse.SORT),
                () -> assertDecision(snapshot, new JoinFieldRef(fixture.root(), "id"), FieldUse.JOIN),
                () -> assertDecision(snapshot, new JoinFieldRef(fixture.joined(), "id"), FieldUse.JOIN),
                () -> assertTrue(snapshot.decisions().isEmpty(),
                        "qualified JOIN decisions must not leak into the bare field namespace"));
    }

    private static void assertDecision(FieldUseSnapshot snapshot, JoinFieldRef field, FieldUse use) {
        assertTrue(snapshot.joinDecision(field, use, com.flying.orm.core.scope.FieldUseOrigin.CALLER)
                           .orElseThrow().allowed(),
                () -> use + " authorization for [" + field + "] must remain source-qualified");
    }

    private static FieldUsePolicy bareFieldPolicy() {
        return basePolicy().visibility("name", FieldVisibility.FULL).build();
    }

    private static FieldUsePolicy.Builder basePolicy() {
        return FieldUsePolicy.builder()
                .allow("id", FieldUse.JOIN)
                .allow("state", FieldUse.FILTER)
                .allow("created_at", FieldUse.SORT);
    }

    private static FieldUsePolicy qualifiedPolicy(Fixture fixture,
                                                  FieldVisibility rootVisibility,
                                                  FieldVisibility joinedVisibility) {
        FieldUsePolicy.Builder builder = qualifiedPolicyBuilder(fixture)
                .joinVisibility(fixture.rootName(), rootVisibility)
                .joinVisibility(fixture.joinedName(), joinedVisibility);
        if (rootVisibility == FieldVisibility.HIDDEN) {
            builder.allowJoin(fixture.rootName(), FieldUse.PROJECT);
        }
        if (joinedVisibility == FieldVisibility.HIDDEN) {
            builder.allowJoin(fixture.joinedName(), FieldUse.PROJECT);
        }
        return builder.build();
    }

    private static FieldUsePolicy.Builder qualifiedPolicyBuilder(Fixture fixture) {
        return FieldUsePolicy.builder()
                .allowJoin(new JoinFieldRef(fixture.root(), "id"), FieldUse.JOIN)
                .allowJoin(new JoinFieldRef(fixture.joined(), "id"), FieldUse.JOIN)
                .allowJoin(new JoinFieldRef(fixture.root(), "state"), FieldUse.FILTER)
                .allowJoin(new JoinFieldRef(fixture.joined(), "state"), FieldUse.FILTER)
                .allowJoin(new JoinFieldRef(fixture.root(), "created_at"), FieldUse.SORT)
                .allowJoin(new JoinFieldRef(fixture.joined(), "created_at"), FieldUse.SORT);
    }

    private static Fixture fixture(boolean reverseSources,
                                   String rootProjectionAlias,
                                   String joinedProjectionAlias) {
        DynamicForm accounts = form("accounts", "customer_accounts");
        DynamicForm audits = form("account-audits", "customer_account_audits");
        DynamicForm rootForm = reverseSources ? audits : accounts;
        DynamicForm joinedForm = reverseSources ? accounts : audits;
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(rootForm);
        JoinSource root = builder.root();
        JoinSource joined = builder.join(JoinType.INNER, joinedForm, root, "id", "id");
        JoinQuerySpec spec = builder
                .selectAs(root, "name", rootProjectionAlias)
                .selectAs(joined, "name", joinedProjectionAlias)
                .where(root, ConditionGroup.and().where("state", "=", "active").build())
                .where(joined, ConditionGroup.and().where("state", "=", "active").build())
                .orderBy(root, "created_at", PageSort.Direction.ASC)
                .orderBy(joined, "created_at", PageSort.Direction.DESC)
                .build();
        return new Fixture(spec, root, joined, new JoinFieldRef(root, "name"),
                new JoinFieldRef(joined, "name"), rootProjectionAlias, joinedProjectionAlias);
    }

    private static DynamicForm form(String id, String table) {
        return DynamicForm.builder(id, table)
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("state", "VARCHAR"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .addField(DynamicField.of("created_at", "TIMESTAMP"))
                .masked("name", MaskedFieldDefinition.builder("full").build())
                .build();
    }

    private static SyncFormClient client(AtomicInteger executions,
                                         Map<String, Object> result,
                                         FieldUsePolicy policy) {
        return SyncFormClient.create(syncExecutor(executions, DynamicRow.copyOf(result)),
                        syncBatchExecutor(), renderer())
                .withFieldUsePolicy(policy);
    }

    private static SyncSqlExecutor syncExecutor(AtomicInteger executions, DynamicRow result) {
        return (SyncSqlExecutor) Proxy.newProxyInstance(
                SyncSqlExecutor.class.getClassLoader(), new Class<?>[]{SyncSqlExecutor.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("queryMapped")) {
                        executions.incrementAndGet();
                        @SuppressWarnings("unchecked")
                        RowMapper<Object> mapper = (RowMapper<Object>) arguments[2];
                        return List.of(mapper.map(result));
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static SyncBatchExecutor syncBatchExecutor() {
        return (SyncBatchExecutor) Proxy.newProxyInstance(
                SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                (proxy, method, arguments) -> {
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static ReactiveSqlExecutor reactiveExecutor(AtomicInteger executions, DynamicRow result) {
        return new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
                executions.incrementAndGet();
                return Flux.just(result);
            }

            @Override
            public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }
        };
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    private record Fixture(JoinQuerySpec spec,
                           JoinSource root,
                           JoinSource joined,
                           JoinFieldRef rootName,
                           JoinFieldRef joinedName,
                           String rootAlias,
                           String joinedAlias) {
    }
}
