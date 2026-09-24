package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.TermExtensionDescriptor;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import io.r2dbc.spi.ConnectionFactories;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperatorUsabilityAcceptanceTest {
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final DynamicForm USERS = DynamicForm.builder("users", "users")
            .addField(DynamicField.primaryKey("id", "INTEGER"))
            .addField(DynamicField.of("name", "VARCHAR(128)"))
            .addField(DynamicField.of("org_id", "INTEGER"))
            .addField(DynamicField.of("deleted", "INTEGER"))
            .logicDelete("deleted", 0, 1).build();
    private Connection keepAlive;
    private FlyingOrmClients clients;

    @BeforeEach
    void open() throws Exception {
        String database = "usability_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database + ";DATABASE_TO_LOWER=TRUE");
        keepAlive = source.getConnection();
        var factory = ConnectionFactories.get("r2dbc:h2:mem:///" + database + "?options=DATABASE_TO_LOWER=TRUE");
        clients = FlyingOrmClients.builder(
                JdbcConnectionAccess.of(request -> source.getConnection(), (connection, request) -> connection.close()),
                R2dbcConnectionAccess.of(request -> factory.create(), (signal, connection, request) -> connection.close()))
                .protectedFields(ProtectedFieldKeyRing.single("test", new byte[32]))
                .renderer(SqlRenderer.builder().addDefaultTerms()
                        .addTerm(SqlTermHandler.of("legacy-eq", ConditionValueShape.SCALAR,
                                SqlTermHandler.equalsTo()::render))
                        .addTerm(SqlTermHandler.of(TermExtensionDescriptor.filter(
                                "same-org", Set.of(), 1, 1), ConditionValueShape.SCALAR,
                                SqlTermHandler.equalsTo()::render))
                        .addTerm(SqlTermHandler.of(TermExtensionDescriptor.filter(
                                "missing-cap", Set.of("missing-capability"), 1, 1),
                                ConditionValueShape.SCALAR, SqlTermHandler.equalsTo()::render)).build())
                .configuredDialect("h2").build();
        clients.syncSchema().createTable(USERS);
    }

    @AfterEach
    void close() throws Exception {
        if (clients != null) { clients.close(); }
        if (keepAlive != null) { keepAlive.close(); }
    }

    @Test
    void dynamicDdlCanReuseTheSameFormForCreationAndQueries() {
        DynamicForm form = DynamicForm.builder("notes", "notes")
                .addField(DynamicField.primaryKey("id", "INTEGER"))
                .addField(DynamicField.of("body", "VARCHAR(128)")).build();
        clients.operator().ddl().createOrAlter(form).block(WAIT);
        clients.syncOperator().ddl().createOrAlter(form);
        clients.syncOperator().dml().insert(form, Map.of("id", 1, "body", "hello"));
        assertEquals("hello", clients.operator().dml().query(form).where("id", 1).one().block(WAIT).get("body"));
    }

    @Test
    void queryProjectionSortPageMappingAndOneUseOneFluentEntry() {
        var dml = clients.syncOperator().dml();
        dml.insert(USERS, row(1, "Alice", 7));
        dml.insert(USERS, row(2, "Bob", 7));
        var page = dml.query(USERS).select("id", "name").orderByDesc("id").page(1, 1);
        assertEquals(2, page.total());
        assertEquals("Bob", page.rows().getFirst().get("name"));
        assertEquals(new UserView(1, "Alice"), dml.query(USERS).select("id", "name")
                .where("id", 1).fetch(UserView.class).getFirst());
        assertEquals("Alice", dml.query(USERS).where("id", 1).one().get("name"));
        assertThrows(IllegalStateException.class, () -> dml.query(USERS).one());
        PageResult<DynamicRow> reactive = clients.operator().dml().query(USERS)
                .select("id", "name").orderByDesc("id").page(1, 1).block(WAIT);
        assertEquals(page, reactive);
        assertEquals(List.of(new UserView(1, "Alice")), clients.operator().dml().query(USERS)
                .select("id", "name").where("id", 1).fetch(UserView.class).collectList().block(WAIT));
    }

    @Test
    void frontendFilterAndBusinessWhereOnlyNarrowTrustedScope() {
        var dml = clients.syncOperator().dml();
        dml.insertBatch(USERS, List.of(row(1, "Alice", 7), row(2, "Bob", 7), row(3, "Bob", 8)));
        var input = StructuredConditionInput.or(StructuredConditionInput.term("id", "=", 2),
                StructuredConditionInput.term("id", "=", 3));
        DataScope scope = DataScope.orgOnly("org_id", 7);
        var query = clients.operator().withDefaultDataScope(scope).dml().query(USERS)
                .filter(input).where("name", "Bob");
        assertEquals(2, query.one().block(WAIT).get("id"));
        assertEquals(1, query.page(1, 10).block(WAIT).total());
        assertEquals(0, clients.syncOperator().withDefaultDataScope(scope).dml().query(USERS)
                .filter(input).where("name", "Alice").fetchMap().size());
        var count = AggregateExpression.count("id", "total");
        assertEquals(1L, query.aggregate(report -> report.aggregate(count)).single().block(WAIT).get(count));
        assertThrows(IllegalArgumentException.class, () -> dml.query(USERS)
                .filter(StructuredConditionInput.term("id) or 1=1 --", "=", 1)).fetchMap());
    }

    @Test
    void fieldPolicyAppliesToPagingMappingAndCombinedConditions() {
        clients.syncOperator().dml().insert(USERS, row(1, "Alice", 7));
        var policy = FieldUsePolicy.builder().visibility("id", FieldVisibility.FULL)
                .allow("id", FieldUse.PROJECT, FieldUse.FILTER, FieldUse.SORT).build();
        var query = clients.operator().dml().query().from(USERS, policy)
                .select("id").filter(StructuredConditionInput.term("id", "=", 1)).where("name", "Alice");
        assertThrows(ScopeAccessException.class, () -> query.page(1, 10).block(WAIT));
        assertThrows(ScopeAccessException.class, () -> query.fetch(UserView.class).blockLast(WAIT));
    }

    @Test
    void aggregateChecksBothFrontendAndBackendFilterFields() {
        clients.syncOperator().dml().insert(USERS, row(1, "Alice", 7));
        var scope = DataScope.none().withFields(FieldScope.readable("id"));
        var input = StructuredConditionInput.term("id", "=", 1);
        var count = AggregateExpression.count("id", "total");
        assertThrows(ScopeAccessException.class, () -> clients.syncOperator().withDefaultDataScope(scope)
                .dml().query(USERS).filter(input).where("name", "Alice")
                .aggregate(report -> report.aggregate(count)));
        assertThrows(ScopeAccessException.class, () -> clients.operator().withDefaultDataScope(scope)
                .dml().query(USERS).filter(input).where("name", "Alice")
                .aggregate(report -> report.aggregate(count)).blockLast(WAIT));
    }

    @Test
    void formWritesRetainLogicDeleteScopeAndEmptyPredicateProtection() {
        var dml = clients.operator().withDefaultDataScope(DataScope.orgOnly("org_id", 7)).dml();
        clients.syncOperator().dml().insertBatch(USERS,
                List.of(row(1, "Alice", 7), row(2, "Bob", 8)));
        assertEquals(1L, dml.update(USERS).set("name", "Changed").where("id", "in", List.of(1, 2))
                .execute().block(WAIT));
        assertEquals("Bob", clients.syncOperator().dml().query(USERS).where("id", 2).one().get("name"));
        assertThrows(IllegalArgumentException.class, () -> dml.delete(USERS).execute().block(WAIT));
        assertEquals(1L, dml.delete(USERS).where("id", "in", List.of(1, 2)).execute().block(WAIT));
        assertEquals(0, dml.query(USERS).fetchMap().collectList().block(WAIT).size());
        assertEquals(1, clients.syncOperator().dml().query().from("users").where("id", 1).one().get("deleted"));
    }

    @Test
    void reactiveBatchStaysLazyAndProducesExecutionEvidence() {
        var dml = clients.operator().dml();
        var pending = dml.insertBatch(USERS, Flux.just(row(1, "Alice", 7), row(2, "Bob", 7)));
        assertEquals(0, clients.syncOperator().dml().query(USERS).fetchMap().size());
        assertEquals(2, pending.block(WAIT).successfulCount());
        assertEquals(1, dml.upsertBatch(USERS, Flux.just(row(1, "Updated", 7))).block(WAIT).successfulCount());
        assertEquals("Updated", dml.query(USERS).where("id", 1).one().block(WAIT).get("name"));
    }

    @Test
    void cursorKeysetGroupingAndCustomMappingKeepFormScope() {
        clients.syncOperator().dml().insertBatch(USERS,
                List.of(row(1, "Alice", 7), row(2, "Bob", 7), row(3, "Other", 8)));
        var reactive = clients.operator().withDefaultDataScope(DataScope.orgOnly("org_id", 7)).dml();
        var sync = clients.syncOperator().withDefaultDataScope(DataScope.orgOnly("org_id", 7)).dml();
        var cursor = CursorPageQuery.after(10, List.of(1), CursorSort.asc("id"));
        assertEquals(sync.query(USERS).cursorPage(cursor), reactive.query(USERS).cursorPage(cursor).block(WAIT));
        assertEquals(1, sync.query(USERS).cursorPage(cursor).rows().size());
        var first = sync.query(USERS).keysetPage(KeysetPageQuery.first(1, KeysetSort.asc("id", NullOrder.LAST)));
        var keyset = KeysetPageQuery.after(10, first.nextPosition(), KeysetSort.asc("id", NullOrder.LAST));
        assertEquals(sync.query(USERS).keysetPage(keyset), reactive.query(USERS).keysetPage(keyset).block(WAIT));
        assertEquals(1, sync.query(USERS).keysetPage(keyset).rows().size());
        assertEquals(List.of(7), sync.query(USERS).select("org_id").groupBy("org_id")
                .fetch(result -> (Integer) result.get("org_id")));
        assertEquals(List.of(7), reactive.query(USERS).select("org_id").groupBy("org_id")
                .fetch(result -> (Integer) result.get("org_id")).collectList().block(WAIT));
        assertEquals(List.of("Bob", "Alice"), clients.syncOperator().dml().query().from("users")
                .select("name").where("org_id", 7).orderByDesc("id")
                .fetch(result -> (String) result.get("name")));
    }

    @Test
    void boundFormWritesAndMappingPreserveEncryptionAndMasking() {
        var protectedForm = DynamicForm.builder("contacts", "contacts")
                .addField(DynamicField.primaryKey("id", "INTEGER"))
                .addField(DynamicField.of("phone", "VARCHAR(128)"))
                .encrypted("phone", EncryptedFieldDefinition.builder().searchModes(EncryptedSearchMode.EXACT).build())
                .masked("phone", MaskedFieldDefinition.builder("partial").prefix(3).suffix(4).build()).build();
        clients.syncSchema().createTable(protectedForm);
        var sync = clients.syncOperator().dml();
        var reactive = clients.operator().dml();
        sync.insertBatch(protectedForm, List.of(Map.of("id", 1, "phone", "13800001234")));
        assertEquals("13800001234", reactive.query(protectedForm).showSensitive()
                .where("phone", ProtectedConditions.EXACT, "13800001234")
                .fetch(result -> (String) result.get("phone")).single().block(WAIT));
        assertEquals(1L, reactive.update(protectedForm).set("phone", "13900005678").where("id", 1)
                .execute().block(WAIT));
        assertEquals("139****5678", sync.query(protectedForm).masked()
                .where("phone", ProtectedConditions.EXACT, "13900005678").one().get("phone"));
        PhoneView masked = new PhoneView(1, "139****5678");
        assertEquals(masked, sync.query(protectedForm).select("id", "phone").masked().one(PhoneView.class));
        assertEquals(masked, reactive.query(protectedForm).select("id", "phone").masked()
                .one(PhoneView.class).block(WAIT));
        assertEquals(List.of(masked), sync.query(protectedForm).select("id", "phone").masked()
                .page(1, 10, PhoneView.class).rows());
        assertEquals(List.of(masked), reactive.query(protectedForm).select("id", "phone").masked()
                .page(1, 10, PhoneView.class).block(WAIT).rows());
        CursorPageQuery cursor = CursorPageQuery.first(10, CursorSort.asc("id"));
        assertEquals(List.of(masked), sync.query(protectedForm).select("id", "phone").masked()
                .cursorPage(cursor, PhoneView.class).rows());
        assertEquals(List.of(masked), reactive.query(protectedForm).select("id", "phone").masked()
                .cursorPage(cursor, PhoneView.class).block(WAIT).rows());
        KeysetPageQuery keyset = KeysetPageQuery.first(10, KeysetSort.asc("id", NullOrder.LAST));
        assertEquals(List.of(masked), sync.query(protectedForm).select("id", "phone").masked()
                .keysetPage(keyset, PhoneView.class).rows());
        assertEquals(List.of(masked), reactive.query(protectedForm).select("id", "phone").masked()
                .keysetPage(keyset, PhoneView.class).block(WAIT).rows());
        assertEquals(0, sync.query(protectedForm)
                .where("phone", ProtectedConditions.EXACT, "13800001234").fetchMap().size());
        assertEquals(1L, sync.delete(protectedForm).where("id", 1).execute());
    }

    private static Map<String, Object> row(int id, String name, int org) {
        return Map.of("id", id, "name", name, "org_id", org, "deleted", 0);
    }

    @Test
    void repositoryProjectionFlowsDirectlyIntoReactorMapping() {
        clients.syncOperator().dml().insert(USERS, row(1, "Alice", 7));
        var repository = clients.repository(UserEntity.class);
        assertEquals(List.of("Alice"), repository.createQuery().select(UserEntity::getName)
                .where(UserEntity::getOrgId, 7).fetch().map(UserEntity::getName).collectList().block(WAIT));
        assertEquals("Alice", clients.syncRepository(UserEntity.class).createQuery()
                .select(UserEntity::getName).where(UserEntity::getOrgId, 7).fetch().getFirst().getName());
    }

    @com.flying.orm.core.annotation.TableName("users")
    public static class UserEntity {
        private Integer id;
        private String name;
        private Integer orgId;
        public UserEntity() { }
        public Integer getId() { return id; }
        public void setId(Integer id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public Integer getOrgId() { return orgId; }
        public void setOrgId(Integer orgId) { this.orgId = orgId; }
    }

    public record UserView(int id, String name) { }
    public record PhoneView(int id, String phone) { }

    @Test
    void developerSelectedLargePagesWorkThroughJdbcAndR2dbc() {
        List<Map<String, Object>> input = java.util.stream.IntStream.rangeClosed(1, 1201)
                .mapToObj(id -> row(id, "User" + id, 7)).toList();
        clients.syncOperator().dml().insertBatch(USERS, input);
        Supplier<SyncQueryOperator> sync = () -> clients.syncOperator().dml().query(USERS).select("id", "name");
        Supplier<QueryOperator> reactive = () -> clients.operator().dml().query(USERS).select("id", "name");
        PageResult<UserView> page = sync.get().orderByAsc("id").page(1, 1200, UserView.class);
        assertEquals(1200, page.rows().size());
        assertEquals(1201, page.total());
        assertEquals(page, reactive.get().orderByAsc("id").page(1, 1200, UserView.class).block(WAIT));
        CursorPageQuery cursor = CursorPageQuery.first(1200, CursorSort.asc("id"));
        assertEquals(1200, sync.get().cursorPage(cursor, UserView.class).rows().size());
        assertTrue(sync.get().cursorPage(cursor, UserView.class).hasMore());
        assertEquals(sync.get().cursorPage(cursor, UserView.class),
                reactive.get().cursorPage(cursor, UserView.class).block(WAIT));
        KeysetPageQuery keyset = KeysetPageQuery.first(1200, KeysetSort.asc("id", NullOrder.LAST));
        assertEquals(1200, sync.get().keysetPage(keyset, UserView.class).rows().size());
        assertTrue(sync.get().keysetPage(keyset, UserView.class).hasMore());
        assertEquals(sync.get().keysetPage(keyset, UserView.class),
                reactive.get().keysetPage(keyset, UserView.class).block(WAIT));
    }

    @Test
    void typedTerminalsKeepScopeProjectionSortingAndSingleRowCardinality() {
        clients.syncOperator().dml().insertBatch(USERS,
                List.of(row(1, "Alice", 7), row(2, "Bob", 7), row(3, "Other", 8)));
        DataScope scope = DataScope.orgOnly("org_id", 7);
        Supplier<SyncQueryOperator> sync = () -> clients.syncOperator().withDefaultDataScope(scope)
                .dml().query(USERS).select("id", "name");
        Supplier<QueryOperator> reactive = () -> clients.operator().withDefaultDataScope(scope)
                .dml().query(USERS).select("id", "name");
        UserView alice = new UserView(1, "Alice");
        assertEquals(alice, sync.get().where("id", 1).one(UserView.class));
        assertEquals(alice, reactive.get().where("id", 1).one(UserView.class).block(WAIT));
        assertNull(sync.get().where("id", 3).one(UserView.class));
        assertNull(reactive.get().where("id", 3).one(UserView.class).block(WAIT));
        assertThrows(IllegalStateException.class, () -> sync.get().one(UserView.class));
        assertThrows(IndexOutOfBoundsException.class, () -> reactive.get().one(UserView.class).block(WAIT));
        PageResult<UserView> page = sync.get().orderByDesc("id").page(1, 1, UserView.class);
        assertEquals(2, page.total());
        assertEquals(List.of(new UserView(2, "Bob")), page.rows());
        assertEquals(page, reactive.get().orderByDesc("id").page(1, 1, UserView.class).block(WAIT));
        CursorPageQuery cursor = CursorPageQuery.first(1, CursorSort.asc("id"));
        assertEquals(List.of(alice), sync.get().cursorPage(cursor, UserView.class).rows());
        assertEquals(sync.get().cursorPage(cursor, UserView.class),
                reactive.get().cursorPage(cursor, UserView.class).block(WAIT));
        KeysetPageQuery keyset = KeysetPageQuery.first(1, KeysetSort.asc("id", NullOrder.LAST));
        assertEquals(List.of(alice), sync.get().keysetPage(keyset, UserView.class).rows());
        assertEquals(sync.get().keysetPage(keyset, UserView.class),
                reactive.get().keysetPage(keyset, UserView.class).block(WAIT));
    }

    @Test
    void typedSingleRowAlsoWorksForTrustedTables() {
        clients.syncOperator().dml().insert(USERS, row(1, "Alice", 7));
        Supplier<SyncQueryOperator> sync = () -> clients.syncOperator().dml().query()
                .from("users").select("id", "name");
        Supplier<QueryOperator> reactive = () -> clients.operator().dml().query()
                .from("users").select("id", "name");
        assertEquals(new UserView(1, "Alice"), sync.get().one(UserView.class));
        assertEquals(new UserView(1, "Alice"), reactive.get().one(UserView.class).block(WAIT));
        assertNull(sync.get().where("id", 99).one(UserView.class));
        assertNull(reactive.get().where("id", 99).one(UserView.class).block(WAIT));
        clients.syncOperator().dml().insert(USERS, row(2, "Bob", 7));
        assertThrows(IllegalStateException.class, () -> sync.get().one(UserView.class));
        assertThrows(IndexOutOfBoundsException.class, () -> reactive.get().one(UserView.class).block(WAIT));
    }

    @Test
    void entityBusinessConditionsUseTheSameWhereSignatureAcrossCommands() {
        clients.syncOperator().dml().insertBatch(USERS,
                List.of(row(1, "Alice", 7), row(2, "Bob", 7), row(3, "Other", 8)));
        FlyingOrmClients scoped = clients.withDefaultDataScope(DataScope.orgOnly("org_id", 7));
        assertEquals(List.of("Alice", "Bob"), scoped.repository(UserEntity.class).createQuery()
                .where(UserEntity::getOrgId, "same-org", 7)
                .or(group -> group.where(UserEntity::getName, "like", "A%")
                        .where(UserEntity::getName, "=", "Bob"))
                .orderByAsc(UserEntity::getId).fetch().map(UserEntity::getName).collectList().block(WAIT));
        assertEquals(List.of("Alice", "Bob"), scoped.syncRepository(UserEntity.class).createQuery()
                .where(UserEntity::getOrgId, "same-org", 7).orderByAsc(UserEntity::getId)
                .fetch().stream().map(UserEntity::getName).toList());
        assertEquals(1L, scoped.repository(UserEntity.class).createUpdate().set(UserEntity::getName, "changed")
                .where(UserEntity::getId, "in", List.of(1, 3)).execute().block(WAIT));
        assertEquals(1L, scoped.syncRepository(UserEntity.class).createUpdate().set(UserEntity::getName, "changed")
                .where(UserEntity::getId, "in", List.of(2, 3)).execute());
        assertEquals(1L, scoped.repository(UserEntity.class).createDelete()
                .where(UserEntity::getId, "in", List.of(1, 3)).execute().block(WAIT));
        assertEquals(1L, scoped.syncRepository(UserEntity.class).createDelete()
                .where(UserEntity::getId, "in", List.of(2, 3)).execute());
        assertEquals("Other", clients.syncOperator().dml().query(USERS).where("id", 3).one().get("name"));
        assertThrows(IllegalArgumentException.class, () -> scoped.repository(UserEntity.class).createQuery()
                .where(UserEntity::getId, "unknown-condition", 1).fetch().blockLast(WAIT));
    }

    @Test
    void trustedTableSupportsBothClassAndCustomMapping() {
        clients.syncOperator().dml().insert(USERS, row(1, "Alice", 7));
        var expected = List.of(new UserView(1, "Alice"));
        assertAll(
                () -> assertEquals(expected, clients.syncOperator().dml().query().from("users")
                        .select("id", "name").fetch(UserView.class)),
                () -> assertEquals(expected, clients.operator().dml().query().from("users")
                        .select("id", "name").fetch(UserView.class).collectList().block(WAIT)),
                () -> assertEquals(expected, clients.syncOperator().dml().query().from("users")
                        .select("id", "name").fetch(RowMapper.of(UserView.class))),
                () -> assertEquals(expected, clients.operator().dml().query().from("users")
                        .select("id", "name").fetch(RowMapper.of(UserView.class)).collectList().block(WAIT)));
    }

    @Test
    void everyExplicitGovernedTerminalRejectsUndescribedTerms() {
        assertGovernedTermRejected("legacy-eq", IllegalArgumentException.class);
    }

    @Test
    void everyExplicitGovernedTerminalChecksDialectCapabilities() {
        assertGovernedTermRejected("missing-cap", UnsupportedOperationException.class);
    }

    @Test
    void configurationCopiesPreserveExplicitGovernanceWithoutMakingDefaultPoliciesSticky() {
        clients.syncOperator().dml().insert(USERS, row(1, "Alice", 7));
        var limits = QueryShapeLimits.defaults();
        var policy = FieldUsePolicy.unrestricted();
        var sync = clients.syncForms().withQueryGovernance(policy, limits)
                .withDefaultDataScope(DataScope.none())
                .withDefaultExecutionOptions(SqlExecutionOptions.safeDefaults())
                .withDefaultBatchWriteOptions(BatchWriteOptions.defaults())
                .withEntityModelRegistry(clients.syncForms().entityModels())
                .withStructuredConditionResolver(StructuredConditionResolver.defaults())
                .withFieldUsePolicy(policy).withQueryShapeLimits(limits);
        var reactive = clients.forms().withQueryGovernance(policy, limits)
                .withDefaultDataScope(DataScope.none())
                .withDefaultExecutionOptions(SqlExecutionOptions.safeDefaults())
                .withDefaultBatchWriteOptions(BatchWriteOptions.defaults())
                .withEntityModelRegistry(clients.forms().entityModels())
                .withStructuredConditionResolver(StructuredConditionResolver.defaults())
                .withFieldUsePolicy(policy).withQueryShapeLimits(limits);
        var spec = QuerySpec.of(USERS, ConditionGroup.and().where("id", "missing-cap", 1).build());
        var restricted = FieldUsePolicy.builder().allow("id", FieldUse.FILTER).build();
        var trustedSync = clients.syncForms().withFieldUsePolicy(restricted).withFieldUsePolicy(policy)
                .withQueryShapeLimits(limits.withMaxBindCount(1000)).withQueryShapeLimits(limits);
        var trustedReactive = clients.forms().withFieldUsePolicy(restricted).withFieldUsePolicy(policy)
                .withQueryShapeLimits(limits.withMaxBindCount(1000)).withQueryShapeLimits(limits);
        assertAll(
                () -> assertThrows(UnsupportedOperationException.class, () -> sync.select(spec)),
                () -> assertThrows(UnsupportedOperationException.class, () -> reactive.select(spec).blockLast(WAIT)),
                () -> assertEquals(1, trustedSync.select(spec).size()),
                () -> assertEquals(1L, trustedReactive.select(spec).count().block(WAIT)));
    }

    private void assertGovernedTermRejected(String term, Class<? extends Throwable> failure) {
        clients.syncOperator().dml().insert(USERS, row(1, "Alice", 7));
        Supplier<SyncQueryOperator> sync = () -> clients.syncOperator().dml().query()
                .from(USERS, FieldUsePolicy.unrestricted()).where("id", term, 1);
        Supplier<QueryOperator> reactive = () -> clients.operator().dml().query()
                .from(USERS, FieldUsePolicy.unrestricted()).where("id", term, 1);
        var cursor = CursorPageQuery.first(10, CursorSort.asc("id"));
        var keyset = KeysetPageQuery.first(10, KeysetSort.asc("id", NullOrder.LAST));
        var count = AggregateExpression.count("id", "total");
        assertAll(term,
                () -> assertThrows(failure, () -> sync.get().fetchMap()),
                () -> assertThrows(failure, () -> sync.get().fetch(UserView.class)),
                () -> assertThrows(failure, () -> sync.get().fetch(row -> row)),
                () -> assertThrows(failure, () -> sync.get().one()),
                () -> assertThrows(failure, () -> sync.get().one(UserView.class)),
                () -> assertThrows(failure, () -> sync.get().page(1, 10, UserView.class)),
                () -> assertThrows(failure, () -> sync.get().cursorPage(cursor, UserView.class)),
                () -> assertThrows(failure, () -> sync.get().keysetPage(keyset, UserView.class)),
                () -> assertThrows(failure, () -> sync.get().page(1, 10)),
                () -> assertThrows(failure, () -> sync.get().cursorPage(cursor)),
                () -> assertThrows(failure, () -> sync.get().keysetPage(keyset)),
                () -> assertThrows(failure, () -> sync.get().aggregate(a -> a.aggregate(count))),
                () -> assertThrows(failure, () -> reactive.get().fetchMap().blockLast(WAIT)),
                () -> assertThrows(failure, () -> reactive.get().fetch(UserView.class).blockLast(WAIT)),
                () -> assertThrows(failure, () -> reactive.get().fetch(row -> row).blockLast(WAIT)),
                () -> assertThrows(failure, () -> reactive.get().one().block(WAIT)),
                () -> assertThrows(failure, () -> reactive.get().one(UserView.class).block(WAIT)),
                () -> assertThrows(failure, () -> reactive.get().page(1, 10, UserView.class).block(WAIT)),
                () -> assertThrows(failure, () -> reactive.get().cursorPage(cursor, UserView.class).block(WAIT)),
                () -> assertThrows(failure, () -> reactive.get().keysetPage(keyset, UserView.class).block(WAIT)),
                () -> assertThrows(failure, () -> reactive.get().page(1, 10).block(WAIT)),
                () -> assertThrows(failure, () -> reactive.get().cursorPage(cursor).block(WAIT)),
                () -> assertThrows(failure, () -> reactive.get().keysetPage(keyset).block(WAIT)),
                () -> assertThrows(failure, () -> reactive.get().aggregate(a -> a.aggregate(count)).blockLast(WAIT)));
    }

    @Test
    void governedContainsCustomMappingPreservesSqlNulls() {
        var form = DynamicForm.builder("nullable_contacts", "nullable_contacts")
                .addField(DynamicField.primaryKey("id", "INTEGER"))
                .addField(DynamicField.of("secret", "VARCHAR(128)"))
                .addField(DynamicField.of("note", "VARCHAR(128)"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).containsMinLength(3).build()).build();
        clients.syncSchema().createTable(form);
        var values = new LinkedHashMap<String, Object>();
        values.put("id", 1);
        values.put("secret", "alpha beta");
        values.put("note", null);
        var dml = clients.syncOperator().dml();
        dml.insert(form, values);
        RowMapper<String> mapper = row -> (String) row.get("note");
        var limits = QueryShapeLimits.defaults().withMaxBindCount(1000);
        assertEquals(Collections.singletonList(null), dml.query(form)
                .where("secret", ProtectedConditions.CONTAINS, "alpha").fetch(mapper));
        assertEquals(Collections.singletonList(null), dml.query().from(form, FieldUsePolicy.unrestricted(), limits)
                .where("id", 1).fetch(mapper));
        var results = dml.query().from(form, FieldUsePolicy.unrestricted(), limits)
                .where("secret", ProtectedConditions.CONTAINS, "alpha").fetch(mapper);
        assertEquals(Collections.singletonList(null), results);
        assertThrows(UnsupportedOperationException.class, () -> results.add("changed"));
    }
}
