package com.flying.orm.consumer;

import com.flying.orm.core.annotation.*;
import com.flying.orm.core.scope.*;
import com.flying.orm.core.sql.render.*;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.*;
import com.flying.orm.rdb.form.*;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.rdb.lock.OptimisticLockConflictException;
import com.flying.orm.rdb.mapping.*;
import com.flying.orm.rdb.operator.*;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class EntityPatchContractTest {
    @Test
    void entityAssignmentsReuseTheCommandsExistingValuePlan() {
        Fixture f = new Fixture();
        Account entity = account("Ada", Status.DISABLED);
        for (boolean selected : List.of(false, true)) {
            SyncEntityDmlUpdateOperator<Account> sync = f.sync(Account.class);
            EntityDmlUpdateOperator<Account> reactive = f.reactive(Account.class);
            long requests = f.sync.entityModels().stats().requestCount();
            if (selected) {
                sync.set(entity, Account::status);
                reactive.set(entity, Account::status);
            } else {
                sync.set(entity);
                reactive.set(entity);
            }
            assertEquals(requests, f.sync.entityModels().stats().requestCount(),
                    "an existing value plan must not be looked up again for SET");
            sync.where(Account::id, 9L).execute();
            reactive.where(Account::id, 9L).execute().block();
            assertEquals(f.requests.get(f.requests.size() - 2).parameters(), f.last().parameters());
            assertTrue(f.last().parameters().contains("D"));
        }
    }

    @Test
    void entityAssignmentsCannotBypassConfiguredWritePermissions() {
        Fixture f = new Fixture();
        FieldUsePolicy policy = FieldUsePolicy.builder().allow("id", FieldUse.FILTER)
                .allow("name", FieldUse.PROJECT).build();
        SyncFormClient sync = f.sync.withFieldUsePolicy(policy);
        ReactiveFormClient reactive = f.reactive.withFieldUsePolicy(policy);
        Account entity = account("Ada", Status.ACTIVE);
        assertThrows(ScopeAccessException.class, () -> SyncEntityDmlOperator.create(sync, sync.entityRenderer(), Account.class)
                .update().set(entity, Account::name).where(Account::id, 9L).execute());
        assertThrows(ScopeAccessException.class, () -> EntityDmlOperator.create(reactive, reactive.entityRenderer(), Account.class)
                .update().set(entity, Account::name).where(Account::id, 9L).execute().block());
        assertTrue(f.requests.isEmpty());
    }

    @Test
    void selectedEncryptedValueStillUsesTheSharedProtectionPlanner() {
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            Fixture f = new Fixture(protection);
            Secret entity = new Secret(9L, "plaintext");
            f.sync(Secret.class).set(entity, Secret::secret).where(Secret::id, 9L).execute();
            f.reactive(Secret.class).set(entity, Secret::secret).where(Secret::id, 9L).execute().block();
            assertEquals(2, f.requests.size());
            for (SqlRequest request : f.requests) {
                assertFalse(request.parameters().contains("plaintext"));
                assertTrue(request.sql().contains("\"secret\" = ?"), request.sql());
                assertTrue(request.parameters().stream().anyMatch(value -> value instanceof byte[]));
            }
        }
    }

    @Test
    void selectedRecordValuesKeepEnumScopeLogicDeleteAndExplicitVersionRules() {
        Fixture f = new Fixture();
        Account entity = account("Ada", Status.DISABLED);
        assertEquals(1L, f.sync(Account.class).set(entity, Account::name, Account::status)
                .where(Account::id, 9L).optimisticLock(3L).execute());
        SqlRequest expected = f.last();
        String set = expected.sql().split(" where ")[0];
        assertTrue(set.contains("\"status_code\" = ?"), set);
        assertFalse(set.contains("\"id\""), set);
        assertFalse(set.contains("\"deleted\""), set);
        assertTrue(expected.parameters().containsAll(List.of("Ada", "D", 9L, 3L, 7L, 0)));
        Mono<Long> result = f.reactive(Account.class).set(entity, Account::name, Account::status)
                .where(Account::id, 9L).optimisticLock(3L).execute();
        assertEquals(1, f.requests.size(), "SQL remains lazy");
        assertEquals(1L, result.block());
        assertEquals(expected.sql(), f.last().sql());
        assertEquals(expected.parameters(), f.last().parameters());
        f.updated = 0;
        assertThrows(OptimisticLockConflictException.class, result::block);
    }

    @Test
    void wholeEntityOmitsSpecialColumnsAndHonorsNullStrategies() {
        Fixture f = new Fixture();
        Account entity = account(null, Status.ACTIVE);
        f.sync(Account.class).set(entity).where(Account::id, 9L).execute();
        SqlRequest expected = f.last();
        String set = expected.sql().split(" where ")[0];
        assertTrue(set.contains("\"name\" = ?"), set);
        for (String field : List.of("id", "version", "deleted", "optional", "empty", "never")) {
            assertFalse(set.contains("\"" + field + "\""), set);
        }
        assertTrue(expected.parameters().contains(new SqlNullParameter(String.class)));
        assertTrue(expected.parameters().contains("A"));
        f.reactive(Account.class).set(entity).where(Account::id, 9L).execute().block();
        assertEquals(expected.sql(), f.last().sql());
        assertEquals(expected.parameters(), f.last().parameters());
    }

    @Test
    void selectedBeanReadsOnlyChosenGetterAndFillerOnce() {
        Fixture f = new Fixture();
        Bean bean = new Bean();
        SyncEntityDmlUpdateOperator<Bean> update = f.sync(Bean.class)
                .set(bean, Bean::getName, Bean::getName).where(Bean::getId, 9L);
        assertEquals(1, bean.nameReads);
        assertEquals(List.of("name"), f.filled);
        bean.name = "changed";
        update.execute();
        assertTrue(f.last().parameters().contains("Ada-filled"));
        f.reactive(Bean.class).set(bean, Bean::getName).where(Bean::getId, 9L).execute().block();
        assertEquals(2, bean.nameReads);
        assertEquals(List.of("name", "name"), f.filled);
        assertTrue(f.last().parameters().contains("changed-filled"));
    }

    @Test
    void invalidSelectionDoesNotLeavePartialAssignments() {
        Fixture f = new Fixture();
        Account entity = account("from-entity", Status.ACTIVE);
        SyncEntityDmlUpdateOperator<Account> update = f.sync(Account.class)
                .set(Account::name, "explicit").where(Account::id, 9L);
        assertThrows(MappingException.class, () -> update.set(entity, Account::name, Account::id));
        assertThrows(MappingException.class, () -> update.set(entity, Account::version));
        assertThrows(MappingException.class, () -> update.set(entity, Account::deleted));
        assertThrows(MappingException.class, () -> update.set(entity, Account::never));
        assertTrue(f.requests.isEmpty());
        update.execute();
        assertTrue(f.last().parameters().contains("explicit"));
        assertFalse(f.last().parameters().contains("from-entity"));
        assertThrows(MappingException.class,
                () -> f.reactive(Account.class).set(entity, Account::name, Account::id));
    }

    @Test
    void assignmentsComposeWithExplicitNullAndIncrementWithoutRetainingEntity() {
        Fixture f = new Fixture();
        Account entity = account("from-entity", Status.ACTIVE);
        EntityDmlUpdateOperator<Account> update = f.reactive(Account.class)
                .set(Account::name, "old")
                .set(entity, Account::name, Account::status)
                .set(Account::name, null).increment(Account::score, 2)
                .where(Account::id, 9L);
        Mono<Long> result = update.execute();
        update.set(Account::name, "later");
        result.block();
        assertTrue(f.last().parameters().contains(new SqlNullParameter(String.class)));
        assertFalse(f.last().parameters().contains("later"));
        assertFalse(f.last().parameters().contains("from-entity"));
        assertTrue(f.last().parameters().contains("A"));
        assertTrue(f.last().sql().contains("\"score\" + ?"), f.last().sql());
    }

    @Test
    void strategySkippedFieldDoesNotEraseEarlierExplicitAssignment() {
        Fixture f = new Fixture();
        Account entity = account("Ada", Status.ACTIVE);
        f.sync(Account.class).set(Account::optional, "keep")
                .set(entity, Account::optional).where(Account::id, 9L).execute();
        assertTrue(f.last().parameters().contains("keep"));
    }

    private static Account account(String name, Status status) {
        return new Account(9L, name, status, 3L, 0, null, "", "unused", 10, 7L);
    }

    @TableName("accounts")
    public record Account(@TableId(type = IdType.INPUT) Long id, String name,
                          @TableField("status_code") Status status, @Version Long version,
                          @TableLogic Integer deleted,
                          @TableField(updateStrategy = FieldStrategy.NOT_NULL) String optional,
                          @TableField(updateStrategy = FieldStrategy.NOT_EMPTY) String empty,
                          @TableField(updateStrategy = FieldStrategy.NEVER) String never,
                          Integer score, Long tenant) { }
    public enum Status {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        Status(String code) { this.code = code; }
    }
    @TableName("secrets")
    public record Secret(@TableId(type = IdType.INPUT) Long id, @EncryptedField String secret) { }
    @TableName("beans")
    public static class Bean {
        @TableId(type = IdType.INPUT) private Long id = 9L;
        @TableField(fill = FieldFill.UPDATE) private String name = "Ada";
        @TableField(fill = FieldFill.UPDATE) private String other;
        @TableField(exist = false) int nameReads;
        public Long getId() { throw new AssertionError("not selected"); }
        public String getName() { nameReads++; return name; }
        public String getOther() { throw new AssertionError("not selected"); }
    }

    private static class Fixture {
        final List<SqlRequest> requests = new ArrayList<>();
        final List<String> filled = new ArrayList<>();
        long updated = 1;
        final SyncFormClient sync;
        final ReactiveFormClient reactive;
        Fixture() { this(null); }
        Fixture(ProtectedFieldRuntime protection) {
            EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults(),
                    IdGenerator.none(), (entity, field, operation, value) -> {
                        filled.add(field.name()); return value + "-filled";
                    });
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
            if (protection != null) renderer = renderer.withProtectedFields(protection);
            sync = SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) { throw new AssertionError("no implicit SELECT"); }
                public long rowsUpdated(SqlRequest request) { requests.add(request); return updated; }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) { throw new AssertionError(); }
            }, (request, completed) -> { throw new AssertionError(); }, renderer).withEntityModelRegistry(models);
            reactive = ReactiveFormClient.create(new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) { throw new AssertionError("no implicit SELECT"); }
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.fromSupplier(() -> { requests.add(request); return updated; });
                }
            }, renderer).withEntityModelRegistry(models);
        }
        <T> SyncEntityDmlUpdateOperator<T> sync(Class<T> type) {
            SyncFormClient client = type == Account.class ? sync.withDefaultDataScope(DataScope.tenant("tenant", 7L)) : sync;
            return SyncEntityDmlOperator.create(client, client.entityRenderer(), type).update();
        }
        <T> EntityDmlUpdateOperator<T> reactive(Class<T> type) {
            ReactiveFormClient client = type == Account.class ? reactive.withDefaultDataScope(DataScope.tenant("tenant", 7L)) : reactive;
            return EntityDmlOperator.create(client, client.entityRenderer(), type).update();
        }
        SqlRequest last() { return requests.getLast(); }
    }
}
