package com.flying.orm.consumer;

import com.flying.orm.core.annotation.*;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.*;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.*;
import com.flying.orm.rdb.form.*;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lock.OptimisticLockConflictException;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.repository.*;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class EntityKeyContractTest {
    @Test
    void multipleScalarIdsUseOneInQueryAndOneDeleteOnBothRepositories() {
        Fixture<Plain> f = new Fixture<>(Plain.class);
        List<Object> keys = List.of(9L, Map.of("id", 10L), 9L);
        List<Plain> rows = f.sync.findByIds(keys);
        assertEquals(List.of(new Plain(9L, "Ada")), rows);
        SqlRequest query = f.last();
        assertTrue(query.sql().contains("\"id\" in (?, ?, ?)"), query.sql());
        assertEquals(List.of(9L, 10L, 9L), query.parameters());
        assertEquals(rows, f.reactive.findByIds(keys).collectList().block());
        assertEquals(query.sql(), f.last().sql());
        assertEquals(query.parameters(), f.last().parameters());
        assertEquals(2, f.requests.size());
        assertEquals(1L, f.sync.deleteByIds(keys));
        SqlRequest delete = f.last();
        assertTrue(delete.sql().startsWith("delete "), delete.sql());
        assertEquals(List.of(9L, 10L, 9L), delete.parameters());
        assertEquals(1L, f.reactive.deleteByIds(keys).block());
        assertEquals(delete.sql(), f.last().sql());
        assertEquals(delete.parameters(), f.last().parameters());
        assertEquals(4, f.requests.size());
    }

    @Test
    void singleEnumKeysUsePersistentValuesInsideInAndKeepThePublishedSnapshot() {
        Fixture<EnumKey> f = new Fixture<>(EnumKey.class);
        List<Object> keys = new ArrayList<>(List.of(Code.ACTIVE, Map.of("code", Code.DISABLED)));
        Flux<EnumKey> query = f.reactive.findByIds(keys);
        Mono<Long> delete = f.reactive.deleteByIds(keys);
        assertTrue(f.requests.isEmpty());
        keys.clear();
        assertEquals(Code.ACTIVE, query.single().block().code());
        assertEquals(List.of("A", "D"), f.last().parameters());
        assertTrue(f.last().sql().contains("\"code_key\" in (?, ?)"), f.last().sql());
        assertEquals(1L, delete.block());
        assertEquals(List.of("A", "D"), f.last().parameters());
        assertEquals(Code.ACTIVE, f.sync.findByIds(List.of(Code.ACTIVE, Code.DISABLED)).getFirst().code());
        assertEquals(List.of("A", "D"), f.last().parameters());
        assertEquals(1L, f.sync.deleteByIds(List.of(Code.ACTIVE, Code.DISABLED)));
        assertEquals(List.of("A", "D"), f.last().parameters());
    }

    @Test
    void multipleKeysDoNotExpandHiddenKeyProjection() {
        Fixture<HiddenKey> f = new Fixture<>(HiddenKey.class);
        HiddenKey row = f.sync.findByIds(List.of(9L, 10L)).getFirst();
        assertEquals("Ada", row.name());
        assertNull(row.id());
        assertTrue(f.last().sql().startsWith("select \"name\" from"), f.last().sql());
        assertEquals(List.of(row), f.reactive.findByIds(List.of(9L, 10L)).collectList().block());
    }

    @Test
    void compositeKeyCollectionsKeepPairingEnumMappingScopeAndLoadEvents() {
        Fixture<Member> f = new Fixture<>(Member.class);
        List<Map<String, Object>> keys = List.of(Map.of("orgId", 7L, "code_key", Code.ACTIVE),
                Map.of("org_key", 8L, "code", Code.DISABLED));
        List<EntityLifecyclePhase> events = new ArrayList<>();
        List<Member> rows = f.sync.withListener(event -> {
            events.add(event.phase()); return Mono.empty();
        }).findByIds(keys);
        SqlRequest query = f.last();
        assertTrue(query.sql().contains("(\"org_key\" = ? and \"code_key\" = ?) or (\"org_key\" = ? and \"code_key\" = ?)"), query.sql());
        assertTrue(query.parameters().containsAll(List.of(7L, "A", 8L, "D", 0)), query.parameters().toString());
        assertEquals(rows, f.reactive.withListener(event -> {
            events.add(event.phase()); return Mono.empty();
        }).findByIds(keys).collectList().block());
        assertEquals(query.sql(), f.last().sql());
        assertEquals(query.parameters(), f.last().parameters());
        assertEquals(List.of(EntityLifecyclePhase.POST_LOAD, EntityLifecyclePhase.POST_LOAD), events);
        assertEquals(2, f.requests.size());
    }

    @Test
    void compositeKeyCollectionDeleteKeepsLogicalDeleteAndDoesNotInventVersionOrEvents() {
        Fixture<Member> f = new Fixture<>(Member.class);
        List<Map<String, Object>> keys = List.of(Map.of("orgId", 7L, "code", Code.ACTIVE),
                Map.of("orgId", 7L, "code", Code.DISABLED));
        assertEquals(1L, f.sync.withListener(event -> { throw new AssertionError("no entity"); }).deleteByIds(keys));
        SqlRequest delete = f.last();
        assertTrue(delete.sql().startsWith("update "), delete.sql());
        assertTrue(delete.sql().contains(" or "), delete.sql());
        assertFalse(delete.sql().contains("\"version\""), delete.sql());
        assertTrue(delete.parameters().containsAll(List.of("A", "D", 7L, 0, 1)), delete.parameters().toString());
        assertEquals(1L, f.reactive.withListener(event -> { throw new AssertionError("no entity"); }).deleteByIds(keys).block());
        assertEquals(delete.sql(), f.last().sql());
        assertEquals(delete.parameters(), f.last().parameters());
        assertEquals(2, f.requests.size());
    }

    @Test
    void keyCollectionSnapshotIsLazyAndStableAcrossSubscriptions() {
        Fixture<Member> f = new Fixture<>(Member.class);
        Map<String, Object> key = new HashMap<>(Map.of("orgId", 7L, "code", Code.ACTIVE));
        List<Object> keys = new ArrayList<>(List.of(key));
        Flux<Member> query = f.reactive.findByIds(keys);
        Mono<Long> delete = f.reactive.deleteByIds(keys);
        key.put("code", Code.DISABLED);
        keys.clear();
        assertTrue(f.requests.isEmpty());
        for (int repeat = 0; repeat < 2; repeat++) {
            query.collectList().block();
            assertTrue(f.last().parameters().contains("A"));
            assertFalse(f.last().parameters().contains("D"));
            assertEquals(1L, delete.block());
            assertTrue(f.last().parameters().contains("A"));
            assertFalse(f.last().parameters().contains("D"));
        }
        assertEquals(4, f.requests.size());
    }

    @Test
    void emptyKeyCollectionsDoNotRunSqlEvenForModelsWithoutKeys() {
        Fixture<NoKey> f = new Fixture<>(NoKey.class);
        assertEquals(List.of(), f.sync.findByIds(List.of()));
        assertEquals(0L, f.sync.deleteByIds(List.of()));
        assertEquals(List.of(), f.reactive.findByIds(List.of()).collectList().block());
        assertEquals(0L, f.reactive.deleteByIds(List.of()).block());
        assertTrue(f.requests.isEmpty());
        assertThrows(IllegalStateException.class, () -> f.sync.findByIds(List.of(1L)));
        assertThrows(IllegalStateException.class, () -> f.reactive.deleteByIds(List.of(1L)));
    }

    @Test
    void invalidKeyInCollectionRejectsTheWholeOperationBeforeAnySql() {
        Fixture<Member> f = new Fixture<>(Member.class);
        Object valid = Map.of("orgId", 7L, "code", Code.ACTIVE);
        for (Object invalid : List.of(7L, Map.of("orgId", 7L), Map.of("orgId", 7L, "org_key", 8L))) {
            List<Object> keys = List.of(valid, invalid);
            assertThrows(IllegalArgumentException.class, () -> f.sync.findByIds(keys));
            assertThrows(IllegalArgumentException.class, () -> f.sync.deleteByIds(keys));
            assertThrows(IllegalArgumentException.class, () -> f.reactive.findByIds(keys));
            assertThrows(IllegalArgumentException.class, () -> f.reactive.deleteByIds(keys));
        }
        Fixture<Plain> single = new Fixture<>(Plain.class);
        Map<String, Object> nullKey = new HashMap<>();
        nullKey.put("id", null);
        assertThrows(IllegalArgumentException.class, () -> single.sync.deleteByIds(List.of(9L, nullKey)));
        assertThrows(NullPointerException.class, () -> single.reactive.findByIds(Arrays.asList(9L, null)));
        assertTrue(f.requests.isEmpty());
        assertTrue(single.requests.isEmpty());
    }

    @Test
    void syncExplicitNullUpdateDoesNotBecomeAnIdentityWrite() {
        assertExplicitNullConditionRejected(false, true);
    }

    @Test
    void syncExplicitNullDeleteDoesNotBecomeAnIdentityWrite() {
        assertExplicitNullConditionRejected(false, false);
    }

    @Test
    void reactiveExplicitNullUpdateDoesNotBecomeAnIdentityWrite() {
        assertExplicitNullConditionRejected(true, true);
    }

    @Test
    void reactiveExplicitNullDeleteDoesNotBecomeAnIdentityWrite() {
        assertExplicitNullConditionRejected(true, false);
    }

    private void assertExplicitNullConditionRejected(boolean reactive, boolean update) {
        Fixture<Plain> f = new Fixture<>(Plain.class);
        Plain entity = new Plain(9L, "Ada");
        List<EntityLifecyclePhase> events = new ArrayList<>();
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> {
                    if (reactive) {
                        ReactiveFormRepository<Plain> repository = f.reactive.withListener(event -> {
                            events.add(event.phase()); return Mono.empty();
                        });
                        (update ? repository.update(entity, null) : repository.delete(entity, null)).block();
                    } else {
                        SyncFormRepository<Plain> repository = f.sync.withListener(event -> {
                            events.add(event.phase()); return Mono.empty();
                        });
                        if (update) repository.update(entity, null);
                        else repository.delete(entity, null);
                    }
                }),
                () -> assertTrue(f.requests.isEmpty(), "explicit null must not execute SQL"),
                () -> assertTrue(events.isEmpty(), "invalid condition must not start entity events"));
    }

    @Test
    void hiddenPrimaryKeyStillLocatesRowsWithoutExpandingDefaultProjection() {
        Fixture<HiddenKey> f = new Fixture<>(HiddenKey.class);
        HiddenKey row = f.sync.findById(9L);
        assertEquals("Ada", row.name());
        assertNull(row.id());
        String sql = f.last().sql();
        assertTrue(sql.startsWith("select \"name\" from"), sql);
        assertTrue(sql.contains("\"id\" = ?"), sql);
        assertEquals(row, f.reactive.findById(9L).block());
    }

    @Test
    void modelsWithoutKeysAndEntitiesWithMissingKeysCannotPerformIdentityOperations() {
        Fixture<NoKey> noKey = new Fixture<>(NoKey.class);
        assertThrows(IllegalStateException.class, () -> noKey.sync.findById(9L));
        assertThrows(IllegalStateException.class, () -> noKey.reactive.update(new NoKey("Ada")).block());
        assertTrue(noKey.requests.isEmpty());
        Fixture<Member> f = new Fixture<>(Member.class);
        Member incomplete = new Member(7L, null, "Ada", 3L, 0);
        assertThrows(IllegalArgumentException.class, () -> f.sync.update(incomplete));
        assertThrows(IllegalArgumentException.class, () -> f.reactive.delete(incomplete).block());
        assertTrue(f.requests.isEmpty());
    }

    @Test
    void reactiveEntityIdentityIsReadAfterPreAndOnlyWhenSubscribed() {
        Fixture<Mutable> f = new Fixture<>(Mutable.class);
        Mutable entity = new Mutable();
        List<EntityLifecyclePhase> events = new ArrayList<>();
        Mono<Long> result = f.reactive.withListener(event -> Mono.fromRunnable(() -> {
            events.add(event.phase());
            if (event.phase() == EntityLifecyclePhase.PRE_UPDATE) entity.id = 11L;
        })).update(entity);
        assertEquals(9L, entity.id);
        assertTrue(events.isEmpty());
        assertTrue(f.requests.isEmpty());
        result.block();
        assertTrue(f.last().parameters().contains(11L));
        assertFalse(f.last().parameters().contains(9L));
        assertEquals(List.of(EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE), events);
    }

    @Test
    void deletionReadsOnlyIdentityAndVersionNotUnrelatedEntityGetters() {
        Fixture<Mutable> f = new Fixture<>(Mutable.class);
        Mutable entity = new Mutable();
        entity.rejectNameRead = true;
        assertEquals(1L, f.sync.delete(entity));
        assertEquals(1L, f.reactive.delete(entity).block());
        assertEquals(2, f.requests.size());
    }

    @Test
    void optimisticConflictsDoNotFireSuccessfulPostEvents() {
        Fixture<Member> f = new Fixture<>(Member.class);
        f.updated = 0;
        Member entity = new Member(7L, Code.ACTIVE, "Ada", 3L, 0);
        List<EntityLifecyclePhase> events = new ArrayList<>();
        assertThrows(OptimisticLockConflictException.class, () -> f.sync.withListener(event -> {
            events.add(event.phase()); return Mono.empty();
        }).update(entity));
        assertThrows(OptimisticLockConflictException.class, () -> f.reactive.withListener(event -> {
            events.add(event.phase()); return Mono.empty();
        }).delete(entity).block());
        assertEquals(List.of(EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.PRE_REMOVE), events);
    }

    @Test
    void lateQueryErrorsAreNotHiddenByOneResult() {
        Fixture<Plain> f = new Fixture<>(Plain.class);
        IllegalStateException failure = new IllegalStateException("late query failure");
        f.queryError = failure;
        List<EntityLifecyclePhase> events = new ArrayList<>();
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> f.reactive.withListener(event -> {
                    events.add(event.phase()); return Mono.empty();
                }).findById(9L).block()));
        assertTrue(events.isEmpty());
    }

    @Test
    void completeCompositeKeysUseAliasesEnumMappingScopeAndLoadEvents() {
        Fixture<Member> f = new Fixture<>(Member.class);
        List<EntityLifecyclePhase> events = new ArrayList<>();
        SyncFormRepository<Member> sync = f.sync.withListener(event -> {
            events.add(event.phase()); return Mono.empty();
        });
        ReactiveFormRepository<Member> reactive = f.reactive.withListener(event -> {
            events.add(event.phase()); return Mono.empty();
        });
        Member row = sync.findById(Map.of("orgId", 7L, "code_key", Code.ACTIVE));
        assertEquals(Code.ACTIVE, row.code());
        SqlRequest expected = f.last();
        assertTrue(expected.sql().contains("\"org_key\" = ?"), expected.sql());
        assertTrue(expected.sql().contains("\"code_key\" = ?"), expected.sql());
        assertTrue(expected.parameters().containsAll(List.of(7L, "A", 0)));
        assertEquals(row, reactive.findById(Map.of("org_key", 7L, "code", Code.ACTIVE)).block());
        assertEquals(expected.sql(), f.last().sql());
        assertEquals(expected.parameters(), f.last().parameters());
        assertEquals(List.of(EntityLifecyclePhase.POST_LOAD, EntityLifecyclePhase.POST_LOAD), events);
    }

    @Test
    void malformedKeysNeverReachTheExecutor() {
        Fixture<Member> f = new Fixture<>(Member.class);
        for (Object key : List.of(7L, Map.of(), Map.of("orgId", 7L),
                Map.of("orgId", 7L, "code", Code.ACTIVE, "name", "Ada"),
                Map.of("orgId", 7L, "org_key", 7L, "code", Code.ACTIVE))) {
            assertThrows(IllegalArgumentException.class, () -> f.sync.findById(key));
            assertThrows(IllegalArgumentException.class, () -> f.reactive.deleteById(key).block());
        }
        Map<String, Object> nullKey = new HashMap<>();
        nullKey.put("orgId", 7L); nullKey.put("code", null);
        assertThrows(IllegalArgumentException.class, () -> f.sync.deleteById(nullKey));
        assertThrows(IllegalArgumentException.class, () -> f.reactive.findById(nullKey).block());
        assertTrue(f.requests.isEmpty());
    }

    @Test
    void entityUpdateAndDeleteUseAllKeysAndVersionWithoutWritingKeys() {
        Fixture<Member> f = new Fixture<>(Member.class);
        Member entity = new Member(7L, Code.ACTIVE, "Ada", 3L, 0);
        List<EntityLifecyclePhase> events = new ArrayList<>();
        SyncFormRepository<Member> sync = f.sync.withListener(event -> {
            events.add(event.phase()); return Mono.empty();
        });
        ReactiveFormRepository<Member> reactive = f.reactive.withListener(event -> {
            events.add(event.phase()); return Mono.empty();
        });
        assertEquals(1L, sync.update(entity));
        SqlRequest expected = f.last();
        String set = expected.sql().split(" where ")[0];
        assertFalse(set.contains("\"org_key\""), set);
        assertFalse(set.contains("\"code_key\""), set);
        assertTrue(expected.parameters().containsAll(List.of("Ada", 7L, "A", 3L)));
        assertEquals(1L, reactive.update(entity).block());
        assertEquals(expected.sql(), f.last().sql());
        assertEquals(expected.parameters(), f.last().parameters());
        assertEquals(1L, sync.delete(entity));
        expected = f.last();
        assertTrue(expected.sql().startsWith("update "), expected.sql());
        assertTrue(expected.parameters().containsAll(List.of(7L, "A", 3L)));
        assertEquals(1L, reactive.delete(entity).block());
        assertEquals(expected.sql(), f.last().sql());
        assertEquals(List.of(EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE,
                EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE,
                EntityLifecyclePhase.PRE_REMOVE, EntityLifecyclePhase.POST_REMOVE,
                EntityLifecyclePhase.PRE_REMOVE, EntityLifecyclePhase.POST_REMOVE), events);
        assertEquals(4, f.requests.size());
    }

    @Test
    void idOnlyDeleteUsesLogicalDeleteButDoesNotFabricateEntityEventsOrVersion() {
        Fixture<Member> f = new Fixture<>(Member.class);
        assertEquals(1L, f.sync.withListener(event -> { throw new AssertionError("no entity"); })
                .deleteById(Map.of("orgId", 7L, "code", Code.ACTIVE)));
        SqlRequest expected = f.last();
        assertTrue(expected.sql().startsWith("update "));
        assertFalse(expected.sql().contains("\"version\""));
        assertEquals(1L, f.reactive.withListener(event -> { throw new AssertionError("no entity"); })
                .deleteById(Map.of("orgId", 7L, "code", Code.ACTIVE)).block());
        assertEquals(expected.sql(), f.last().sql());
    }

    @Test
    void singleIdAndPlainEntityOperationsNeedNoVersion() {
        Fixture<Plain> f = new Fixture<>(Plain.class);
        assertEquals(9L, f.sync.findById(9L).id());
        assertEquals(9L, f.reactive.findById(9L).block().id());
        assertEquals(1L, f.sync.update(new Plain(9L, "Ada")));
        assertEquals(1L, f.reactive.update(new Plain(9L, "Ada")).block());
        assertEquals(1L, f.sync.delete(new Plain(9L, "Ada")));
        assertEquals(1L, f.reactive.deleteById(9L).block());
        assertTrue(f.last().sql().startsWith("delete "));
    }

    @Test
    void publishedKeyQueriesAreLazyAndKeepTheirKeySnapshot() {
        Fixture<Member> f = new Fixture<>(Member.class);
        Map<String, Object> key = new HashMap<>(Map.of("orgId", 7L, "code", Code.ACTIVE));
        Mono<Member> query = f.reactive.findById(key);
        key.put("code", Code.DISABLED);
        assertTrue(f.requests.isEmpty());
        query.block();
        assertTrue(f.last().parameters().contains("A"));
        assertFalse(f.last().parameters().contains("D"));
        f.resultRows = List.of();
        assertNull(f.sync.findById(Map.of("orgId", 7L, "code", Code.ACTIVE)));
        assertNull(query.block());
        f.resultRows = List.of(f.row, f.row);
        assertThrows(RuntimeException.class, () -> f.sync.findById(key));
        assertThrows(RuntimeException.class, query::block);
    }

    @TableName("members")
    public record Member(@TableId(type = IdType.INPUT) @TableField("org_key") Long orgId,
                         @TableId(type = IdType.INPUT) @TableField("code_key") Code code,
                         String name, @Version Long version, @TableLogic Integer deleted) { }
    @TableName("plain")
    public record Plain(@TableId(type = IdType.INPUT) Long id, String name) { }
    @TableName("enum_key")
    public record EnumKey(@TableId(type = IdType.INPUT) @TableField("code_key") Code code) { }
    @TableName("hidden")
    public record HiddenKey(@TableId(type = IdType.INPUT) @TableField(select = false) Long id, String name) { }
    @TableName("no_key")
    public record NoKey(String name) { }
    @TableName("mutable")
    public static class Mutable {
        @TableId(type = IdType.INPUT) private Long id = 9L;
        private String name = "Ada";
        @TableField(exist = false) boolean rejectNameRead;
        public Long getId() { return id; }
        public String getName() {
            if (rejectNameRead) throw new AssertionError("unrelated getter must not be read for deletion");
            return name;
        }
    }
    public enum Code {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        Code(String code) { this.code = code; }
    }

    private static class Fixture<T> {
        final List<SqlRequest> requests = new ArrayList<>();
        final DynamicRow row;
        List<DynamicRow> resultRows;
        long updated = 1;
        RuntimeException queryError;
        final SyncFormRepository<T> sync;
        final ReactiveFormRepository<T> reactive;
        Fixture(Class<T> type) {
            row = DynamicRow.copyOf(type == Plain.class ? Map.of("id", 9L, "name", "Ada")
                    : Map.of("org_key", 7L, "code_key", "A", "name", "Ada", "version", 3L, "deleted", 0));
            resultRows = List.of(row);
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
            SyncFormClient jdbc = SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) { requests.add(request); return resultRows; }
                public long rowsUpdated(SqlRequest request) { requests.add(request); return updated; }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) { throw new AssertionError(); }
            }, (request, completed) -> { throw new AssertionError(); }, renderer);
            ReactiveFormClient r2dbc = ReactiveFormClient.create(new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.defer(() -> {
                        requests.add(request);
                        Flux<DynamicRow> rows = Flux.fromIterable(resultRows);
                        return queryError == null ? rows : rows.concatWith(Flux.error(queryError));
                    });
                }
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.fromSupplier(() -> { requests.add(request); return updated; });
                }
            }, renderer);
            if (type == Member.class) {
                jdbc = jdbc.withDefaultDataScope(DataScope.tenant("org_key", 7L));
                r2dbc = r2dbc.withDefaultDataScope(DataScope.tenant("org_key", 7L));
            }
            sync = SyncFormRepository.create(jdbc, jdbc.entityModels().metadata(type).toDynamicForm(), type);
            reactive = ReactiveFormRepository.create(r2dbc, r2dbc.entityModels().metadata(type).toDynamicForm(), type);
        }
        SqlRequest last() { return requests.getLast(); }
    }
}
