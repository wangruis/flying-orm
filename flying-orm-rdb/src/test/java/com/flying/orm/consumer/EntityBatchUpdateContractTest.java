package com.flying.orm.consumer;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.repository.BatchEvidenceFixtures;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityBatchUpdateContractTest {
    @Test
    void ordinarySyncBatchUsesAllKeysAndMappedValuesWithoutVersionRequirements() {
        Fixture fixture = new Fixture();
        SyncFormRepository<Member> repository = fixture.sync(Member.class);
        repository.updateBatch(List.of(new Member(7L, 1L, State.ACTIVE), new Member(7L, 2L, State.DISABLED)));
        ordinaryRequest(fixture);
    }

    @Test
    void ordinaryReactiveBatchIsLazyAndSharesTheSameMapping() {
        Fixture fixture = new Fixture();
        Mono<BatchExecutionEvidence> batch = fixture.reactive(Member.class)
                .updateBatch(Flux.just(new Member(7L, 1L, State.ACTIVE), new Member(7L, 2L, State.DISABLED)));
        assertNull(fixture.request);
        batch.block();
        ordinaryRequest(fixture);
    }

    @Test
    void ordinaryBatchesRetainConfiguredScopesInBothModes() {
        Fixture fixture = new Fixture();
        Member member = new Member(7L, 1L, State.ACTIVE);
        DataScope scope = DataScope.tenant("tenant_id", 7L);
        fixture.sync(Member.class).updateBatch(Flux.just(member), scope, BatchWriteOptions.of(5));
        String syncSql = fixture.request.sql();
        List<Object> syncRow = fixture.rows.getFirst();
        assertTrue(syncSql.contains("\"tenant_id\" = ?"), syncSql);
        assertEquals(List.of("A", 7L, 1L, 7L), syncRow);
        fixture.reactive(Member.class).updateBatch(Flux.just(member), scope, BatchWriteOptions.of(5)).block();
        assertEquals(syncSql, fixture.request.sql());
        assertEquals(syncRow, fixture.rows.getFirst());
    }

    @Test
    void ordinaryUpdateCallbacksRunForEachCompletedInput() {
        Fixture fixture = new Fixture();
        List<EntityLifecyclePhase> events = new ArrayList<>();
        fixture.sync(Member.class).withListener(event -> {
            events.add(event.phase());
            return Mono.empty();
        }).updateBatch(List.of(new Member(7L, 1L, State.ACTIVE)));
        assertEquals(List.of(EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE), events);
        events.clear();
        fixture.reactive(Member.class).withListener(event -> {
            events.add(event.phase());
            return Mono.empty();
        }).updateBatch(List.of(new Member(7L, 1L, State.ACTIVE))).block();
        assertEquals(List.of(EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE), events);
    }

    @Test
    void ordinaryBatchRejectsAMissingCompositeKeyBeforeSendingSql() {
        Fixture fixture = new Fixture();
        Member invalid = new Member(7L, null, State.ACTIVE);
        assertThrows(IllegalArgumentException.class, () -> fixture.sync(Member.class).updateBatch(List.of(invalid)));
        assertNull(fixture.request);
        assertThrows(IllegalArgumentException.class, () -> fixture.reactive(Member.class).updateBatch(List.of(invalid)).block());
        assertNull(fixture.request);
    }

    @Test
    void declaredVersionStillRequiresStrictCountsAndExpectedVersion() {
        Fixture fixture = new Fixture();
        VersionedMember member = new VersionedMember();
        member.id = 1L;
        member.version = 3L;
        member.name = "Ada";
        fixture.sync(VersionedMember.class).updateBatch(List.of(member));
        assertEquals(BatchRowCountPolicy.EXACTLY_ONE, fixture.request.rowCountPolicy());
        assertTrue(fixture.request.sql().contains("\"version\" = \"version\" + 1"));
        assertEquals(List.of("Ada", 1L, 3L), fixture.rows.getFirst());
        fixture.reactive(VersionedMember.class).updateBatch(List.of(member)).block();
        assertEquals(BatchRowCountPolicy.EXACTLY_ONE, fixture.request.rowCountPolicy());
        member.version = null;
        assertThrows(RuntimeException.class, () -> fixture.sync(VersionedMember.class).updateBatch(List.of(member)));
        assertThrows(RuntimeException.class, () -> fixture.reactive(VersionedMember.class).updateBatch(List.of(member)).block());
    }

    private static void ordinaryRequest(Fixture fixture) {
        assertNotNull(fixture.request);
        assertEquals(BatchRowCountPolicy.ANY, fixture.request.rowCountPolicy());
        assertEquals("update \"members\" set \"state\" = ? where \"tenant_id\" = ? and \"user_id\" = ?", fixture.request.sql());
        assertEquals(List.of(List.of("A", 7L, 1L), List.of("D", 7L, 2L)), fixture.rows);
        assertFalse(fixture.request.sql().contains("version"));
    }

    @TableName("members")
    public static final class Member {
        @TableId(type = IdType.INPUT) private Long tenantId;
        @TableId(type = IdType.INPUT) private Long userId;
        private State state;
        public Member() { }
        Member(Long tenantId, Long userId, State state) {
            this.tenantId = tenantId;
            this.userId = userId;
            this.state = state;
        }
    }

    @TableName("members")
    public static final class VersionedMember {
        @TableId(type = IdType.INPUT) private Long id;
        @Version private Long version;
        private String name;
    }

    public enum State {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        State(String code) { this.code = code; }
    }

    private static final class Fixture {
        private BatchWriteRequest request;
        private final List<List<Object>> rows = new ArrayList<>();
        private final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());

        private <T> SyncFormRepository<T> sync(Class<T> type) {
            SyncSqlExecutor sql = new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest value) { throw new UnsupportedOperationException(); }
                public long rowsUpdated(SqlRequest value) { throw new UnsupportedOperationException(); }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest value, SqlExecutionOptions options, String generatedKeyColumn) {
                    throw new UnsupportedOperationException();
                }
            };
            SyncFormClient client = SyncFormClient.create(sql, (batch, completed) -> {
                request = batch;
                rows.clear();
                Flux.from(batch.rows()).index().doOnNext(indexed -> {
                    rows.add(Arrays.asList(indexed.getT2()));
                    if (completed != null) { completed.accept(indexed.getT1()); }
                }).blockLast();
                return BatchEvidenceFixtures.successful(rows.size());
            }, renderer);
            return SyncFormRepository.create(client, client.entityModels().metadata(type).toDynamicForm(), type);
        }

        private <T> ReactiveFormRepository<T> reactive(Class<T> type) {
            ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest value) { return Flux.error(new UnsupportedOperationException()); }
                public Mono<Long> rowsUpdated(SqlRequest value) { return Mono.error(new UnsupportedOperationException()); }
                @Override
                public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest batch,
                        LongFunction<? extends Publisher<Void>> completed) {
                    return Mono.defer(() -> {
                        request = batch;
                        rows.clear();
                        return Flux.from(batch.rows()).index().concatMap(indexed -> {
                            rows.add(Arrays.asList(indexed.getT2()));
                            return completed == null ? Mono.empty() : Mono.from(completed.apply(indexed.getT1()));
                        }).then(Mono.fromSupplier(() -> BatchEvidenceFixtures.successful(rows.size())));
                    });
                }
            };
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer);
            return ReactiveFormRepository.create(client, client.entityModels().metadata(type).toDynamicForm(), type);
        }
    }
}
