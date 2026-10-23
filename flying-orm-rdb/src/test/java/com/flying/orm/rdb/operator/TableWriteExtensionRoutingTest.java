package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.FeatureRegistry;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.ReactiveWriteInterceptor;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.SyncWriteInterceptor;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.support.ListQueryFixture;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableWriteExtensionRoutingTest {

    @Test
    void dynamicEntityAndRepositoryWritesShareTheSyncExtensionAndKeepEntityEvents() {
        List<String> events = new ArrayList<>();
        List<SqlRequest> requests = new ArrayList<>();
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        ListQueryFixture executor = new ListQueryFixture() {
            @Override public List<DynamicRow> query(SqlRequest request) { return List.of(); }
            @Override public long rowsUpdated(SqlRequest request) {
                events.add("sql"); requests.add(request); return 1;
            }
        };
        SyncFormClient client = SyncFormClient.create(executor,
                (request, completed) -> { throw new AssertionError("not batch"); },
                FormDataSqlRenderer.create(renderer, RdbDialect.postgresql()));
        DynamicForm form = form(FeatureRegistry.builder().add(new SyncWriteInterceptor() {
            @Override public <R> R write(WriteSpec spec, Function<WriteSpec, R> next) {
                events.add("table-before");
                R result = next.apply(spec);
                events.add("table-after");
                return result;
            }
        }).build());
        SyncDmlOperator dynamic = new SyncDmlOperator(client, executor, renderer, DataScope.none(), RdbDialect.postgresql());
        assertEquals(1, dynamic.update(form).set("name", "Ada").where("id", 7L).execute());
        assertEquals(1, dynamic.delete(form).where("id", 7L).execute());
        SyncFormRepository<User> repository = SyncFormRepository.create(client, form, User.class)
                .withListener(event -> { events.add(event.phase().name()); return Mono.empty(); });
        assertEquals(1, repository.createUpdate().set(User::getName, "next").where(User::getId, 7L).execute());
        assertEquals(1, repository.createDelete().where(User::getId, 7L).execute());
        assertEquals(1, repository.deleteById(7L));
        assertEquals(1, repository.deleteByIds(List.of(7L, 8L)));
        assertEquals(java.util.Collections.nCopies(6, List.of("table-before", "sql", "table-after"))
                .stream().flatMap(List::stream).toList(), events);
        assertTrue(requests.get(1).sql().startsWith("update "), "logical delete remains an update");
        events.clear();
        repository.insert(new User());
        assertEquals(List.of("PRE_PERSIST", "table-before", "sql", "table-after", "POST_PERSIST"), events);
    }

    @Test
    void dynamicEntityAndRepositoryWritesShareTheReactiveExtensionAndKeepEntityEvents() {
        List<String> events = new ArrayList<>();
        List<SqlRequest> requests = new ArrayList<>();
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override public Flux<DynamicRow> query(SqlRequest request) { return Flux.empty(); }
            @Override public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.fromSupplier(() -> { events.add("sql"); requests.add(request); return 1L; });
            }
        };
        ReactiveFormClient client = ReactiveFormClient.create(executor,
                FormDataSqlRenderer.create(renderer, RdbDialect.postgresql()));
        DynamicForm form = form(FeatureRegistry.builder().add(new ReactiveWriteInterceptor() {
            @Override public <R> Mono<R> write(WriteSpec spec, Function<WriteSpec, Mono<R>> next) {
                events.add("table-before");
                return next.apply(spec).doOnNext(result -> events.add("table-after"));
            }
        }).build());
        DmlOperator dynamic = new DmlOperator(client, executor, renderer, DataScope.none(), RdbDialect.postgresql());
        assertEquals(1L, dynamic.update(form).set("name", "Ada").where("id", 7L).execute().block());
        assertEquals(1L, dynamic.delete(form).where("id", 7L).execute().block());
        ReactiveFormRepository<User> repository = ReactiveFormRepository.create(client, form, User.class)
                .withListener(event -> Mono.fromRunnable(() -> events.add(event.phase().name())));
        assertEquals(1L, repository.createUpdate().set(User::getName, "next").where(User::getId, 7L).execute().block());
        assertEquals(1L, repository.createDelete().where(User::getId, 7L).execute().block());
        assertEquals(1L, repository.deleteById(7L).block());
        assertEquals(1L, repository.deleteByIds(List.of(7L, 8L)).block());
        assertEquals(java.util.Collections.nCopies(6, List.of("table-before", "sql", "table-after"))
                .stream().flatMap(List::stream).toList(), events);
        assertTrue(requests.get(1).sql().startsWith("update "));
        events.clear();
        repository.insert(new User()).block();
        assertEquals(List.of("PRE_PERSIST", "table-before", "sql", "table-after", "POST_PERSIST"), events);
    }

    private static DynamicForm form(FeatureRegistry features) {
        return DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR(128)"))
                .addField(DynamicField.of("deleted", "INTEGER"))
                .logicDelete("deleted", 0, 1).features(features).build();
    }

    @TableName("users")
    public static final class User {
        @TableId(type = IdType.INPUT)
        private Long id = 7L;
        private String name = "Ada";
        private Integer deleted = 0;
        public Long getId() { return id; }
        public String getName() { return name; }
        public Integer getDeleted() { return deleted; }
    }
}
