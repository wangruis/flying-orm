package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConditionDslContractTest {
    @Test
    void entityExtensionsKeepOriginalCollectionValuesAcrossCopiesAndSubscriptions() {
        Fixture f = new Fixture();
        List<String> values = new ArrayList<>(List.of("", " ", " x "));
        EntityDmlQueryOperator<User> query = EntityDmlOperator.create(f.reactive, f.sql, User.class).query()
                .where(User::getName, "any-of", values)
                .or(nested -> nested.isNull(User::getName).whereIfPresent(User::getName, " ")
                        .andGroup(group -> group.in(User::getState, List.of(State.ACTIVE))));
        EntityDmlQueryOperator<User> copy = query.copy();
        values.clear();
        Flux<User> pending = query.fetch();
        query.where(User::getId, 7L);
        pending.blockLast();
        pending.blockLast();
        copy.fetch().blockLast();
        List<Object> expected = List.of("", " ", " x ", "A");
        for (SqlRequest request : f.requests) assertEquals(expected, request.parameters());
        assertTrue(f.requests.getFirst().sql().contains("is null or"));
        query.fetch().blockLast();
        assertEquals(List.of("", " ", " x ", "A", 7L), f.requests.getLast().parameters());
    }

    @Test
    void allCrudEntrypointsShareTypedConditionsAndPersistedEnumValues() {
        Fixture f = new Fixture();
        SyncEntityDmlOperator<User> sync = SyncEntityDmlOperator.create(f.sync, f.sql, User.class);
        EntityDmlOperator<User> reactive = EntityDmlOperator.create(f.reactive, f.sql, User.class);
        // Method references must remain tied to User, and fluent calls retain the concrete terminal type.
        conditions(sync.query(), User::getId, User::getName, User::getState, State.ACTIVE).fetch();
        conditions(reactive.query(), User::getId, User::getName, User::getState, State.ACTIVE).fetch().blockLast();
        conditions(sync.update(), User::getId, User::getName, User::getState, State.ACTIVE).set(User::getName, "new").execute();
        conditions(reactive.update(), User::getId, User::getName, User::getState, State.ACTIVE).set(User::getName, "new").execute().block();
        conditions(sync.delete(), User::getId, User::getName, User::getState, State.ACTIVE).execute();
        conditions(reactive.delete(), User::getId, User::getName, User::getState, State.ACTIVE).execute().block();

        DynamicForm form = f.sync.entityModels().metadata(User.class).toDynamicForm();
        conditions(new SyncQueryOperator(f.sync, f.syncExecutor, f.sql, DataScope.none(), f.dialect).from(form),
                "id", "name", "state", "A").fetch();
        conditions(new QueryOperator(f.reactive, f.reactiveExecutor, f.sql, DataScope.none(), f.dialect).from(form),
                "id", "name", "state", "A").fetch().blockLast();
        conditions(new SyncDmlUpdateOperator(f.sync, f.sql, form.table(), form),
                "id", "name", "state", "A").set("name", "new").execute();
        conditions(new DmlUpdateOperator(f.reactive, f.sql, form.table(), form),
                "id", "name", "state", "A").set("name", "new").execute().block();
        conditions(new SyncDmlDeleteOperator(f.sync, f.sql, form.table(), form),
                "id", "name", "state", "A").execute();
        conditions(new DmlDeleteOperator(f.reactive, f.sql, form.table(), form),
                "id", "name", "state", "A").execute().block();

        assertEquals(12, f.requests.size());
        String where = f.requests.getFirst().sql().split(" where ", 2)[1];
        for (SqlRequest request : f.requests) {
            assertEquals(where, request.sql().split(" where ", 2)[1]);
            List<Object> expected = new ArrayList<>(List.of("", "  Ada  ", "A", 1L, 2L, 3L, 9L, 4L));
            if (request.sql().startsWith("update ")) expected.addFirst("new");
            assertEquals(expected, request.parameters(), request.sql());
        }
        assertTrue(where.contains("is null"));
        assertTrue(where.contains("is not null"));
    }

    private static <F, S extends ConditionDsl<F, S>> S conditions(
            ConditionDsl<F, S> conditions, F id, F name, F state, Object value) {
        return conditions.where(name, "").and(name, "=", "  Ada  ")
                .whereIfPresent(name, " ").whereIfPresent(name, "=", null)
                .in(state, List.of(value)).notIn(id, List.of(1L, 2L))
                .between(id, 3L, 9L).and(id, 4L).isNull(name).isNotNull(state);
    }

    @TableName("users")
    public static final class User {
        @TableId(type = IdType.INPUT) private Long id;
        private String name;
        private State state;
        public Long getId() { return id; }
        public String getName() { return name; }
        public State getState() { return state; }
    }

    enum State {
        ACTIVE("A");
        @EnumValue private final String code;
        State(String code) { this.code = code; }
    }

    private static final class Fixture {
        final List<SqlRequest> requests = new ArrayList<>();
        final RdbDialect dialect = RdbDialect.postgresql();
        final SqlRenderer sql = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.of("any-of", ConditionValueShape.COLLECTION, SqlTermHandler.in()::render)).build()
                .withIdentifierRenderer(dialect.schema()::identifier);
        final SyncSqlExecutor syncExecutor = new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { requests.add(request); return List.of(); }
            public long rowsUpdated(SqlRequest request) { requests.add(request); return 1; }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                throw new UnsupportedOperationException();
            }
        };
        final ReactiveSqlExecutor reactiveExecutor = new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.defer(() -> { requests.add(request); return Flux.empty(); });
            }
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.fromSupplier(() -> { requests.add(request); return 1L; });
            }
        };
        final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(sql, dialect);
        final SyncFormClient sync = SyncFormClient.create(syncExecutor,
                (request, consumer) -> { throw new UnsupportedOperationException(); }, renderer);
        final ReactiveFormClient reactive = ReactiveFormClient.create(reactiveExecutor, renderer);
    }
}
