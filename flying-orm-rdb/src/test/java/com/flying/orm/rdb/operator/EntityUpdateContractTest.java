package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EntityUpdateContractTest {
    @Test
    void allAssignmentInputsUseOneMappingAndReadSelectedGettersOnce() {
        Fixture f = new Fixture();
        for (int mode = 0; mode < 5; mode++) {
            User user = new User(State.DISABLED);
            SyncEntityDmlUpdateOperator<User> sync = f.sync.update();
            switch (mode) {
                case 0 -> sync.set(User::getState, user.getState());
                case 1 -> sync.set("state", user.getState());
                case 2 -> sync.set(user::getState);
                case 3 -> sync.set(user, User::getState, User::getState);
                case 4 -> sync.setExcept(user, User::getName);
            }
            assertEquals(1, user.stateReads);
            assertEquals(0, user.nameReads);
            user.state = State.ACTIVE;
            sync.where(User::getId, 7L).execute();
            assertEquals(1, user.stateReads);

            user = new User(State.DISABLED);
            EntityDmlUpdateOperator<User> reactive = f.reactive.update();
            switch (mode) {
                case 0 -> reactive.set(User::getState, user.getState());
                case 1 -> reactive.set("state", user.getState());
                case 2 -> reactive.set(user::getState);
                case 3 -> reactive.set(user, User::getState, User::getState);
                case 4 -> reactive.setExcept(user, User::getName);
            }
            assertEquals(1, user.stateReads);
            assertEquals(0, user.nameReads);
            user.state = State.ACTIVE;
            Mono<Long> update = reactive.where(User::getId, 7L).execute();
            update.block();
            update.block();
            assertEquals(1, user.stateReads);
        }
        assertEquals(15, f.requests.size());
        for (SqlRequest request : f.requests) {
            assertEquals("update \"users\" set \"user_state\" = ? where \"id\" = ?", request.sql());
            assertEquals(List.of("D", 7L), request.parameters());
        }
    }

    @Test
    void boundReferenceCacheDoesNotCaptureAnEarlierEntityAndExplicitNullIsWritten() {
        Fixture f = new Fixture();
        for (State state : State.values()) {
            User user = new User(state);
            f.sync.update().set(user::getState).where(User::getId, 7L).execute();
            assertEquals(state.code, f.requests.getLast().parameters().getFirst());
            assertEquals(1, user.stateReads);
        }
        f.sync.update().set("state", null).where(User::getId, 7L).execute();
        assertEquals(String.class, assertInstanceOf(SqlNullParameter.class,
                f.requests.getLast().parameters().getFirst()).javaType());
    }

    @Test
    void excludesAreResolvedBeforeReadingAndLaterAssignmentsKeepTheirOrder() {
        Fixture f = new Fixture();
        User user = new User(State.ACTIVE);
        f.sync.update().set("name", "first").setExcept(user, User::getName, User::getId, User::getVersion)
                .set("state", State.DISABLED).where(User::getId, 7L).execute();
        assertEquals(0, user.nameReads);
        assertEquals(1, user.stateReads);
        assertEquals(List.of("first", "D", 7L), f.requests.getLast().parameters());
        assertTrue(f.requests.getLast().sql().contains("set \"name\" = ?, \"user_state\" = ?"));
    }

    @Test
    void invalidPropertyReferencesFailBeforeGetterInvocationOrExecution() {
        Fixture f = new Fixture();
        User user = new User(State.ACTIVE);
        assertThrows(MappingException.class, () -> f.sync.update().set(user::getVersion));
        assertThrows(MappingException.class, () -> f.sync.update().set(user::getIgnored));
        assertThrows(MappingException.class, () -> f.reactive.update().set(() -> user.getState()));
        assertThrows(IllegalArgumentException.class, () -> f.sync.update().set("missing", State.ACTIVE));
        assertEquals(0, user.stateReads);
        assertTrue(f.requests.isEmpty());
    }

    @Test
    void boundRecordAccessorAndBooleanGetterUseTheExistingResolver() {
        Fixture f = new Fixture();
        RecordUser record = new RecordUser(7L, "record");
        f.syncClient.entity(RecordUser.class).update().set(record::name).where(RecordUser::id, 7L).execute();
        assertEquals(List.of("record", 7L), f.requests.getLast().parameters());
        BooleanUser user = new BooleanUser();
        f.reactiveClient.entity(BooleanUser.class).update().set(user::isEnabled)
                .where(BooleanUser::getId, 7L).execute().block();
        assertEquals(List.of(true, 7L), f.requests.getLast().parameters());
    }

    @TableName("users")
    public record RecordUser(@TableId(type = IdType.INPUT) Long id, String name) { }

    @TableName("users")
    public static final class BooleanUser {
        @TableId(type = IdType.INPUT) private Long id = 7L;
        private boolean enabled = true;
        public Long getId() { return id; }
        public boolean isEnabled() { return enabled; }
    }

    @TableName("users")
    public static final class User {
        @TableId(type = IdType.INPUT) private Long id = 7L;
        @TableField("user_state") private State state;
        private String name = "Ada";
        @Version private Long version = 1L;
        @TableField(exist = false) private String ignored;
        transient int stateReads;
        transient int nameReads;
        User(State state) { this.state = state; }
        public Long getId() { return id; }
        public State getState() { stateReads++; return state; }
        public String getName() { nameReads++; return name; }
        public Long getVersion() { throw new AssertionError("version must not be read for SET"); }
        public String getIgnored() { throw new AssertionError("non-persistent getter must not be invoked"); }
    }

    public enum State {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        State(String code) { this.code = code; }
    }

    private static final class Fixture {
        final List<SqlRequest> requests = new ArrayList<>();
        final SqlRenderer sql = SqlRenderer.builder().addDefaultTerms().build();
        final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(sql, RdbDialect.postgresql());
        final SyncFormClient syncClient = SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { throw new UnsupportedOperationException(); }
            public long rowsUpdated(SqlRequest request) { requests.add(request); return 1; }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                throw new UnsupportedOperationException();
            }
        }, (request, consumer) -> { throw new UnsupportedOperationException(); }, renderer);
        final ReactiveFormClient reactiveClient = ReactiveFormClient.create(new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) { return Flux.error(new UnsupportedOperationException()); }
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.fromSupplier(() -> { requests.add(request); return 1L; });
            }
        }, renderer);
        final SyncEntityDmlOperator<User> sync = SyncEntityDmlOperator.create(syncClient, sql, User.class);
        final EntityDmlOperator<User> reactive = EntityDmlOperator.create(reactiveClient, sql, User.class);
    }
}
