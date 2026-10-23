package com.flying.orm.consumer;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.FlyingTenant;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityUpsertContractTest {

    @TestFactory
    Stream<DynamicTest> singleUpsertUsesEntityStrategiesWithoutMultiRowLayoutRestrictions() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                reactive ? "R2DBC entity upsert" : "JDBC entity upsert", () -> verify(reactive)));
    }

    private static void verify(boolean reactive) throws Exception {
        String database = "entity_upsert_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database);
        source.setUser("sa");
        source.setPassword("");
        ConnectionFactory factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "h2").option(ConnectionFactoryOptions.PROTOCOL, "mem")
                .option(ConnectionFactoryOptions.DATABASE, database).option(ConnectionFactoryOptions.USER, "sa")
                .option(ConnectionFactoryOptions.PASSWORD, "").build());
        try (Connection keeper = source.getConnection(); Statement statement = keeper.createStatement();
             FlyingOrmClients base = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2").build();
             FlyingOrmClients clients = base.withDefaultDataScope(DataScope.tenant("tenant_id", 7L))) {
            statement.execute("""
                    create table upsert_people(tenant_id bigint, id bigint, state varchar not null,
                        note varchar default 'database-default', nullable_value varchar,
                        primary key(tenant_id,id))
                    """);
            List<EntityLifecyclePhase> events = new ArrayList<>();
            ReactiveFormRepository<Person> async = clients.repository(Person.class).withListener(event -> {
                events.add(event.phase());
                return Mono.empty();
            });
            SyncFormRepository<Person> sync = clients.syncRepository(Person.class).withListener(event -> {
                events.add(event.phase());
                return Mono.empty();
            });
            Person first = new Person(7L, 1L, State.ACTIVE, null, "original");
            if (reactive) {
                Mono<BatchExecutionEvidence> pending = async.upsert(first);
                assertTrue(events.isEmpty());
                try (ResultSet rows = statement.executeQuery("select count(*) from upsert_people")) {
                    assertTrue(rows.next());
                    assertEquals(0, rows.getLong(1));
                }
                successful(pending.block());
                successful(pending.block());
                assertEquals(List.of(EntityLifecyclePhase.PRE_PERSIST, EntityLifecyclePhase.POST_PERSIST,
                        EntityLifecyclePhase.PRE_PERSIST, EntityLifecyclePhase.POST_PERSIST), events);
            } else {
                successful(sync.upsert(first));
                assertEquals(List.of(EntityLifecyclePhase.PRE_PERSIST, EntityLifecyclePhase.POST_PERSIST), events);
            }
            row(statement, "A", "database-default", "original");
            events.clear();
            Person changed = new Person(7L, 1L, State.DISABLED, "edited", null);
            successful(reactive ? async.upsert(changed).block() : sync.upsert(changed));
            row(statement, "D", "edited", null);
            assertEquals(List.of(EntityLifecyclePhase.PRE_PERSIST, EntityLifecyclePhase.POST_PERSIST), events);
            Person skipNull = new Person(7L, 1L, State.ACTIVE, null, "again");
            successful(reactive ? async.upsert(skipNull).block() : sync.upsert(skipNull));
            row(statement, "A", "edited", "again");

            events.clear();
            Person invalid = new Person(7L, 1L, null, "must not replace", null);
            assertThrows(RuntimeException.class, () -> {
                if (reactive) async.upsert(invalid).block();
                else sync.upsert(invalid);
            });
            assertEquals(List.of(EntityLifecyclePhase.PRE_PERSIST), events);
            row(statement, "A", "edited", "again");

            Person otherTenant = new Person(9L, 1L, State.DISABLED, "foreign", null);
            assertThrows(RuntimeException.class, () -> {
                if (reactive) async.upsert(otherTenant).block();
                else sync.upsert(otherTenant);
            });
            try (ResultSet rows = statement.executeQuery("select count(*) from upsert_people")) {
                assertTrue(rows.next());
                assertEquals(1, rows.getLong(1));
            }
            events.clear();
            List<Person> batch = List.of(first, new Person(7L, 2L, State.ACTIVE, null, "second"));
            BatchExecutionEvidence result = reactive ? async.upsertBatch(batch).block() : sync.upsertBatch(batch);
            assertEquals(BatchExecutionState.SUCCESS, result.state());
            assertEquals(2L, result.successfulCount());
            assertEquals(2L, events.stream().filter(phase -> phase == EntityLifecyclePhase.PRE_PERSIST).count());
            assertEquals(2L, events.stream().filter(phase -> phase == EntityLifecyclePhase.POST_PERSIST).count());
            row(statement, "A", "edited", "original");
            try (ResultSet rows = statement.executeQuery("select note from upsert_people where id=2 and tenant_id=7")) {
                assertTrue(rows.next());
                assertEquals("database-default", rows.getString(1));
            }
            events.clear();
            List<Person> mismatch = List.of(new Person(7L, 1L, State.ACTIVE, "prefix-write", "prefix"),
                    new Person(7L, 2L, null, null, "must-not-write"));
            BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class, () -> {
                if (reactive) async.upsertBatch(Flux.fromIterable(mismatch), BatchWriteOptions.of(1)).block();
                else sync.upsertBatch(Flux.fromIterable(mismatch), BatchWriteOptions.of(1));
            });
            BatchExecutionEvidence partial = failure.evidence();
            assertTrue(partial.successfulCount() <= 1);
            assertTrue(!partial.isSuccessful(1));
            row(statement, "A", partial.isSuccessful(0) ? "prefix-write" : "edited",
                    partial.isSuccessful(0) ? "prefix" : "original");
            assertEquals(partial.successfulCount(),
                    events.stream().filter(phase -> phase == EntityLifecyclePhase.POST_PERSIST).count());
            try (ResultSet rows = statement.executeQuery("select nullable_value from upsert_people where id=2 and tenant_id=7")) {
                assertTrue(rows.next());
                assertEquals("second", rows.getString(1));
            }
            statement.execute("create table generated_people(id bigint generated by default as identity primary key, name varchar)");
            GeneratedPerson generated = new GeneratedPerson();
            generated.name = "first";
            if (reactive) {
                Mono<BatchExecutionEvidence> save = base.repository(GeneratedPerson.class).upsert(generated);
                successful(save.block());
                Long assigned = generated.id;
                assertNotNull(assigned);
                generated.name = "changed";
                successful(save.block());
                assertEquals(assigned, generated.id);
            } else {
                successful(base.syncRepository(GeneratedPerson.class).upsert(generated, new String[0]));
                Long assigned = generated.id;
                assertNotNull(assigned);
                generated.name = "changed";
                successful(base.syncRepository(GeneratedPerson.class).upsert(generated));
                assertEquals(assigned, generated.id);
            }
            try (ResultSet rows = statement.executeQuery("select id,name from generated_people")) {
                assertTrue(rows.next());
                assertEquals(generated.id, rows.getLong(1));
                assertEquals("changed", rows.getString(2));
                assertTrue(!rows.next());
            }
            assertThrows(NullPointerException.class, () -> {
                if (reactive) async.upsert(null);
                else sync.upsert(null);
            });
        }
    }

    private static void successful(BatchExecutionEvidence result) {
        assertNotNull(result);
        assertEquals(BatchExecutionState.SUCCESS, result.state());
        assertEquals(1, result.inputCount());
        assertEquals(1, result.successfulCount());
        assertEquals(0, result.failedCount());
    }

    private static void row(Statement statement, String state, String note, String nullable) throws Exception {
        try (ResultSet rows = statement.executeQuery("select state,note,nullable_value from upsert_people where tenant_id=7 and id=1")) {
            assertTrue(rows.next());
            assertEquals(state, rows.getString(1));
            assertEquals(note, rows.getString(2));
            assertEquals(nullable, rows.getString(3));
        }
    }

    @TableName("upsert_people")
    public record Person(@TableId(type = IdType.INPUT) @FlyingTenant Long tenantId,
                         @TableId(type = IdType.INPUT) Long id, State state,
                         @TableField(insertStrategy = FieldStrategy.NOT_NULL,
                                 updateStrategy = FieldStrategy.NOT_NULL) String note,
                         String nullableValue) { }

    public enum State {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        State(String code) { this.code = code; }
    }

    @TableName("generated_people")
    public static final class GeneratedPerson {
        @TableId(type = IdType.AUTO) private Long id;
        private String name;
    }
}
