package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ExternalTransactionTestSupport;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import io.r2dbc.h2.H2ConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityFieldCodecConditionH2Test {

    private static final Duration WAIT = Duration.ofSeconds(10);

    @TestFactory
    Stream<DynamicTest> standardConditionsUseTheCodecDeclaredByTheField() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("reactive=" + reactive, () -> {
            try (Database database = new Database()) {
                database.insert(reactive);
                assertEquals("db:old", database.rawText(1L));
                ConditionGroup where = ConditionGroup.and().where("text", "=", "old").build();
                List<CodedRow> selected = reactive
                        ? database.clients.repository(CodedRow.class).select(where).collectList().block(WAIT)
                        : database.clients.syncRepository(CodedRow.class).select(where);
                assertEquals(List.of(new CodedRow(1L, "old")), selected);
                ConditionGroup in = ConditionGroup.and().where("text", "in", List.of("old", "missing")).build();
                List<CodedRow> members = reactive
                        ? database.clients.repository(CodedRow.class).select(in).collectList().block(WAIT)
                        : database.clients.syncRepository(CodedRow.class).select(in);
                assertEquals(selected, members);
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> scopedUpsertUsesTheSameFieldCodecAsTheWrite() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("reactive=" + reactive, () -> {
            try (Database database = new Database()) {
                database.insert(reactive);
                assertEquals("db:old", database.rawText(1L));
                BatchSpec request = BatchSpec.upsert(database.descriptor.form(), Flux.just(
                                Map.of("id", 1L, "text", "changed"),
                                Map.of("id", 3L, "text", "inserted")))
                        .withScope(DataScope.where(ConditionGroup.and().where("text", "=", "old").build()))
                        .withOptions(BatchWriteOptions.of(2));
                assertDoesNotThrow(() -> database.execute(request, reactive));
                assertEquals("db:changed", database.rawText(1L));
                assertEquals("db:other", database.rawText(2L));
                assertEquals("db:inserted", database.rawText(3L));
            }
        }));
    }

    @TableName("coded_scope_rows")
    private record CodedRow(@TableId(type = IdType.INPUT) Long id,
                             @TableColumn(databaseTypeId = "coded-text") CharSequence text) {
    }

    private static final class CodedTextCodec implements ValueCodec {
        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == CharSequence.class;
        }

        @Override
        public Object write(Object value) {
            return value == null ? null : "db:" + value;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value == null ? null : value.toString().substring(3);
        }
    }

    private static final class Database implements AutoCloseable {
        private final Connection keeper;
        private final EntitySchemaDescriptor<CodedRow> descriptor;
        private final FlyingOrmClients clients;
        private final JdbcDataSource source;
        private final ConnectionFactory factory;
        private com.flying.orm.rdb.jdbc.JdbcConnectionAccess jdbcTransaction;
        private com.flying.orm.rdb.reactive.R2dbcConnectionAccess reactiveTransaction;

        private Database() throws Exception {
            String name = "coded_scope_" + UUID.randomUUID().toString().replace("-", "");
            source = new JdbcDataSource();
            source.setURL("jdbc:h2:mem:" + name + ";DATABASE_TO_LOWER=TRUE");
            source.setUser("sa");
            source.setPassword("");
            keeper = source.getConnection();
            try (Statement statement = keeper.createStatement()) {
                statement.execute("create table coded_scope_rows (id bigint primary key, text varchar(64))");
            }
            descriptor = EntitySchemaDescriptor.builder(CodedRow.class)
                    .typeMappings(EntityTypeMappingRegistry.builder()
                            .register("coded-text", CharSequence.class, DatabaseType.of("VARCHAR"), new CodedTextCodec())
                            .build()).build();
            factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                    .option(DRIVER, "h2").option(PROTOCOL, H2ConnectionFactoryProvider.PROTOCOL_MEM)
                    .option(DATABASE, name).option(USER, "sa").option(PASSWORD, "")
                    .option(H2ConnectionFactoryProvider.OPTIONS, "DATABASE_TO_LOWER=TRUE").build());
            clients = FlyingOrmClients.builder(jdbcAccess(), reactiveAccess()).configuredDialect("h2").entitySchema(descriptor).build();
        }

        private void insert(boolean reactive) throws Exception {
            BatchSpec request = BatchSpec.insert(descriptor.form(), Flux.just(
                    Map.of("id", 1L, "text", "old"), Map.of("id", 2L, "text", "other")));
            execute(request, reactive);
        }

        private com.flying.orm.rdb.jdbc.JdbcConnectionAccess jdbcAccess() {
            var normal = com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(source);
            return new com.flying.orm.rdb.jdbc.JdbcConnectionAccess() {
                public java.sql.Connection getConnection(com.flying.orm.core.sql.render.SqlRequest request)
                        throws java.sql.SQLException {
                    return (jdbcTransaction == null ? normal : jdbcTransaction).getConnection(request);
                }
                public void releaseConnection(java.sql.Connection connection,
                        com.flying.orm.core.sql.render.SqlRequest request) throws java.sql.SQLException {
                    (jdbcTransaction == null ? normal : jdbcTransaction).releaseConnection(connection, request);
                }
            };
        }

        private com.flying.orm.rdb.reactive.R2dbcConnectionAccess reactiveAccess() {
            var normal = com.flying.orm.rdb.execution.ConnectionAccessTestSupport.reactive(factory);
            return new com.flying.orm.rdb.reactive.R2dbcConnectionAccess() {
                public org.reactivestreams.Publisher<? extends io.r2dbc.spi.Connection> getConnection(
                        com.flying.orm.core.sql.render.SqlRequest request) {
                    return (reactiveTransaction == null ? normal : reactiveTransaction).getConnection(request);
                }
                public org.reactivestreams.Publisher<Void> releaseConnection(reactor.core.publisher.SignalType signal,
                        io.r2dbc.spi.Connection connection, com.flying.orm.core.sql.render.SqlRequest request) {
                    return (reactiveTransaction == null ? normal : reactiveTransaction)
                            .releaseConnection(signal, connection, request);
                }
            };
        }

        private BatchExecutionEvidence execute(BatchSpec request, boolean reactive) throws Exception {
            try {
                BatchExecutionEvidence result = reactive
                        ? ExternalTransactionTestSupport.reactive(factory, transaction -> {
                            reactiveTransaction = transaction;
                            return clients.forms().writeBatch(request);
                        })
                        : ExternalTransactionTestSupport.jdbc(source, transaction -> {
                            jdbcTransaction = transaction;
                            return clients.syncForms().writeBatch(request);
                        });
                assertEquals(com.flying.orm.rdb.batch.BatchExecutionState.SUCCESS, result.state());
                return result;
            } finally {
                jdbcTransaction = null;
                reactiveTransaction = null;
            }
        }

        private String rawText(long id) throws Exception {
            try (Statement statement = keeper.createStatement();
                 ResultSet rows = statement.executeQuery("select text from coded_scope_rows where id = " + id)) {
                assertTrue(rows.next());
                return rows.getString(1);
            }
        }

        @Override
        public void close() throws Exception {
            clients.close();
            keeper.close();
        }
    }
}
