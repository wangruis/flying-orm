package com.flying.orm.rdb.execution;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtectedBatchSequentialUpdateTest {

    @TestFactory
    List<DynamicTest> sequentialUpdatesDoNotDependOnInputBufferSize() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean reactive : List.of(false, true)) {
            for (boolean protectedField : List.of(false, true)) {
                for (int buffer : List.of(1, 2)) {
                    tests.add(DynamicTest.dynamicTest(
                            "reactive=" + reactive + "/protected=" + protectedField + "/buffer=" + buffer,
                            () -> verify(reactive, protectedField, buffer, false)));
                }
            }
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> laterVersionConflictKeepsTheCompletedProtectedRow() {
        return List.of(false, true).stream().map(reactive -> DynamicTest.dynamicTest(
                "version conflict/reactive=" + reactive, () -> verify(reactive, true, 2, true))).toList();
    }

    private static void verify(boolean reactive, boolean protectedField, int buffer, boolean conflict) {
        var builder = DynamicForm.builder("sequential", "sequential")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("version", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"));
        if (protectedField) {
            builder.encrypted("secret", EncryptedFieldDefinition.builder()
                    .searchModes(EncryptedSearchMode.CONTAINS).build());
        }
        DynamicForm form = builder.build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            RdbDialect dialect = RdbDialect.postgresql();
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), dialect).withProtectedFields(runtime);
            BatchSpec spec = BatchSpec.update(form, Flux.just(
                    update("alphabet", 1L), update("different", conflict ? 9L : 2L)))
                    .withOptions(BatchWriteOptions.of(buffer));
            Database database = new Database();
            List<Long> completed = new ArrayList<>();
            java.util.function.Supplier<BatchExecutionEvidence> execute;
            if (reactive) {
                var executor = R2dbcSqlExecutor.create(database.reactiveAccess(), dialect);
                execute = () -> ReactiveFormClient.create(executor, renderer).writeBatch(spec,
                                offset -> Mono.fromRunnable(() -> completed.add(offset)))
                        .block(Duration.ofSeconds(5));
            } else {
                var access = database.jdbcAccess();
                execute = () -> SyncFormClient.create(JdbcSqlExecutor.create(access, dialect),
                        JdbcBatchWriter.create(access, dialect), renderer).writeBatch(spec, completed::add);
            }
            BatchExecutionEvidence result = conflict
                    ? assertThrows(BatchExecutionEvidenceException.class, execute::get).evidence() : execute.get();
            Set<String> expectedTokens = tokens(runtime, form, conflict ? "alphabet" : "different");
            List<Set<String>> expectedBeforeReads = protectedField
                    ? List.of(Set.of(), tokens(runtime, form, "alphabet")) : List.of();
            assertAll(
                    () -> assertEquals(conflict ? BatchExecutionState.PARTIAL : BatchExecutionState.SUCCESS,
                            result.state()),
                    () -> assertEquals(conflict ? 1L : 2L, result.successfulCount()),
                    () -> assertEquals(conflict ? 2L : 3L, database.version),
                    () -> assertEquals(conflict ? 1 : 2, database.businessWrites),
                    () -> assertEquals(conflict ? List.of(0L) : List.of(0L, 1L), completed),
                    () -> assertEquals(1, database.acquired),
                    () -> assertEquals(1, database.released),
                    () -> assertEquals(expectedBeforeReads, database.tokensBeforeOwnerReads,
                            "each prior row must finish token replacement before the next owner query"),
                    () -> assertEquals(expectedTokens, database.tokens));
        }
    }

    private static Set<String> tokens(ProtectedFieldRuntime runtime, DynamicForm form, String value) {
        Set<String> result = new HashSet<>();
        runtime.prepareContainsTokens(form, Map.of("secret", value),
                DataScope.none(), ValueCodecRegistry.standard()).forEach(field ->
                field.tokens().forEach(token -> result.add(binary(token))));
        return result;
    }

    private static BatchOptimisticUpdate update(String value, long expectedVersion) {
        return new BatchOptimisticUpdate(Map.of("secret", value),
                ConditionGroup.and().where("id", "=", 1L).build(),
                OptimisticLockOptions.increment("version", expectedVersion));
    }

    /** Models only the SQL used by this test; the ORM owns all planning and execution sequencing. */
    private static final class Database {
        private long version = 1L;
        private int businessWrites;
        private int acquired;
        private int released;
        private final Set<String> tokens = new HashSet<>();
        private final List<Set<String>> tokensBeforeOwnerReads = new ArrayList<>();

        private com.flying.orm.rdb.jdbc.JdbcConnectionAccess jdbcAccess() {
            return new com.flying.orm.rdb.jdbc.JdbcConnectionAccess() {
                public java.sql.Connection getConnection(com.flying.orm.core.sql.render.SqlRequest request) {
                    acquired++;
                    return jdbc();
                }
                public void releaseConnection(java.sql.Connection connection,
                                              com.flying.orm.core.sql.render.SqlRequest request) {
                    released++;
                }
            };
        }

        private com.flying.orm.rdb.reactive.R2dbcConnectionAccess reactiveAccess() {
            return new com.flying.orm.rdb.reactive.R2dbcConnectionAccess() {
                public Mono<io.r2dbc.spi.Connection> getConnection(
                        com.flying.orm.core.sql.render.SqlRequest request) {
                    return Mono.fromSupplier(() -> { acquired++; return reactive(); });
                }
                public Mono<Void> releaseConnection(reactor.core.publisher.SignalType signal,
                        io.r2dbc.spi.Connection connection, com.flying.orm.core.sql.render.SqlRequest request) {
                    return Mono.fromRunnable(() -> released++);
                }
            };
        }

        private List<Object[]> owners(String sql, List<Object> parameters) {
            List<Object[]> rows = new ArrayList<>();
            boolean slots = sql.contains("flying_owner_");
            int width = 2; // Only owner id and expected version are bound; slots are SQL literals.
            for (int offset = 0; offset < parameters.size(); offset += width) {
                tokensBeforeOwnerReads.add(Set.copyOf(tokens));
                long expected = ((Number) parameters.get(offset + width - 1)).longValue();
                if (expected == version) {
                    rows.add(slots ? new Object[]{1L, offset / width} : new Object[]{1L});
                }
            }
            return rows;
        }

        private int write(String sql, List<Object> parameters) {
            if (sql.startsWith("update ")) {
                // SET secret=?, WHERE id=? AND version=?; an owner restriction may follow.
                long expected = ((Number) parameters.get(2)).longValue();
                if (expected != version) return 0;
                version++;
                businessWrites++;
                return 1;
            }
            if (sql.startsWith("delete ")) {
                int count = tokens.size();
                tokens.clear();
                return count;
            }
            if (sql.startsWith("insert ")) {
                tokens.add(binary(parameters.getLast()));
                return 1;
            }
            throw new AssertionError("unexpected SQL: " + sql);
        }

        private java.sql.Connection jdbc() {
            return proxy(java.sql.Connection.class, (self, method, args) -> {
                if (method.getName().equals("prepareStatement")) return jdbcStatement((String) args[0]);
                throw new AssertionError("unexpected connection call: " + method);
            });
        }

        private PreparedStatement jdbcStatement(String sql) {
            Map<Integer, Object> bindings = new TreeMap<>();
            List<List<Object>> batch = new ArrayList<>();
            return proxy(PreparedStatement.class, (self, method, args) -> switch (method.getName()) {
                case "setObject", "setBinaryStream" -> {
                    Object value = args[1] instanceof InputStream input ? input.readAllBytes() : args[1];
                    bindings.put((Integer) args[0], value);
                    yield null;
                }
                case "addBatch" -> { batch.add(new ArrayList<>(bindings.values())); yield null; }
                case "executeBatch" -> {
                    int[] counts = batch.stream().mapToInt(row -> write(sql, row)).toArray();
                    batch.clear();
                    yield counts;
                }
                case "executeUpdate" -> write(sql, new ArrayList<>(bindings.values()));
                case "executeLargeUpdate" -> (long) write(sql, new ArrayList<>(bindings.values()));
                case "executeQuery" -> jdbcRows(owners(sql, new ArrayList<>(bindings.values())),
                        sql.contains("flying_owner_") ? 2 : 1);
                case "close", "cancel" -> null;
                case "isClosed" -> false;
                default -> throw new AssertionError("unexpected statement call: " + method);
            });
        }

        private io.r2dbc.spi.Connection reactive() {
            return proxy(io.r2dbc.spi.Connection.class, (self, method, args) -> {
                if (method.getName().equals("createStatement")) return reactiveStatement((String) args[0]);
                throw new AssertionError("unexpected connection call: " + method);
            });
        }

        private io.r2dbc.spi.Statement reactiveStatement(String sql) {
            Map<Integer, Object> bindings = new TreeMap<>();
            List<List<Object>> batch = new ArrayList<>();
            return proxy(io.r2dbc.spi.Statement.class, (self, method, args) -> switch (method.getName()) {
                case "bind" -> { bindings.put((Integer) args[0], args[1]); yield self; }
                case "fetchSize" -> self;
                case "add" -> { batch.add(new ArrayList<>(bindings.values())); yield self; }
                case "execute" -> Flux.defer(() -> {
                    if (sql.startsWith("select ")) {
                        return Flux.just(reactiveRows(owners(sql, new ArrayList<>(bindings.values())),
                                sql.contains("flying_owner_") ? 2 : 1));
                    }
                    batch.add(new ArrayList<>(bindings.values()));
                    return Flux.fromIterable(batch).map(row -> updated(write(sql, row)));
                });
                default -> throw new AssertionError("unexpected statement call: " + method);
            });
        }
    }

    private static ResultSet jdbcRows(List<Object[]> rows, int columns) {
        ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (self, method, args) -> switch (method.getName()) {
            case "getColumnCount" -> columns;
            case "getColumnLabel", "getColumnName" -> (Integer) args[0] == 1 ? "id" : "__flying_owner_slot";
            case "getColumnType" -> Types.BIGINT;
            case "getColumnTypeName" -> "BIGINT";
            case "getColumnClassName" -> Long.class.getName();
            default -> null;
        });
        int[] cursor = {-1};
        return proxy(ResultSet.class, (self, method, args) -> switch (method.getName()) {
            case "getMetaData" -> metadata;
            case "next" -> ++cursor[0] < rows.size();
            case "getObject" -> rows.get(cursor[0])[(Integer) args[0] - 1];
            case "wasNull", "isClosed" -> false;
            case "close" -> null;
            default -> throw new AssertionError("unexpected row call: " + method);
        });
    }

    @SuppressWarnings("unchecked")
    private static Result reactiveRows(List<Object[]> rows, int columns) {
        List<ColumnMetadata> metadataColumns = new ArrayList<>();
        for (int index = 0; index < columns; index++) {
            String name = index == 0 ? "id" : "__flying_owner_slot";
            metadataColumns.add(proxy(ColumnMetadata.class, (self, method, args) -> switch (method.getName()) {
                case "getName" -> name;
                case "getJavaType" -> Long.class;
                case "getType" -> R2dbcType.BIGINT;
                case "getNullability" -> io.r2dbc.spi.Nullability.NON_NULL;
                default -> null;
            }));
        }
        RowMetadata metadata = proxy(RowMetadata.class, (self, method, args) -> switch (method.getName()) {
            case "getColumnMetadatas" -> metadataColumns;
            case "getColumnMetadata" -> metadataColumns.get((Integer) args[0]);
            default -> throw new AssertionError(method);
        });
        return proxy(Result.class, (self, method, args) -> {
            if (!method.getName().equals("map")) throw new AssertionError(method);
            return Flux.fromIterable(rows).map(values -> {
                Row row = proxy(Row.class, (rowSelf, rowMethod, rowArgs) -> switch (rowMethod.getName()) {
                    case "get" -> values[(Integer) rowArgs[0]];
                    case "getMetadata" -> metadata;
                    default -> throw new AssertionError(rowMethod);
                });
                return ((BiFunction<Row, RowMetadata, Object>) args[0]).apply(row, metadata);
            });
        });
    }

    private static Result updated(long count) {
        return proxy(Result.class, (self, method, args) -> {
            if (method.getName().equals("getRowsUpdated")) return Mono.just(count);
            throw new AssertionError(method);
        });
    }

    private static String binary(Object value) {
        if (value instanceof byte[] bytes) return Base64.getEncoder().encodeToString(bytes);
        ByteBuffer buffer = ((ByteBuffer) value).duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
