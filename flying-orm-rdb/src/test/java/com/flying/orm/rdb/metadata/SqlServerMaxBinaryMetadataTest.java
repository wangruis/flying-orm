package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaCompatibilityMode;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.schema.ReviewedSchemaPlan;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlServerMaxBinaryMetadataTest {

    @Test
    void builtInMaxBinaryTypesHaveAnExactCompleteSnapshotOnBothReaders() {
        RdbDialect dialect = RdbDialect.sqlServer();
        SyncSqlExecutor jdbc = new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                return catalogRows(request);
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                throw new UnsupportedOperationException();
            }
        };
        ReactiveSqlExecutor reactive = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.defer(() -> Flux.fromIterable(catalogRows(request)));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }
        };

        var jdbcReader = JdbcFormMetadataReaders.create(jdbc, dialect);
        var reactiveReader = ReactiveFormMetadataReaders.create(reactive, dialect);
        List<SchemaSnapshot> snapshots = List.of(jdbcReader.readSnapshot("dbo", "binary_values"),
                reactiveReader.readSnapshot("dbo", "binary_values").block());

        for (SchemaSnapshot snapshot : snapshots) {
            ColumnDefinition actual = snapshot.completeTable().orElseThrow().columns().getFirst();
            assertEquals("varbinary(max)", actual.databaseType().canonical());
            assertNull(actual.length());
        }
        DatabaseDescriptor database = DatabaseDescriptor.of("SQL Server", "16", dialect);
        for (String logicalType : List.of("BLOB", "BINARY", "PROTECTED_BINARY")) {
            RelationalTableDefinition desired = RelationalTableDefinition.builder(snapshots.getFirst().identity())
                    .addColumn(ColumnDefinition.builder("payload", logicalType).build()).build();
            List<ReviewedSchemaPlan> plans = List.of(
                    JdbcSchemaClient.create(jdbc, dialect).reviewRelational(
                            database, desired, jdbcReader, SchemaCompatibilityMode.EXACT),
                    ReactiveSchemaClient.create(reactive, dialect).reviewRelational(
                            database, desired, reactiveReader, SchemaCompatibilityMode.EXACT).block());
            for (ReviewedSchemaPlan plan : plans) {
                assertTrue(plan.operations().isEmpty(), logicalType);
                assertTrue(plan.requests().isEmpty(), logicalType);
            }
        }
    }

    private static List<DynamicRow> catalogRows(SqlRequest request) {
        String sql = request.sql();
        if (sql.contains("from INFORMATION_SCHEMA.COLUMNS c")) {
            return List.of(evaluateColumnProjections(sql));
        }
        return sql.contains("TABLE_COMMENT")
                ? List.of(DynamicRow.copyOf(Map.of("TABLE_REPRESENTABLE", true))) : List.of();
    }

    private static DynamicRow evaluateColumnProjections(String query) {
        List<String> projections = selectProjections(query);
        String projectionSql = String.join(", ", projections.stream().filter(projection -> {
            String normalized = projection.toUpperCase(Locale.ROOT);
            return normalized.endsWith(" AS PHYSICAL_DATA_TYPE")
                    || normalized.endsWith(" AS COLUMN_REPRESENTABLE")
                    || normalized.endsWith(" AS UNSUPPORTED_COLUMN_REASON");
        }).toList());
        // Only SQL Server scalar intrinsics are adapted. The production representability
        // predicate itself is executed unchanged against ordinary, non-generated catalog facts.
        projectionSql = projectionSql
                .replaceAll("(?i)columnproperty\\(sc.object_id, sc.name,\\s*'[^']+'\\)", "0")
                .replaceAll("(?i)try_convert\\(bigint, ([^)]+)\\)", "cast($1 as bigint)");
        String sql = "select " + projectionSql + """

                from (values ('varbinary', -1, cast(null as varchar),
                              cast(null as integer), cast(null as integer)))
                    c(DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, DOMAIN_NAME,
                      NUMERIC_PRECISION, NUMERIC_SCALE)
                cross join (values (0, 0, 0, 0, 0, 1, 'payload', 0, 0))
                    sc(is_computed, is_rowguidcol, is_sparse, is_column_set, is_filestream,
                       object_id, name, rule_object_id, default_object_id)
                cross join (values (cast(null as integer))) dc(object_id)
                cross join (values (cast(null as integer), 0, 1, 1))
                    idc(column_id, is_not_for_replication, seed_value, increment_value)
                cross join (values (cast(null as integer), 0, 1, 1, 1,
                                    cast(null as varchar), cast(null as integer),
                                    cast(null as integer), cast(null as integer)))
                    generation_sequence(object_id, is_cycling, is_cached, start_value, increment,
                                        data_type, numeric_precision, numeric_scale, default_bounds)
                """;
        try (Connection connection = DriverManager.getConnection(
                "jdbc:h2:mem:sqlserver_max_binary_metadata;MODE=MSSQLServer");
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("COLUMN_NAME", "payload");
            row.put("DATA_TYPE", "varbinary");
            row.put("CHARACTER_MAXIMUM_LENGTH", -1);
            row.put("NULLABLE", true);
            row.put("PHYSICAL_DATA_TYPE", result.getString("PHYSICAL_DATA_TYPE"));
            row.put("COLUMN_REPRESENTABLE", result.getBoolean("COLUMN_REPRESENTABLE"));
            row.put("UNSUPPORTED_COLUMN_REASON", result.getString("UNSUPPORTED_COLUMN_REASON"));
            return DynamicRow.copyOf(row);
        } catch (SQLException failure) {
            throw new AssertionError("could not execute the production column projections", failure);
        }
    }

    private static List<String> selectProjections(String sql) {
        String selection = sql.substring(sql.indexOf("select") + "select".length(),
                sql.indexOf("from INFORMATION_SCHEMA.COLUMNS c"));
        List<String> projections = new ArrayList<>();
        int depth = 0;
        int start = 0;
        boolean quoted = false;
        for (int index = 0; index < selection.length(); index++) {
            char character = selection.charAt(index);
            if (character == '\'') {
                if (quoted && index + 1 < selection.length() && selection.charAt(index + 1) == '\'') {
                    index++;
                } else {
                    quoted = !quoted;
                }
            } else if (!quoted) {
                if (character == '(') {
                    depth++;
                } else if (character == ')') {
                    depth--;
                } else if (character == ',' && depth == 0) {
                    projections.add(selection.substring(start, index).strip());
                    start = index + 1;
                }
            }
        }
        projections.add(selection.substring(start).strip());
        return projections;
    }
}
