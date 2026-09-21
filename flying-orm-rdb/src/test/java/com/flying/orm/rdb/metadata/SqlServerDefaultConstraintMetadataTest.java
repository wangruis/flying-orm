package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.schema.ReviewedSchemaPlan;
import com.flying.orm.rdb.schema.SchemaCompatibilityMode;
import com.flying.orm.rdb.schema.SchemaOperation;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlServerDefaultConstraintMetadataTest {

    private static final RelationIdentity TABLE = RelationIdentity.of(null, "dbo", "events");
    private static final RdbDialect DIALECT = RdbDialect.sqlServer();
    private static final DatabaseDescriptor DATABASE = DatabaseDescriptor.of("SQL Server", "16", DIALECT);

    @Test
    void catalogQueryProjectsTheObservedDefaultConstraintName() {
        String query = SqlServerMetadataQueries.queries().columnQuery().create("dbo", "events").sql();

        assertTrue(query.contains("dc.name as DEFAULT_CONSTRAINT_NAME"));
    }

    @Test
    void changingOnlyTheObservedDefaultNameInvalidatesBothReadersReviewedFingerprints() {
        Catalog catalog = new Catalog("DF_events_recorded_at_1");
        RelationalTableDefinition desired = desired(ColumnDefault.currentTimestamp());

        List<ReviewedSchemaPlan> first = catalog.reviews(desired);
        catalog.defaultName.set("DF_events_recorded_at_2");
        List<ReviewedSchemaPlan> second = catalog.reviews(desired);

        for (int index = 0; index < first.size(); index++) {
            assertTrue(first.get(index).operations().isEmpty(), "an unnamed desired default matches its observed name");
            assertTrue(second.get(index).operations().isEmpty(), "renaming alone does not change an unnamed target");
            assertNotEquals(first.get(index).actualFingerprint(), second.get(index).actualFingerprint(),
                    "the frozen physical name must participate in the pre-execution snapshot fingerprint");
        }
        assertEquals(first.getFirst().actualFingerprint(), first.getLast().actualFingerprint());
        assertEquals(second.getFirst().actualFingerprint(), second.getLast().actualFingerprint());
    }

    @Test
    void explicitlyNamedDesiredDefaultConstrainsTheObservedName() {
        Catalog catalog = new Catalog("DF_events_recorded_at_1");
        // A captured complete table is also a public explicit relational target; no new API is needed for RED.
        RelationalTableDefinition namedDesired = catalog.snapshot().completeTable().orElseThrow();
        assertTrue(catalog.reviews(namedDesired).stream().allMatch(plan -> plan.operations().isEmpty()));
        catalog.defaultName.set("DF_events_recorded_at_2");

        for (ReviewedSchemaPlan plan : catalog.reviews(namedDesired)) {
            assertEquals(List.of(SchemaOperation.Kind.CHANGE_COLUMN), plan.operations().stream()
                    .map(SchemaOperation::kind).distinct().toList());
        }
    }

    @Test
    void removingTheTimestampDefaultIsReviewedUsingItsObservedConstraintName() {
        Catalog catalog = new Catalog("DF__events__recorded__6E01572D");

        for (ReviewedSchemaPlan plan : catalog.reviews(desired(ColumnDefault.none()))) {
            assertFalse(plan.requiresManualAction());
            assertTrue(plan.requests().stream().anyMatch(request ->
                    request.sql().contains("drop constraint [DF__events__recorded__6E01572D]")),
                    "default removal must use the frozen catalog name");
        }
    }

    @Test
    void bothReadersRetainTheConstraintNameForASequenceBackedDefault() {
        Catalog catalog = new Catalog("DF_events_id", true);

        for (SchemaSnapshot snapshot : List.of(catalog.snapshot(), catalog.reactiveReader.readSnapshot(TABLE).block())) {
            ColumnDefinition column = snapshot.completeTable().orElseThrow().columns().getFirst();
            assertEquals("DF_events_id", column.defaultConstraintName());
            assertEquals(ColumnDefault.none(), column.defaultValue());
            assertEquals(ValueGeneration.sequence("event_seq"), column.generation());
        }
    }

    @Test
    void otherDialectSnapshotsDoNotImportASqlServerOnlyCatalogFact() {
        for (InformationSchemaFormMetadataReader.SnapshotDialect dialect
                : InformationSchemaFormMetadataReader.SnapshotDialect.values()) {
            if (dialect == InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER) {
                continue;
            }
            SchemaSnapshot snapshot = FormMetadataRowConverter.toCompleteSchemaSnapshot(TABLE,
                    List.of(Map.of("COLUMN_NAME", "id", "DATA_TYPE", "BIGINT",
                            "NULLABLE", "YES", "DEFAULT_CONSTRAINT_NAME", "DF_events_id")),
                    List.of(Map.of("TABLE_REPRESENTABLE", true)), List.of(), List.of(), List.of(),
                    List.of(), List.of(), type -> type, dialect);
            assertNull(snapshot.completeTable().orElseThrow().columns().getFirst().defaultConstraintName());
        }
    }

    private static RelationalTableDefinition desired(ColumnDefault defaultValue) {
        return RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").build())
                .addColumn(ColumnDefinition.builder("recorded_at", "DATETIMEOFFSET")
                        .temporalPrecision(7).defaultValue(defaultValue).build())
                .build();
    }

    private static final class Catalog {
        private final AtomicReference<String> defaultName;
        private final boolean sequenceDefault;
        private final JdbcFormMetadataReader jdbcReader;
        private final ReactiveFormMetadataReader reactiveReader;
        private final JdbcSchemaClient jdbc;
        private final ReactiveSchemaClient reactive;

        private Catalog(String defaultName) {
            this(defaultName, false);
        }

        private Catalog(String defaultName, boolean sequenceDefault) {
            this.defaultName = new AtomicReference<>(defaultName);
            this.sequenceDefault = sequenceDefault;
            SyncSqlExecutor syncExecutor = new SyncSqlExecutor() {
                @Override
                public List<DynamicRow> query(SqlRequest request) {
                    return rows(request);
                }

                @Override
                public long rowsUpdated(SqlRequest request) {
                    throw new AssertionError("metadata review must not execute DDL");
                }

                @Override
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                    throw new AssertionError("metadata review must not request generated keys");
                }
            };
            ReactiveSqlExecutor reactiveExecutor = new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.defer(() -> Flux.fromIterable(rows(request)));
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new AssertionError("metadata review must not execute DDL"));
                }
            };
            jdbcReader = JdbcFormMetadataReaders.create(syncExecutor, DIALECT);
            reactiveReader = ReactiveFormMetadataReaders.create(reactiveExecutor, DIALECT);
            jdbc = JdbcSchemaClient.create(syncExecutor, DIALECT);
            reactive = ReactiveSchemaClient.create(reactiveExecutor, DIALECT);
        }

        private SchemaSnapshot snapshot() {
            return jdbcReader.readSnapshot(TABLE);
        }

        private List<ReviewedSchemaPlan> reviews(RelationalTableDefinition desired) {
            return List.of(jdbc.reviewRelational(DATABASE, desired, jdbcReader, SchemaCompatibilityMode.EXACT),
                    reactive.reviewRelational(DATABASE, desired, reactiveReader, SchemaCompatibilityMode.EXACT).block());
        }

        private List<DynamicRow> rows(SqlRequest request) {
            if (request.sql().contains("from INFORMATION_SCHEMA.COLUMNS c")) {
                if (sequenceDefault) {
                    return List.of(row("COLUMN_NAME", "id", "DATA_TYPE", "bigint", "NULLABLE", "YES",
                            "COLUMN_DEFAULT", "(NEXT VALUE FOR [dbo].[event_seq])", "RESOLUTION_SCHEMA", "dbo",
                            "GENERATION_EXPRESSION", "(NEXT VALUE FOR [dbo].[event_seq])",
                            "GENERATION_START", 1L, "GENERATION_INCREMENT", 1L, "GENERATION_CACHE", 100,
                            "DEFAULT_CONSTRAINT_NAME", defaultName.get()));
                }
                return List.of(row("COLUMN_NAME", "id", "DATA_TYPE", "bigint", "NULLABLE", "YES"),
                        row("COLUMN_NAME", "recorded_at", "DATA_TYPE", "datetimeoffset", "NULLABLE", "YES",
                                "TEMPORAL_PRECISION", 7, "COLUMN_DEFAULT", "(CURRENT_TIMESTAMP)",
                                "GENERATION_EXPRESSION", "(CURRENT_TIMESTAMP)",
                                "DEFAULT_CONSTRAINT_NAME", defaultName.get()));
            }
            return request.sql().contains("TABLE_COMMENT")
                    ? List.of(row("TABLE_REPRESENTABLE", true)) : List.of();
        }
    }

    private static DynamicRow row(Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) {
            row.put((String) values[index], values[index + 1]);
        }
        return DynamicRow.copyOf(row);
    }
}
