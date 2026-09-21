package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompletePhysicalTypeRoundTripTest {

    @Test
    void identicalColumnsRetainTheirLogicalOrPhysicalSnapshotInterpretation() {
        RdbDialect dialect = RdbDialect.sqlServer();
        for (SchemaSnapshot physical : readSnapshots(dialect, "VARCHAR", "VARCHAR", 32)) {
            RelationalTableDefinition table = physical.completeTable().orElseThrow();
            SchemaSnapshot logical = SchemaSnapshot.present(table);
            assertEquals(RelationalMetadataFingerprint.of(table),
                    RelationalMetadataFingerprint.of(logical.completeTable().orElseThrow()));
            assertTrue(SchemaDiffer.diff(table, logical, dialect.capabilities(),
                    SchemaCompatibilityMode.EXACT, dialect.name(), dialect.schema()).operations().isEmpty());
            assertEquals(List.of(SchemaOperation.Kind.CHANGE_COLUMN),
                    SchemaDiffer.diff(table, physical, dialect.capabilities(), SchemaCompatibilityMode.EXACT,
                            dialect.name(), dialect.schema()).operations().stream().map(SchemaOperation::kind).toList());
        }
    }

    @Test
    void snapshotFingerprintBindsTypeInterpretationAcrossBothReaders() {
        List<SchemaSnapshot> snapshots = readSnapshots(RdbDialect.sqlServer(), "VARCHAR", "VARCHAR", 32);
        assertEquals(SchemaSnapshotFingerprint.of(snapshots.getFirst()),
                SchemaSnapshotFingerprint.of(snapshots.getLast()));
        for (SchemaSnapshot physical : snapshots) {
            SchemaSnapshot logical = SchemaSnapshot.present(physical.completeTable().orElseThrow());
            assertNotEquals(SchemaSnapshotFingerprint.of(logical), SchemaSnapshotFingerprint.of(physical));
        }
    }

    @Test
    void logicalSnapshotNullabilityChangeDoesNotInventATypeChange() {
        RdbDialect dialect = RdbDialect.postgresql();
        RelationalTableDefinition actual = RelationalTableDefinition.builder(RelationIdentity.table("types"))
                .addColumn(ColumnDefinition.builder("value", "VARCHAR").build()).build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(RelationIdentity.table("types"))
                .addColumn(ColumnDefinition.builder("value", "VARCHAR").nullable(false).build()).build();
        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of(dialect.name(), "test", dialect), desired, SchemaSnapshot.present(actual),
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);

        assertFalse(plan.requiresManualAction());
        assertEquals(1, plan.requests().size());
        assertTrue(plan.requests().getFirst().sql().contains("set not null"));
    }

    @Test
    void changedSnapshotTypeOriginFailsExecutionPreconditionsBeforeSql() {
        for (SchemaSnapshot physical : readSnapshots(RdbDialect.sqlServer(), "VARCHAR", "VARCHAR", 32)) {
            RelationalTableDefinition actual = physical.completeTable().orElseThrow();
            SchemaSnapshot logical = SchemaSnapshot.present(actual);
            RelationalTableDefinition desired = RelationalTableDefinition.builder(actual.identity())
                    .addColumn(actual.columns().getFirst())
                    .addColumn(ColumnDefinition.builder("note", "INTEGER").build()).build();
            ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(logical, desired,
                    new SqlRequest("alter table types add note integer", List.of()));
            VerifiedSchemaPlanFixtures.SyncExecutor jdbc = new VerifiedSchemaPlanFixtures.SyncExecutor(ignored -> 1L);
            SchemaExecutionReport jdbcReport = VerifiedSchemaPlanExecutor.executeJdbc(plan, jdbc,
                    () -> physical, SchemaSnapshotCoverage::complete, () -> { }, SqlExecutionOptions.safeDefaults());
            assertEquals(SchemaExecutionStatus.PRECONDITION_FAILED, jdbcReport.status());
            assertTrue(jdbc.requests.isEmpty());
            VerifiedSchemaPlanFixtures.ReactiveExecutor reactive =
                    new VerifiedSchemaPlanFixtures.ReactiveExecutor(ignored -> Mono.just(1L));
            SchemaExecutionReport reactiveReport = VerifiedSchemaPlanExecutor.executeReactive(plan, reactive,
                    () -> Mono.just(physical), SchemaSnapshotCoverage::complete,
                    () -> { }, SqlExecutionOptions.safeDefaults()).block();
            assertEquals(SchemaExecutionStatus.PRECONDITION_FAILED, reactiveReport.status());
            assertTrue(reactive.requests.isEmpty());
        }
    }

    @Test
    void sqlServerVarcharDoesNotMasqueradeAsNvarchar() {
        RdbDialect dialect = RdbDialect.sqlServer();
        ColumnDefinition actual = read(dialect, "varchar", "varchar", 32);
        assertFalse(SchemaDefinitionEquality.sameColumnType(actual,
                ColumnDefinition.builder("value", "VARCHAR").length(32).build(), dialect.schema()));
        assertTrue(SchemaDefinitionEquality.sameColumnType(
                read(dialect, "nvarchar", "nvarchar", 32),
                ColumnDefinition.builder("value", "VARCHAR").length(32).build(), dialect.schema()));
    }

    @Test
    void sqlServerUniqueidentifierConverges() {
        assertRoundTrip(RdbDialect.sqlServer(), "uniqueidentifier", "uniqueidentifier", null, "UNIQUEIDENTIFIER");
    }

    @Test
    void oracleRawHashConverges() {
        assertRoundTrip(RdbDialect.oracle(), "RAW", "RAW", 32, "PROTECTED_HASH");
    }

    @Test
    void sqlServerBinaryHashConverges() {
        assertRoundTrip(RdbDialect.sqlServer(), "binary", "binary", 32, "PROTECTED_HASH");
    }

    @Test
    void mysqlBlobCapacityIsNotATypeArgument() {
        assertRoundTrip(RdbDialect.mysql(), "blob", "blob", 65_535, "MYSQL_BLOB");
    }

    @Test
    void h2BinaryLargeObjectConvergesWithBlob() {
        assertRoundTrip(RdbDialect.h2(), "BINARY LARGE OBJECT", "BINARY LARGE OBJECT", null, "BLOB");
    }

    @Test
    void h2BinaryLargeObjectConvergesWithProtectedBinary() {
        assertRoundTrip(RdbDialect.h2(), "BINARY LARGE OBJECT", "BINARY LARGE OBJECT", null, "PROTECTED_BINARY");
    }

    @Test
    void h2CharacterLargeObjectConvergesWithClob() {
        assertRoundTrip(RdbDialect.h2(), "CHARACTER LARGE OBJECT", "CHARACTER LARGE OBJECT", null, "CLOB");
    }

    @Test
    void h2BoundedBlobDiffersFromBareBlob() {
        assertH2LargeObjectDiff("BINARY LARGE OBJECT", "BLOB", true);
    }

    @Test
    void h2BoundedBlobConvergesWithEqualCapacity() {
        assertH2LargeObjectDiff("BINARY LARGE OBJECT", "BLOB(32)", false);
    }

    @Test
    void h2BoundedBlobDiffersFromAnotherCapacity() {
        assertH2LargeObjectDiff("BINARY LARGE OBJECT", "BLOB(64)", true);
    }

    @Test
    void h2BoundedClobDiffersFromBareClob() {
        assertH2LargeObjectDiff("CHARACTER LARGE OBJECT", "CLOB", true);
    }

    @Test
    void h2BoundedClobConvergesWithEqualCapacity() {
        assertH2LargeObjectDiff("CHARACTER LARGE OBJECT", "CLOB(32)", false);
    }

    @Test
    void h2BoundedClobDiffersFromAnotherCapacity() {
        assertH2LargeObjectDiff("CHARACTER LARGE OBJECT", "CLOB(64)", true);
    }

    @Test
    void h2LongBlobCapacityDiffersFromBareBlob() {
        assertH2LargeObjectDiff("BINARY LARGE OBJECT", 3_000_000_000L, "BLOB", true);
    }

    @Test
    void h2LongBlobCapacityConvergesWithEqualCapacity() {
        assertH2LargeObjectDiff("BINARY LARGE OBJECT", 3_000_000_000_000L, "BLOB(3000000000000)", false);
        assertH2LargeObjectDiff("BINARY LARGE OBJECT", 3_000_000_000L, "BLOB(3000000000)", false);
    }

    @Test
    void h2LongBlobCapacityDiffersFromAnotherCapacity() {
        assertH2LargeObjectDiff("BINARY LARGE OBJECT", 3_000_000_000L, "BLOB(3000000001)", true);
    }

    @Test
    void h2LongClobCapacityDiffersFromBareClob() {
        assertH2LargeObjectDiff("CHARACTER LARGE OBJECT", 3_000_000_000L, "CLOB", true);
    }

    @Test
    void h2LongClobCapacityConvergesWithEqualCapacity() {
        assertH2LargeObjectDiff("CHARACTER LARGE OBJECT", 3_000_000_000L, "CLOB(3000000000)", false);
    }

    @Test
    void h2LongClobCapacityDiffersFromAnotherCapacity() {
        assertH2LargeObjectDiff("CHARACTER LARGE OBJECT", 3_000_000_000L, "CLOB(3000000001)", true);
    }

    @Test
    void h2UnboundedLargeObjectSentinelConvergesWithBareTypes() {
        assertH2LargeObjectDiff("BINARY LARGE OBJECT", Long.MAX_VALUE, "BLOB", false);
        assertH2LargeObjectDiff("CHARACTER LARGE OBJECT", Long.MAX_VALUE, "CLOB", false);
    }

    @Test
    void h2NumericConvergesWithDecimalWithoutLosingPrecisionAndScale() {
        ColumnDefinition actual = read(RdbDialect.h2(), "NUMERIC", "NUMERIC", null);
        assertEquals(12, actual.precision());
        assertEquals(2, actual.scale());
        assertTrue(SchemaDefinitionEquality.sameColumnType(actual,
                ColumnDefinition.builder("value", "DECIMAL").precision(12).scale(2).build(),
                RdbDialect.h2().schema()));
        assertFalse(SchemaDefinitionEquality.sameColumnType(actual,
                ColumnDefinition.builder("value", "DECIMAL").precision(12).scale(3).build(),
                RdbDialect.h2().schema()));
    }

    @Test
    void sqlServerTimestampConvergesWithRowversion() {
        assertRoundTrip(RdbDialect.sqlServer(), "timestamp", "timestamp", 8, "ROWVERSION");
    }

    @Test
    void fixedAndVaryingBinaryTypesRemainDistinct() {
        for (RdbDialect dialect : List.of(RdbDialect.h2(), RdbDialect.mysql(), RdbDialect.sqlServer())) {
            assertFalse(SchemaDefinitionEquality.sameColumnType(
                    read(dialect, "BINARY", "BINARY", 32),
                    ColumnDefinition.builder("value", "VARBINARY").length(32).build(), dialect.schema()));
        }
    }

    @Test
    void mysqlCharRetainsItsLengthAndPhysicalIdentity() {
        RdbDialect dialect = RdbDialect.mysql();
        ColumnDefinition actual = read(dialect, "char", "char(32)", 32);
        assertEquals("CHAR(32)", actual.databaseType().declaration());
        assertTrue(SchemaDefinitionEquality.sameColumnType(actual,
                ColumnDefinition.builder("value", "CHAR").length(32).build(), dialect.schema()));
        assertFalse(SchemaDefinitionEquality.sameColumnType(actual,
                ColumnDefinition.builder("value", "VARCHAR").length(32).build(), dialect.schema()));
    }

    @Test
    void h2NormalizesPhysicalNameWhitespaceWithoutLosingLength() {
        ColumnDefinition actual = read(RdbDialect.h2(), "CHARACTER VARYING", " character   varying ", 32);
        assertEquals("CHARACTER VARYING", actual.databaseType().declaration());
        assertTrue(SchemaDefinitionEquality.sameColumnType(actual,
                ColumnDefinition.builder("value", "VARCHAR").length(32).build(), RdbDialect.h2().schema()));
    }

    @Test
    void h2AliasComparisonPreservesTemporalPrecisionPosition() {
        assertRoundTrip(RdbDialect.h2(), "TIMESTAMP(3) WITH TIME ZONE",
                "TIMESTAMP(3) WITH TIME ZONE", null, "TIMESTAMPTZ(3)");
    }

    @Test
    void oracleLogicalNumberProjectionDoesNotReplacePhysicalNumber() {
        RdbDialect dialect = RdbDialect.oracle();
        ColumnDefinition actual = read(dialect, "BIGINT", "NUMBER", null);
        assertEquals("NUMBER", actual.databaseType().declaration());
        assertEquals(19, actual.precision());
        assertEquals(0, actual.scale());
        assertTrue(SchemaDefinitionEquality.sameColumnType(actual,
                ColumnDefinition.builder("value", "BIGINT").build(), dialect.schema()));
    }

    @Test
    void postgresqlNamedSequenceReadBackCanBeReviewedAndRenderedAgain() {
        RdbDialect dialect = RdbDialect.postgresql();
        ValueGeneration generation = ValueGeneration.sequence("public.types_seq", 7, 3, 32);
        Map<String, Object> column = new LinkedHashMap<>();
        column.put("COLUMN_NAME", "value");
        column.put("DATA_TYPE", "bigint");
        column.put("PHYSICAL_DATA_TYPE", "bigint");
        column.put("PHYSICAL_TYPE_SCHEMA", "pg_catalog");
        column.put("PHYSICAL_TYPE_NAME", "int8");
        column.put("PHYSICAL_ARRAY", false);
        column.put("NULLABLE", false);
        column.put("COLUMN_REPRESENTABLE", true);
        column.put("GENERATION_EXPRESSION", "nextval('public.types_seq'::regclass)");
        column.put("GENERATION_START", 7L);
        column.put("GENERATION_INCREMENT", 3L);
        column.put("GENERATION_CACHE", 32);
        RelationalTableDefinition desired = RelationalTableDefinition.builder(RelationIdentity.table("types"))
                .addColumn(ColumnDefinition.builder("value", "BIGINT")
                        .nullable(false).generation(generation).build()).build();
        DatabaseDescriptor database = DatabaseDescriptor.of("PostgreSQL", "17", dialect);
        ReactiveSqlExecutor reactiveExecutor = reactiveExecutor(column);
        var reactiveReader = ReactiveFormMetadataReaders.create(reactiveExecutor, dialect);
        SyncSqlExecutor jdbcExecutor = jdbcExecutor(column);
        var jdbcReader = JdbcFormMetadataReaders.create(jdbcExecutor, dialect);

        for (SchemaSnapshot snapshot : List.of(reactiveReader.readSnapshot("types").block(),
                jdbcReader.readSnapshot("types"))) {
            ColumnDefinition actual = snapshot.completeTable().orElseThrow().columns().getFirst();
            assertEquals("pg_catalog.int8", actual.databaseType().canonical());
            assertEquals(generation, actual.generation());
            assertEquals(dialect.schema().createSequenceSql(generation, "BIGINT"),
                    dialect.schema().createSequenceSql(generation, actual.databaseType().declaration()));
            assertEquals(dialect.schema().generatedValueClause(generation, "BIGINT"),
                    dialect.schema().generatedValueClause(generation, actual.databaseType().declaration()));
        }

        ReviewedSchemaPlan jdbcPlan = JdbcSchemaClient.create(jdbcExecutor, dialect)
                .reviewRelational(database, desired, jdbcReader, SchemaCompatibilityMode.EXACT);
        ReviewedSchemaPlan reactivePlan = ReactiveSchemaClient.create(reactiveExecutor, dialect)
                .reviewRelational(database, desired, reactiveReader, SchemaCompatibilityMode.EXACT).block();
        for (ReviewedSchemaPlan plan : List.of(jdbcPlan, reactivePlan)) {
            assertFalse(plan.requiresManualAction());
            assertTrue(plan.steps().isEmpty(), plan.operations().toString());
            assertTrue(plan.requests().isEmpty());
        }
    }

    @Test
    void oracleFloatBinaryPrecisionSurvivesCompleteReaderRoundTrip() {
        RdbDialect dialect = RdbDialect.oracle();
        List<SchemaSnapshot> float10 = readSnapshots(dialect, oracleFloatColumn(10));
        List<SchemaSnapshot> float20 = readSnapshots(dialect, oracleFloatColumn(20));
        for (int reader = 0; reader < float10.size(); reader++) {
            SchemaSnapshot actual = float10.get(reader);
            ColumnDefinition column = actual.completeTable().orElseThrow().columns().getFirst();
            assertEquals("FLOAT(10)", dialect.schema().physicalDataType(
                    column.databaseType().declaration(), column.length(), column.precision(), column.scale()));
            RelationalTableDefinition desired = RelationalTableDefinition.builder(actual.identity())
                    .addColumn(ColumnDefinition.builder("value", "FLOAT(10)").build()).build();
            assertTrue(SchemaDiffer.diff(desired, actual, dialect.capabilities(),
                    SchemaCompatibilityMode.EXACT, dialect.name(), dialect.schema()).operations().isEmpty());
            assertNotEquals(SchemaSnapshotFingerprint.of(actual),
                    SchemaSnapshotFingerprint.of(float20.get(reader)));
            assertEquals(List.of(SchemaOperation.Kind.CHANGE_COLUMN),
                    SchemaDiffer.diff(desired, float20.get(reader), dialect.capabilities(),
                            SchemaCompatibilityMode.EXACT, dialect.name(), dialect.schema())
                            .operations().stream().map(SchemaOperation::kind).toList());
        }
    }

    @Test
    void oracleDefaultFloatPrecisionIsEquivalentInBothDirections() {
        RdbDialect dialect = RdbDialect.oracle();
        assertTrue(dialect.schema().sameDataType("FLOAT", "FLOAT(126)"));
        assertTrue(dialect.schema().sameDataType("FLOAT(126)", "FLOAT"));
        for (SchemaSnapshot actual : readSnapshots(dialect, oracleFloatColumn(126))) {
            for (String declaration : List.of("FLOAT", "FLOAT(126)")) {
                RelationalTableDefinition desired = RelationalTableDefinition.builder(actual.identity())
                        .addColumn(ColumnDefinition.builder("value", declaration).build()).build();
                assertTrue(SchemaDiffer.diff(desired, actual, dialect.capabilities(),
                        SchemaCompatibilityMode.EXACT, dialect.name(), dialect.schema()).operations().isEmpty(),
                        declaration);
            }
        }
    }

    private static Map<String, Object> oracleFloatColumn(int precision) {
        Map<String, Object> column = new LinkedHashMap<>();
        column.put("COLUMN_NAME", "value");
        column.put("DATA_TYPE", "FLOAT");
        column.put("PHYSICAL_DATA_TYPE", "FLOAT");
        column.put("NUMERIC_PRECISION", precision);
        column.put("NULLABLE", true);
        column.put("COLUMN_REPRESENTABLE", true);
        return column;
    }

    private static void assertRoundTrip(RdbDialect dialect, String logical, String physical,
                                        Integer length, String desired) {
        ColumnDefinition actual = read(dialect, logical, physical, length);
        assertTrue(SchemaDefinitionEquality.sameColumnType(actual,
                ColumnDefinition.builder("value", desired).build(), dialect.schema()),
                () -> actual.databaseType().declaration() + " / " + desired);
    }

    private static ColumnDefinition read(RdbDialect dialect, String logical,
                                          String physical, Integer length) {
        return readSnapshots(dialect, logical, physical, length).getFirst().columns().value().getFirst();
    }

    private static void assertH2LargeObjectDiff(String physical, String desiredType, boolean changed) {
        assertH2LargeObjectDiff(physical, 32, desiredType, changed);
    }

    private static void assertH2LargeObjectDiff(String physical, Number length,
                                               String desiredType, boolean changed) {
        RdbDialect dialect = RdbDialect.h2();
        for (SchemaSnapshot actual : readSnapshots(dialect, physical, physical, length)) {
            RelationalTableDefinition desired = RelationalTableDefinition.builder(actual.identity())
                    .addColumn(ColumnDefinition.builder("value", desiredType).build()).build();
            SchemaCompatibilityReport report = SchemaDiffer.diff(desired, actual, dialect.capabilities(),
                    SchemaCompatibilityMode.EXACT, dialect.name(), dialect.schema());
            assertEquals(changed ? List.of(SchemaOperation.Kind.CHANGE_COLUMN) : List.of(),
                    report.operations().stream().map(SchemaOperation::kind).toList());
        }
    }

    private static List<SchemaSnapshot> readSnapshots(RdbDialect dialect, String logical,
                                                       String physical, Number length) {
        Map<String, Object> column = new LinkedHashMap<>();
        column.put("COLUMN_NAME", "value");
        column.put("DATA_TYPE", logical);
        column.put("PHYSICAL_DATA_TYPE", physical);
        column.put("CHARACTER_MAXIMUM_LENGTH", length);
        column.put("NULLABLE", true);
        if ("NUMBER".equals(physical)) {
            column.put("NUMERIC_PRECISION", 19);
            column.put("NUMERIC_SCALE", 0);
        } else if ("NUMERIC".equals(physical)) {
            column.put("NUMERIC_PRECISION", 12);
            column.put("NUMERIC_SCALE", 2);
        }
        return readSnapshots(dialect, column);
    }

    private static List<SchemaSnapshot> readSnapshots(RdbDialect dialect, Map<String, Object> column) {
        SchemaSnapshot reactiveSnapshot = ReactiveFormMetadataReaders.create(reactiveExecutor(column), dialect)
                .readSnapshot("types").block();
        ColumnDefinition actual = reactiveSnapshot.columns().value().getFirst();
        SchemaSnapshot jdbcSnapshot = JdbcFormMetadataReaders.create(jdbcExecutor(column), dialect)
                .readSnapshot("types");
        ColumnDefinition jdbcActual = jdbcSnapshot.columns().value().getFirst();
        assertEquals(actual.databaseType(), jdbcActual.databaseType());
        assertEquals(actual.length(), jdbcActual.length());
        assertEquals(actual.precision(), jdbcActual.precision());
        assertEquals(actual.scale(), jdbcActual.scale());
        assertEquals(actual.temporalPrecision(), jdbcActual.temporalPrecision());
        return List.of(reactiveSnapshot, jdbcSnapshot);
    }

    private static ReactiveSqlExecutor reactiveExecutor(Map<String, Object> column) {
        return new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.fromIterable(rows(request, column));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }
        };
    }

    private static SyncSqlExecutor jdbcExecutor(Map<String, Object> column) {
        return new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                return rows(request, column);
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
    }

    private static List<DynamicRow> rows(SqlRequest request, Map<String, Object> column) {
        if (request.sql().contains("end as DATA_TYPE")) {
            assertTrue(request.sql().contains("as PHYSICAL_DATA_TYPE"));
            return List.of(DynamicRow.copyOf(column));
        }
        return request.sql().contains("TABLE_COMMENT")
                ? List.of(DynamicRow.copyOf(Map.of("TABLE_REPRESENTABLE", true))) : List.of();
    }
}
