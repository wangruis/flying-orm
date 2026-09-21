package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import java.lang.reflect.Method;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SchemaObservedTimeTypeRegressionTest {
    private record Case(RdbDialect dialect, String metadataDialect, String version,
                        String physical, String logical) { }
    private static final Case MYSQL = new Case(RdbDialect.mysql(), "MYSQL", "8.0", "VARCHAR(32)", "OFFSET_TIME");
    private static final Case ORACLE_TIME = new Case(RdbDialect.oracle(), "ORACLE", "19c", "VARCHAR2(18)", "TIME");
    private static final Case ORACLE_OFFSET = new Case(RdbDialect.oracle(), "ORACLE", "19c", "VARCHAR2(32)", "OFFSET_TIME");
    private static final Case SQL_SERVER = new Case(RdbDialect.sqlServer(), "SQL_SERVER", "2022", "VARCHAR(32)", "OFFSET_TIME");
    private static final List<Case> CASES = List.of(MYSQL, ORACLE_TIME, ORACLE_OFFSET, SQL_SERVER);

    @Test void nullableOnlyUsesObservedLogicalTypeAcrossMarkerDialects() throws Exception {
        for (Case c : CASES) {
            ReviewedSchemaPlan plan = review(c, observed(c, c.logical(), false, null), column(c.logical(), true, null));
            assertExecutable(plan, c);
            if (c == MYSQL) assertTrue(sql(plan).contains("[[flying-orm:v1:OFFSET_TIME]]"));
        }
    }

    @Test void defaultOnlyDoesNotCountUnchangedMarkerAsSecondChange() throws Exception {
        for (Case c : CASES) {
            Object literal = c == ORACLE_TIME ? LocalTime.parse("12:34:56.123456")
                    : OffsetTime.parse("12:34:56.123456+08:00");
            ColumnDefinition desired = ColumnDefinition.builder("n", c.logical()).nullable(true)
                    .defaultValue(ColumnDefault.literal(literal)).build();
            assertExecutable(review(c, observed(c, c.logical(), true, null), desired), c);
        }
    }

    @Test void userCommentChangesPreserveMarkerAndUpdateExistingSqlServerProperty() throws Exception {
        for (Case c : CASES) {
            for (String[] change : List.of(new String[] {null, "note"},
                    new String[] {"note", "new"}, new String[] {"note", null})) {
                ReviewedSchemaPlan plan = review(c, observed(c, c.logical(), true, change[0]),
                        column(c.logical(), true, change[1]));
                assertExecutable(plan, c);
                assertTrue(sql(plan).contains("[[flying-orm:v1:" + c.logical() + "]]"));
                if (c == SQL_SERVER) {
                    assertTrue(sql(plan).contains("sp_updateextendedproperty"), sql(plan));
                    assertFalse(sql(plan).contains("sp_addextendedproperty"), sql(plan));
                    assertFalse(sql(plan).contains("sp_dropextendedproperty"), sql(plan));
                }
            }
        }
    }

    @Test void unmarkedSqlServerColumnStillAddsFirstUserComment() throws Exception {
        assertTrue(sql(review(SQL_SERVER, observed(SQL_SERVER, "VARCHAR(32)", true, null),
                column("VARCHAR(32)", true, "note"))).contains("sp_addextendedproperty"));
    }

    @Test void samePhysicalUnmarkedToMarkedTypeIsNotAnEmptyPlan() throws Exception {
        assertManual(review(MYSQL, observed(MYSQL, "VARCHAR(32)", true, null),
                column("OFFSET_TIME", true, null)));
    }

    @Test void samePhysicalMarkedToUnmarkedTypeIsNotAnEmptyPlan() throws Exception {
        assertManual(review(MYSQL, observed(MYSQL, "OFFSET_TIME", true, null),
                column("VARCHAR(32)", true, null)));
    }

    @Test void wideningMustNotSilentlyRemoveTimeSemantics() throws Exception {
        assertManual(review(MYSQL, observed(MYSQL, "OFFSET_TIME", true, null),
                column("VARCHAR(64)", true, null)));
        assertManual(review(ORACLE_TIME, observed(ORACLE_TIME, "TIME", true, null),
                column("VARCHAR2(32)", true, null)));
    }

    @Test void changingTimeMarkerKindRemainsManual() throws Exception {
        assertManual(review(ORACLE_TIME, observed(ORACLE_TIME, "TIME", true, null),
                column("OFFSET_TIME", true, null)));
    }

    @Test void realMultipleAttributeChangeRemainsManual() throws Exception {
        ColumnDefinition desired = ColumnDefinition.builder("n", "OFFSET_TIME").nullable(true)
                .defaultValue(ColumnDefault.literal(OffsetTime.parse("12:34:56+08:00"))).build();
        assertManual(review(MYSQL, observed(MYSQL, "OFFSET_TIME", false, null), desired));
    }

    @Test void oldPhysicalSnapshotDoesNotGuessDesiredMarker() {
        SchemaSnapshot snapshot = builder(MYSQL).physicalColumns(List.of(column("VARCHAR(32)", true, null))).build();
        assertManual(review(MYSQL, snapshot, column("OFFSET_TIME", true, null)));
    }

    @Test void logicalSnapshotAlreadyKnowsItsActualType() {
        SchemaSnapshot snapshot = builder(MYSQL).columns(List.of(column("OFFSET_TIME", false, null))).build();
        assertExecutable(review(MYSQL, snapshot, column("OFFSET_TIME", true, null)), MYSQL);
    }

    @Test void ordinaryUnmarkedWideningRemainsExecutable() throws Exception {
        assertExecutable(review(MYSQL, observed(MYSQL, "VARCHAR(32)", true, null),
                column("VARCHAR(64)", true, null)), MYSQL);
    }

    @Test void observedMarkerFactChangesFingerprintWithoutChangingPhysicalColumns() throws Exception {
        SchemaSnapshot marked = observed(MYSQL, "OFFSET_TIME", true, null);
        SchemaSnapshot plain = observed(MYSQL, "VARCHAR(32)", true, null);
        SchemaSnapshot unknown = builder(MYSQL).physicalColumns(plain.columns().value()).build();
        assertNotEquals(SchemaSnapshotFingerprint.of(marked), SchemaSnapshotFingerprint.of(plain));
        assertNotEquals(SchemaSnapshotFingerprint.of(marked), SchemaSnapshotFingerprint.of(unknown));
        assertNotEquals(SchemaSnapshotFingerprint.of(plain), SchemaSnapshotFingerprint.of(unknown));
    }

    @Test void compatibleOverloadSnapshotsInputAndClearsFactsOnBuilderReuse() throws Exception {
        List<ColumnDefinition> columns = List.of(column("VARCHAR(32)", true, null));
        Map<String, DatabaseType> types = new LinkedHashMap<>();
        types.put("n", DatabaseType.of("OFFSET_TIME"));
        SchemaSnapshot.Builder builder = withTypes(builder(MYSQL), columns, types);
        SchemaSnapshot marked = builder.build();
        String fingerprint = SchemaSnapshotFingerprint.of(marked);
        types.put("n", DatabaseType.of("VARCHAR(32)"));
        assertEquals(fingerprint, SchemaSnapshotFingerprint.of(builder.build()));
        assertNotEquals(fingerprint, SchemaSnapshotFingerprint.of(withTypes(builder(MYSQL), columns, types).build()));
        SchemaSnapshot reset = builder.physicalColumns(columns).build();
        assertEquals(SchemaSnapshotFingerprint.of(builder(MYSQL).physicalColumns(columns).build()),
                SchemaSnapshotFingerprint.of(reset));
        withTypes(builder, columns, Map.of("n", DatabaseType.of("OFFSET_TIME")));
        assertEquals(SchemaSnapshotFingerprint.of(builder(MYSQL).columns(columns).build()),
                SchemaSnapshotFingerprint.of(builder.columns(columns).build()));
    }

    @Test void observedTypeMapOrderDoesNotChangeFingerprint() throws Exception {
        List<ColumnDefinition> columns = List.of(column("VARCHAR(32)", true, null),
                ColumnDefinition.builder("m", "INTEGER").build());
        Map<String, DatabaseType> first = new LinkedHashMap<>();
        first.put("n", DatabaseType.of("OFFSET_TIME")); first.put("m", DatabaseType.of("INTEGER"));
        Map<String, DatabaseType> second = new LinkedHashMap<>();
        second.put("m", DatabaseType.of("INTEGER")); second.put("n", DatabaseType.of("OFFSET_TIME"));
        assertEquals(SchemaSnapshotFingerprint.of(withTypes(builder(MYSQL), columns, first).build()),
                SchemaSnapshotFingerprint.of(withTypes(builder(MYSQL), columns, second).build()));
    }


    @Test void unknownStorageMarkerDoesNotBecomeAnAbsentComment() {
        for (Case c : CASES) {
            SchemaSnapshot actual = builder(c).physicalColumns(List.of(column(c.physical(), true, null))).build();
            assertManual(review(c, actual, column(c.physical(), true, "note")));
        }
    }

    @Test void unknownStorageMarkerIsNotRemovedWhenReplacingUserComment() {
        for (Case c : CASES) {
            SchemaSnapshot actual = builder(c).physicalColumns(List.of(column(c.physical(), true, "old"))).build();
            assertManual(review(c, actual, column(c.physical(), true, "new")));
        }
    }

    @Test void mysqlFullColumnRewriteCannotDiscardAnUnknownMarker() {
        SchemaSnapshot actual = builder(MYSQL).physicalColumns(List.of(column("VARCHAR(32)", false, null))).build();
        assertManual(review(MYSQL, actual, column("VARCHAR(32)", true, null)));
    }

    @Test void wideningCannotDiscardUnknownMarkerReadbackSemantics() {
        for (Case c : CASES) {
            SchemaSnapshot actual = builder(c).physicalColumns(List.of(column(c.physical(), true, null))).build();
            String target = c == ORACLE_TIME || c == ORACLE_OFFSET ? "VARCHAR2(64)" : "VARCHAR(64)";
            assertManual(review(c, actual, column(target, true, null)));
        }
    }

    @Test void unknownLogicalTypeDoesNotBlockNonMarkerColumnComments() {
        for (Case c : CASES) {
            for (String logical : List.of("INTEGER", "DATE")) {
                String physical = c.dialect().schema().dataType(logical);
                SchemaSnapshot actual = builder(c).physicalColumns(List.of(column(physical, true, null))).build();
                assertExecutable(review(c, actual, column(logical, true, "note")), c);
            }
        }
    }

    @Test void unknownOrdinaryTextTypeDoesNotBlockUnrelatedWidening() {
        for (Case c : CASES) {
            String nativeType = c == ORACLE_TIME || c == ORACLE_OFFSET ? "VARCHAR2" : "VARCHAR";
            SchemaSnapshot actual = builder(c).physicalColumns(List.of(column(nativeType + "(64)", true, null))).build();
            assertExecutable(review(c, actual, column(nativeType + "(128)", true, null)), c);
        }
    }

    @Test void missingObservedColumnEntryKeepsStorageMarkerUnknown() throws Exception {
        SchemaSnapshot actual = withTypes(builder(SQL_SERVER),
                List.of(column("VARCHAR(32)", true, null)), Map.of()).build();
        assertManual(review(SQL_SERVER, actual, column("VARCHAR(32)", true, "note")));
    }


    @Test void unknownMarkerIdentityCannotBeCertifiedAsZeroDifference() {
        for (Case c : CASES) {
            SchemaSnapshot actual = builder(c).physicalColumns(List.of(column(c.physical(), true, null))).build();
            assertManual(review(c, actual, column(c.physical(), true, null)));
        }
    }

    @Test void nonMarkerOracleTextWidthDoesNotLoseExistingCommentSupport() {
        SchemaSnapshot actual = builder(ORACLE_TIME)
                .physicalColumns(List.of(column("VARCHAR2(16)", true, null))).build();
        assertExecutable(review(ORACLE_TIME, actual, column("VARCHAR2(16)", true, "note")), ORACLE_TIME);
    }

    private static SchemaSnapshot.Builder withTypes(SchemaSnapshot.Builder builder,
            List<ColumnDefinition> columns, Map<String, DatabaseType> types) throws Exception {
        Method method = SchemaSnapshot.Builder.class.getMethod("physicalColumns", List.class, Map.class);
        return (SchemaSnapshot.Builder) method.invoke(builder, columns, types);
    }

    private static RelationIdentity identity(Case c) {
        return RelationIdentity.of(null, c == SQL_SERVER ? "dbo" : null, "t");
    }
    private static SchemaSnapshot.Builder builder(Case c) {
        return SchemaSnapshot.builder(identity(c)).tablePresent().tableCommentAbsent().primaryKeyAbsent()
                .uniqueConstraints(List.of()).indexes(List.of()).foreignKeys(List.of()).checks(List.of()).partitionAbsent();
    }
    private static ColumnDefinition column(String type, boolean nullable, String comment) {
        return ColumnDefinition.builder("n", type).nullable(nullable).comment(comment).build();
    }
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static SchemaSnapshot observed(Case c, String logical, boolean nullable, String comment) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("COLUMN_NAME", "n"); row.put("DATA_TYPE", c.physical());
        row.put("PHYSICAL_DATA_TYPE", c.physical()); row.put("LOGICAL_DATA_TYPE", logical);
        row.put("NULLABLE", nullable); row.put("REMARKS", comment);
        Class<?> converter = Class.forName("com.flying.orm.rdb.metadata.FormMetadataRowConverter");
        Method method = Arrays.stream(converter.getDeclaredMethods())
                .filter(m -> m.getName().equals("toCompleteSchemaSnapshot") && m.getParameterCount() == 10)
                .findFirst().orElseThrow();
        method.setAccessible(true);
        Object dialect = Enum.valueOf((Class) method.getParameterTypes()[9], c.metadataDialect());
        return (SchemaSnapshot) method.invoke(null, identity(c), List.of(row), List.of(Map.of()),
                List.of(), List.of(), List.of(), List.of(), List.of(), Function.identity(), dialect);
    }
    private static ReviewedSchemaPlan review(Case c, SchemaSnapshot actual, ColumnDefinition desired) {
        return RelationalSchemaPlanReviewer.create(c.dialect()).review(
                DatabaseDescriptor.of(c.dialect().name(), c.version(), c.dialect()),
                RelationalTableDefinition.builder(identity(c)).addColumn(desired).build(), actual,
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
    }
    private static String sql(ReviewedSchemaPlan plan) {
        return plan.requests().stream().map(r -> r.sql()).reduce("", (a, b) -> a + "\n" + b);
    }
    private static void assertExecutable(ReviewedSchemaPlan plan, Case c) {
        assertFalse(plan.requiresManualAction(), c.metadataDialect() + "/" + c.logical() + ": " + plan.operations());
        assertFalse(plan.requests().isEmpty());
    }
    private static void assertManual(ReviewedSchemaPlan plan) {
        assertFalse(plan.operations().isEmpty(), "marker change must not disappear from diff");
        assertTrue(plan.requiresManualAction(), sql(plan));
        assertTrue(plan.requests().isEmpty(), sql(plan));
    }
}
