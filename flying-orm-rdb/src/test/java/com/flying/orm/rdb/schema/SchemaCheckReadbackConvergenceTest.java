package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.sql.Connection;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.IntStream;

import static com.flying.orm.core.metadata.CheckPredicate.ComparisonOperator.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaCheckReadbackConvergenceTest {

    @TestFactory
    Stream<DynamicTest> unchangedChecksConvergeAfterDatabaseNormalization() {
        CheckPredicate lower = CheckPredicate.compare("amount", GREATER_THAN_OR_EQUAL, 0);
        CheckPredicate upper = CheckPredicate.compare("amount", LESS_THAN_OR_EQUAL, 10);
        return Stream.concat(
                Stream.of(CheckPredicate.ComparisonOperator.values()).map(operator ->
                        CheckPredicate.not(CheckPredicate.compare("amount", operator, 0))),
                Stream.of(
                        CheckPredicate.not(CheckPredicate.isNull("amount")),
                        CheckPredicate.not(CheckPredicate.not(lower)),
                        CheckPredicate.not(CheckPredicate.range("amount", 0, 10)),
                        CheckPredicate.not(CheckPredicate.and(lower, upper)),
                        CheckPredicate.not(CheckPredicate.or(lower, upper)),
                        CheckPredicate.or(CheckPredicate.compare("amount", EQUAL, 1),
                                CheckPredicate.compare("amount", EQUAL, 2)),
                        CheckPredicate.in("amount", List.of(3, 1, 2)),
                        CheckPredicate.in("amount", List.of(1, 1, 2)),
                        CheckPredicate.in("amount", List.of(1)),
                        CheckPredicate.not(CheckPredicate.in("amount", List.of(1, 2))),
                        CheckPredicate.and(lower, CheckPredicate.and(upper,
                                CheckPredicate.compare("amount", NOT_EQUAL, 5)))))
                .map(predicate -> DynamicTest.dynamicTest(predicate.toString(), () -> verifyConvergence(predicate)));
    }

    private static void verifyConvergence(CheckPredicate predicate) throws Exception {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:check_readback_" + UUID.randomUUID().toString().replace("-", ""));
        source.setUser("sa");
        RdbDialect dialect = RdbDialect.h2();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(RelationIdentity.table("events"))
                .addColumn(ColumnDefinition.builder("amount", "BIGINT").build())
                .addCheck(CheckConstraintDefinition.of("ck_amount", predicate)).build();
        try (Connection keeper = source.getConnection()) {
            SyncSqlExecutor jdbc = SyncSqlExecutor.jdbc(ConnectionAccessTestSupport.jdbc(source), dialect);
            RelationalSchemaSqlRenderer.create(dialect.schema()).render(SchemaOperation.of(
                    SchemaOperation.Kind.CREATE_TABLE, desired.identity(), "events", null, desired,
                    SchemaOperation.Compatibility.SAFE_INCREMENTAL)).forEach(jdbc::rowsUpdated);
            ReviewedSchemaPlan plan = JdbcSchemaClient.create(jdbc, dialect).reviewRelational(
                    DatabaseDescriptor.of("H2", "2.4.240", dialect), desired,
                    JdbcFormMetadataReaders.create(jdbc, dialect), SchemaCompatibilityMode.EXACT);
            assertTrue(plan.operations().isEmpty(), () -> plan.operations().toString());
        }
    }

    @Test
    void differentChecksStillRequireMigration() {
        assertDifferent(CheckPredicate.compare("amount", LESS_THAN, 1),
                CheckPredicate.compare("amount", LESS_THAN_OR_EQUAL, 1));
        assertDifferent(CheckPredicate.in("amount", List.of(1, 2)), CheckPredicate.in("amount", List.of(1, 3)));
        assertDifferent(CheckPredicate.isNull("amount"), CheckPredicate.isNotNull("amount"));
        assertDifferent(CheckPredicate.range("amount", 0, 10), CheckPredicate.range("amount", 0, false, 10, true));
        CheckPredicate a = CheckPredicate.compare("amount", GREATER_THAN, 0);
        CheckPredicate b = CheckPredicate.compare("amount", LESS_THAN, 10);
        assertDifferent(CheckPredicate.and(a, b), CheckPredicate.or(a, b));
        assertDifferent(a, CheckPredicate.not(a));
    }

    @Test
    void eachNormalizedTermMustMatchADistinctTerm() {
        assertDifferent(CheckPredicate.in("amount", List.of(
                        OffsetDateTime.parse("2026-01-01T00:00:00Z"),
                        OffsetDateTime.parse("2026-01-01T08:00:00+08:00"))),
                CheckPredicate.in("amount", List.of(
                        Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-02T00:00:00Z"))));
    }

    @Test
    void unchangedInValuesAllowDifferentNumericCarriers() {
        CheckPredicate left = CheckPredicate.in("amount", IntStream.range(0, 10000).boxed().toList());
        CheckPredicate right = CheckPredicate.in("amount", IntStream.range(0, 10000).mapToObj(value -> (long) value).toList());
        assertTrue(SchemaDefinitionEquality.sameCheck(CheckConstraintDefinition.of("ck_amount", left),
                CheckConstraintDefinition.of("ck_amount", right), RdbDialect.h2().schema()));
    }

    @Test
    void reorderedInValuesDoNotRequirePairwiseNumericConversions() {
        assertLinearNumericComparison(false);
    }

    @Test
    void nestedInValuesDoNotRequirePairwiseNumericConversions() {
        assertLinearNumericComparison(true);
    }

    @Test
    void hashCollisionsStillCompareLiteralsAndPreserveDistinctTerms() {
        CheckPredicate left = CheckPredicate.in("amount", List.of("Aa", "BB"));
        CheckPredicate reordered = CheckPredicate.in("amount", List.of("BB", "Aa"));
        assertTrue(sameCheck(left, reordered, RdbDialect.h2()));
        assertDifferent(left, CheckPredicate.in("amount", List.of("Aa", "Aa")));
    }

    @TestFactory
    Stream<DynamicTest> numericBooleanHashingFollowsDialectSemantics() {
        return Stream.of(RdbDialect.h2(), RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer()).map(dialect ->
                DynamicTest.dynamicTest(dialect.name(), () -> {
                    CheckPredicate flags = CheckPredicate.in("amount", List.of(true, false));
                    CheckPredicate numbers = CheckPredicate.in("amount", List.of(0, BigDecimal.ONE));
                    boolean numeric = switch (dialect.schema().generatedValueStyle()) {
                        case MYSQL, ORACLE, SQL_SERVER -> true;
                        default -> false;
                    };
                    assertEquals(numeric, sameCheck(flags, numbers, dialect));
                    assertFalse(sameCheck(flags, CheckPredicate.in("amount", List.of(0, 2)), dialect));
                }));
    }

    @Test
    void temporalHashCollisionsRespectOffsetSemantics() {
        CheckPredicate first = CheckPredicate.in("amount", List.of(
                OffsetDateTime.parse("2026-01-01T00:00:00Z"),
                OffsetDateTime.parse("2026-01-02T00:00:00Z")));
        CheckPredicate shifted = CheckPredicate.in("amount", List.of(
                OffsetDateTime.parse("2026-01-02T08:00:00+08:00"),
                OffsetDateTime.parse("2026-01-01T08:00:00+08:00")));
        assertTrue(sameCheck(first, shifted, RdbDialect.postgresql()));
        assertFalse(sameCheck(first, shifted, RdbDialect.h2()));
    }

    private static boolean sameCheck(CheckPredicate left, CheckPredicate right, RdbDialect dialect) {
        return SchemaDefinitionEquality.sameCheck(CheckConstraintDefinition.of("ck_amount", left),
                CheckConstraintDefinition.of("ck_amount", right), dialect.schema());
    }

    private static void assertLinearNumericComparison(boolean nested) {
        int count = 500;
        AtomicInteger conversions = new AtomicInteger();
        CheckPredicate left = CheckPredicate.in("amount", IntStream.range(0, count).boxed().toList());
        CheckPredicate right = CheckPredicate.in("AMOUNT", IntStream.range(0, count)
                .mapToObj(value -> new CountingDecimal(count - value - 1, conversions)).toList());
        if (nested) {
            left = CheckPredicate.and(left, CheckPredicate.isNotNull("amount"));
            right = CheckPredicate.and(CheckPredicate.isNotNull("AMOUNT"), right);
        }
        assertTrue(SchemaDefinitionEquality.sameCheck(CheckConstraintDefinition.of("ck_amount", left),
                CheckConstraintDefinition.of("ck_amount", right), RdbDialect.h2().schema()));
        assertTrue(conversions.get() < count * 20,
                () -> "numeric conversions must grow linearly, actual: " + conversions.get());
    }

    private static final class CountingDecimal extends BigDecimal {
        private static final long serialVersionUID = 1L;
        private final AtomicInteger conversions;

        private CountingDecimal(int value, AtomicInteger conversions) {
            super(value + ".00");
            this.conversions = conversions;
        }

        @Override
        public String toString() {
            conversions.incrementAndGet();
            return super.toString();
        }
    }

    private static void assertDifferent(CheckPredicate left, CheckPredicate right) {
        assertFalse(SchemaDefinitionEquality.sameCheck(CheckConstraintDefinition.of("ck_amount", left),
                CheckConstraintDefinition.of("ck_amount", right), RdbDialect.h2().schema()));
    }
}
