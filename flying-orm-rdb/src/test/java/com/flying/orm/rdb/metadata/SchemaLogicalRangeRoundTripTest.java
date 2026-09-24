package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.schema.SchemaCompatibilityMode;
import com.flying.orm.rdb.schema.SchemaDiffer;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaLogicalRangeRoundTripTest {

    @Test
    void logicalBoundsRemainEquivalentAfterMetadataRestoresARange() {
        CheckPredicate observed = RelationalMetadataValueParser.checkPredicate(
                "(\"score\" > 1 and \"score\" <= 10)",
                Map.of("score", DatabaseType.of("INTEGER")),
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL);
        CheckPredicate desired = bounds(1, 10);
        assertTrue(equivalent(observed, desired));
        assertTrue(equivalent(desired, observed));
        assertTrue(equivalent(CheckPredicate.not(observed), CheckPredicate.not(desired)));
    }

    @Test
    void comparisonKeepsBoundsOperatorsAndColumnsDistinct() {
        CheckPredicate observed = CheckPredicate.range("score", 1, false, 10, true);
        assertFalse(equivalent(observed, bounds(2, 10)));
        assertFalse(equivalent(observed, CheckPredicate.and(
                CheckPredicate.compare("score", CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL, 1),
                CheckPredicate.compare("score", CheckPredicate.ComparisonOperator.LESS_THAN_OR_EQUAL, 10))));
        assertFalse(equivalent(observed, CheckPredicate.and(
                CheckPredicate.compare("score", CheckPredicate.ComparisonOperator.GREATER_THAN, 1),
                CheckPredicate.compare("other", CheckPredicate.ComparisonOperator.LESS_THAN_OR_EQUAL, 10))));
        assertFalse(equivalent(observed, CheckPredicate.or(
                CheckPredicate.compare("score", CheckPredicate.ComparisonOperator.GREATER_THAN, 1),
                CheckPredicate.compare("score", CheckPredicate.ComparisonOperator.LESS_THAN_OR_EQUAL, 10))));
    }

    private static CheckPredicate bounds(int lower, int upper) {
        return CheckPredicate.and(
                CheckPredicate.compare("score", CheckPredicate.ComparisonOperator.GREATER_THAN, lower),
                CheckPredicate.compare("score", CheckPredicate.ComparisonOperator.LESS_THAN_OR_EQUAL, upper));
    }

    private static boolean equivalent(CheckPredicate observed, CheckPredicate desired) {
        return SchemaDiffer.diff(table(desired), SchemaSnapshot.present(table(observed)),
                RdbDialect.postgresql().capabilities(), SchemaCompatibilityMode.EXACT).exact();
    }

    private static RelationalTableDefinition table(CheckPredicate predicate) {
        return RelationalTableDefinition.builder(RelationIdentity.table("scores"))
                .addColumn(ColumnDefinition.builder("score", "INTEGER").build())
                .addColumn(ColumnDefinition.builder("other", "INTEGER").build())
                .addCheck(CheckConstraintDefinition.of("ck_score", predicate)).build();
    }
}
