package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaTimestampDefaultEquivalenceTest {

    @Test
    void postgresqlDefaultDoesNotChangeWhenReadInAnotherSessionTimeZone() {
        assertTrue(compare(RdbDialect.postgresql(), "2024-01-01T09:30:00+05:30",
                "2024-01-01T12:00:00+08:00").compatible());
    }

    @Test
    void postgresqlStillDetectsDifferentDefaultInstants() {
        assertFalse(compare(RdbDialect.postgresql(), "2024-01-01T09:30:00+05:30",
                "2024-01-01T13:00:00+08:00").compatible());
    }

    @Test
    void postgresqlNullabilityChangeDoesNotAlsoChangeTheEquivalentDefault() {
        ColumnDefinition actual = ColumnDefinition.builder("created_at", "TIMESTAMP WITH TIME ZONE")
                .nullable(false)
                .defaultValue(ColumnDefault.literal(OffsetDateTime.parse("2024-01-01T09:30:00+05:30")))
                .build();
        ColumnDefinition desired = ColumnDefinition.builder("created_at", "TIMESTAMP WITH TIME ZONE")
                .nullable(true)
                .defaultValue(ColumnDefault.literal(OffsetDateTime.parse("2024-01-01T12:00:00+08:00")))
                .build();
        SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.CHANGE_COLUMN,
                RelationIdentity.table("events"), "created_at", actual, desired,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);

        List<SqlRequest> requests = RelationalSchemaSqlRenderer.create(RdbDialect.postgresql().schema())
                .render(operation);

        assertEquals(1, requests.size());
        assertTrue(requests.getFirst().sql().endsWith("drop not null"));
    }

    @Test
    void offsetPreservingDialectsStillDetectDifferentOffsets() {
        for (RdbDialect dialect : List.of(RdbDialect.h2(), RdbDialect.sqlServer())) {
            assertFalse(compare(dialect, "2024-01-01T09:30:00+05:30",
                    "2024-01-01T12:00:00+08:00").compatible(), dialect.name());
        }
    }

    @Test
    void postgresqlCheckLiteralsCompareInstantsAcrossAllPredicateShapes() {
        List<CheckPredicate> actual = predicates("2024-01-01T09:30:00+05:30");
        List<CheckPredicate> sameInstant = predicates("2024-01-01T12:00:00+08:00");
        List<CheckPredicate> differentInstant = predicates("2024-01-01T13:00:00+08:00");
        for (int index = 0; index < actual.size(); index++) {
            CheckConstraintDefinition observed = CheckConstraintDefinition.of("ck_time", actual.get(index));
            CheckConstraintDefinition equivalent = CheckConstraintDefinition.of("ck_time", sameInstant.get(index));
            CheckConstraintDefinition different = CheckConstraintDefinition.of("ck_time", differentInstant.get(index));
            assertTrue(SchemaDefinitionEquality.sameCheck(observed, equivalent, RdbDialect.postgresql().schema()));
            assertFalse(SchemaDefinitionEquality.sameCheck(observed, different, RdbDialect.postgresql().schema()));
            assertFalse(SchemaDefinitionEquality.sameCheck(observed, equivalent, RdbDialect.sqlServer().schema()));
        }
    }

    private static List<CheckPredicate> predicates(String timestamp) {
        OffsetDateTime value = OffsetDateTime.parse(timestamp);
        return List.of(
                CheckPredicate.compare("created_at", CheckPredicate.ComparisonOperator.GREATER_THAN, value),
                CheckPredicate.range("created_at", value, value.plusDays(1)),
                CheckPredicate.in("created_at", List.of(value, value.plusDays(1))));
    }

    private static SchemaCompatibilityReport compare(RdbDialect dialect, String actual, String desired) {
        RelationIdentity identity = RelationIdentity.table("events");
        RelationalTableDefinition observed = table(identity, actual);
        RelationalTableDefinition target = table(identity, desired);
        return SchemaDiffer.diff(target, SchemaSnapshot.present(observed), dialect.capabilities(),
                SchemaCompatibilityMode.EXACT, dialect.name(), dialect.schema());
    }

    private static RelationalTableDefinition table(RelationIdentity identity, String value) {
        return RelationalTableDefinition.builder(identity)
                .addColumn(ColumnDefinition.builder("created_at", "TIMESTAMP WITH TIME ZONE")
                        .defaultValue(ColumnDefault.literal(OffsetDateTime.parse(value))).build())
                .build();
    }
}
