package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaGeneratedValueComparisonTest {

    private final SchemaTableSqlRenderer tables = new SchemaTableSqlRenderer(RdbDialect.h2().schema());

    @Test
    void h2PhysicalSequenceMetadataRetainsRecoveredOptionsAndExactName() {
        ColumnMetadata current = ColumnMetadata.of("event_id", "BIGINT")
                                                .withGeneration(ValueGeneration.sequence("CaseSeq", 10, 2, 50));
        DynamicField sameSequence = DynamicField.of("event_id", "BIGINT")
                                                  .withGeneration(ValueGeneration.sequence("CaseSeq", 10, 2, 50));
        DynamicField differentCase = DynamicField.of("event_id", "BIGINT")
                                                  .withGeneration(ValueGeneration.sequence("caseseq", 10, 2, 50));

        assertTrue(SchemaMigrationSupport.sameColumnShape(current, sameSequence, tables));
        assertFalse(SchemaMigrationSupport.sameColumnShape(current, differentCase, tables));
    }

    @Test
    void h2DistinguishesEachRecoveredSequenceOptionButAllowsUnspecifiedTargetCache() {
        ColumnMetadata current = ColumnMetadata.of("event_id", "BIGINT")
                .withGeneration(ValueGeneration.sequence("CaseSeq", 10, 2, 50));
        for (ValueGeneration changed : java.util.List.of(
                ValueGeneration.sequence("CaseSeq", 11, 2, 50),
                ValueGeneration.sequence("CaseSeq", 10, 3, 50),
                ValueGeneration.sequence("CaseSeq", 10, 2, 51))) {
            assertFalse(SchemaMigrationSupport.sameColumnShape(current,
                    DynamicField.of("event_id", "BIGINT").withGeneration(changed), tables));
        }
        assertTrue(SchemaMigrationSupport.sameColumnShape(current,
                DynamicField.of("event_id", "BIGINT")
                        .withGeneration(ValueGeneration.sequence("CaseSeq", 10, 2, 0)), tables));
    }

    @Test
    void customDialectWithoutRecoverableOptionsStillComparesStrategyAndExactSequenceName() {
        ColumnMetadata current = ColumnMetadata.of("event_id", "BIGINT")
                .withGeneration(ValueGeneration.sequence("CaseSeq"));
        DynamicField sameSequence = DynamicField.of("event_id", "BIGINT")
                .withGeneration(ValueGeneration.sequence("CaseSeq", 10, 2, 50));
        DynamicField differentCase = DynamicField.of("event_id", "BIGINT")
                .withGeneration(ValueGeneration.sequence("caseseq", 10, 2, 50));
        assertTrue(SchemaGeneratedValueComparison.same(current, sameSequence,
                SchemaDialect.GeneratedValueStyle.NONE));
        assertFalse(SchemaGeneratedValueComparison.same(current, differentCase,
                SchemaDialect.GeneratedValueStyle.NONE));
        assertFalse(SchemaGeneratedValueComparison.same(current,
                DynamicField.of("event_id", "BIGINT").withGeneration(ValueGeneration.identity()),
                SchemaDialect.GeneratedValueStyle.NONE));
    }
}
