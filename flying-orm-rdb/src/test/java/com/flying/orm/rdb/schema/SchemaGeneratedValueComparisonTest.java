package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaGeneratedValueComparisonTest {

    private final SchemaDialect.GeneratedValueStyle style = RdbDialect.h2().schema().generatedValueStyle();

    @Test
    void h2PhysicalSequenceMetadataRetainsRecoveredOptionsAndExactName() {
        DynamicField current = DynamicField.of("event_id", "BIGINT")
                                                .withGeneration(ValueGeneration.sequence("CaseSeq", 10, 2, 50));
        DynamicField sameSequence = DynamicField.of("event_id", "BIGINT")
                                                  .withGeneration(ValueGeneration.sequence("CaseSeq", 10, 2, 50));
        DynamicField differentCase = DynamicField.of("event_id", "BIGINT")
                                                  .withGeneration(ValueGeneration.sequence("caseseq", 10, 2, 50));

        assertTrue(SchemaGeneratedValueComparison.same(current.generation(), sameSequence.generation(), style));
        assertFalse(SchemaGeneratedValueComparison.same(current.generation(), differentCase.generation(), style));
    }

    @Test
    void h2DistinguishesEachRecoveredSequenceOptionButAllowsUnspecifiedTargetCache() {
        DynamicField current = DynamicField.of("event_id", "BIGINT")
                .withGeneration(ValueGeneration.sequence("CaseSeq", 10, 2, 50));
        for (ValueGeneration changed : java.util.List.of(
                ValueGeneration.sequence("CaseSeq", 11, 2, 50),
                ValueGeneration.sequence("CaseSeq", 10, 3, 50),
                ValueGeneration.sequence("CaseSeq", 10, 2, 51))) {
            assertFalse(SchemaGeneratedValueComparison.same(current.generation(), changed, style));
        }
        assertTrue(SchemaGeneratedValueComparison.same(current.generation(),
                ValueGeneration.sequence("CaseSeq", 10, 2, 0), style));
    }

    @Test
    void customDialectWithoutRecoverableOptionsStillComparesStrategyAndExactSequenceName() {
        DynamicField current = DynamicField.of("event_id", "BIGINT")
                .withGeneration(ValueGeneration.sequence("CaseSeq"));
        DynamicField sameSequence = DynamicField.of("event_id", "BIGINT")
                .withGeneration(ValueGeneration.sequence("CaseSeq", 10, 2, 50));
        DynamicField differentCase = DynamicField.of("event_id", "BIGINT")
                .withGeneration(ValueGeneration.sequence("caseseq", 10, 2, 50));
        assertTrue(SchemaGeneratedValueComparison.same(current.generation(), sameSequence.generation(),
                SchemaDialect.GeneratedValueStyle.NONE));
        assertFalse(SchemaGeneratedValueComparison.same(current.generation(), differentCase.generation(),
                SchemaDialect.GeneratedValueStyle.NONE));
        assertFalse(SchemaGeneratedValueComparison.same(current.generation(),
                ValueGeneration.identity(),
                SchemaDialect.GeneratedValueStyle.NONE));
    }
}
