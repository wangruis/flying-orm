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
    void physicalSequenceMetadataIgnoresUnrecoverableOptionsButRetainsExactName() {
        ColumnMetadata current = ColumnMetadata.of("event_id", "BIGINT")
                                                .withGeneration(ValueGeneration.sequence("CaseSeq"));
        DynamicField sameSequence = DynamicField.of("event_id", "BIGINT")
                                                  .withGeneration(ValueGeneration.sequence("CaseSeq", 10, 2, 50));
        DynamicField differentCase = DynamicField.of("event_id", "BIGINT")
                                                  .withGeneration(ValueGeneration.sequence("caseseq", 10, 2, 50));

        assertTrue(SchemaMigrationSupport.sameColumnShape(current, sameSequence, tables));
        assertFalse(SchemaMigrationSupport.sameColumnShape(current, differentCase, tables));
    }
}
