package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaColumnShapeChangeTest {

    @Test
    void appliesAProvenSafeWideningWithoutMixingItWithSkippedChanges() {
        List<SqlRequest> requests = new ArrayList<>();
        List<SkippedSchemaChange> skipped = new ArrayList<>();
        SchemaDialect dialect = SchemaDialect.standard();
        SchemaColumnShapeChange change = new SchemaColumnShapeChange(
                requests,
                skipped,
                new SchemaColumnShapeChange.Input(
                        "device",
                        ColumnMetadata.of("name", "VARCHAR").withLength(32),
                        DynamicField.of("name", "VARCHAR").withLength(64),
                        SchemaMigrationOptions.safe(),
                        false),
                dialect,
                new SchemaTableSqlRenderer(dialect));

        assertTrue(change.apply());
        assertEquals(1, requests.size());
        assertTrue(skipped.isEmpty());
    }
}
