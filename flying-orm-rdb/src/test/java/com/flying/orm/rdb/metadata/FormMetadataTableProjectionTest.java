package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormMetadataTableProjectionTest {

    @Test
    void preservesOrderedColumnFactsAndActualIndexesAndForeignKeysInBothProjections() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT").withGeneration(ValueGeneration.identity()))
                .addField(DynamicField.of("code", "VARCHAR").withLength(42).withNullable(false)
                        .withComment("keep"))
                .addField(DynamicField.of("amount", "DECIMAL").withPrecision(12, 3))
                .addField(DynamicField.of("created", "TIMESTAMP").withPrecision(0, null))
                .addField(DynamicField.of("sequence_value", "BIGINT")
                        .withGeneration(ValueGeneration.sequence("items_seq")))
                .build();
        var indexes = List.<Map<String, Object>>of(Map.of(
                "INDEX_NAME", "ix_code", "COLUMN_NAME", "code", "UNIQUE_INDEX", false));
        var foreignKeys = List.<Map<String, Object>>of(Map.of(
                "FOREIGN_KEY_NAME", "fk_code", "COLUMN_NAME", "code",
                "REFERENCED_TABLE_NAME", "codes", "REFERENCED_COLUMN_NAME", "code"));
        var ordinary = FormMetadataRowConverter.toTableMetadata("items", form, indexes, foreignKeys);
        var mysql = FormMetadataRowConverter.toTableMetadata("items", form, indexes, foreignKeys,
                List.of(Map.of("CONSTRAINT_NAME", "uq_code", "COLUMN_NAME", "code")),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL);

        for (var table : List.of(ordinary, mysql)) {
            assertEquals(form.toTableMetadata().columns(), table.columns());
            assertEquals(List.of("id", "code", "amount", "created", "sequence_value"),
                    table.columns().stream().map(column -> column.name()).toList());
            assertEquals(List.of("code"), table.indexes().getFirst().columns());
            assertEquals("ix_code", table.indexes().getFirst().name());
            assertEquals("fk_code", table.foreignKeys().getFirst().name());
            assertEquals("codes", table.foreignKeys().getFirst().referenceTable());
        }
        assertEquals(1, ordinary.indexes().size());
        assertEquals(2, mysql.indexes().size());
        assertEquals("uq_code", mysql.indexes().getLast().name());
        assertTrue(mysql.indexes().getLast().unique());
    }

    @Test
    void emptyDictionaryRemainsMissingTableForFormButAbsentForSnapshot() {
        var failure = assertThrows(IllegalArgumentException.class,
                () -> FormMetadataRowConverter.toDynamicForm("items", "items", List.of(), value -> value));
        assertEquals("table metadata not found", failure.getMessage());
        assertEquals(SchemaSnapshot.State.ABSENT,
                FormMetadataRowConverter.toSchemaSnapshot(RelationIdentity.table("items"), "items",
                        List.of(), List.of(), false, List.of(), false, value -> value).tableState());
    }

    @Test
    void dictionaryStillPassesThroughFormDuplicateFieldValidation() {
        assertThrows(IllegalArgumentException.class, () -> FormMetadataRowConverter.toDynamicForm(
                "items", "items", List.of(
                        Map.of("COLUMN_NAME", "Code", "DATA_TYPE", "VARCHAR"),
                        Map.of("COLUMN_NAME", "code", "DATA_TYPE", "VARCHAR")), value -> value));
    }
}
