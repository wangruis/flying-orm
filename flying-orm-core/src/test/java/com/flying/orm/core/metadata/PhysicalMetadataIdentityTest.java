package com.flying.orm.core.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 物理元数据保留精确名称，同时只在折叠名称唯一时提供兼容查找。 */
class PhysicalMetadataIdentityTest {

    @Test
    void nameIndexLookupsDoNotCarryUnusedContext() {
        assertTrue(Arrays.stream(MetadataNameIndex.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("find") || method.getName().equals("findExact"))
                .allMatch(method -> method.getParameterCount() == 1));
    }

    @Test
    void preservesExactAndUnambiguousNamesAcrossSchemaAndDatabaseLookups() {
        RelationalTableDefinition lower = RelationalTableDefinition.builder("orders").build();
        RelationalTableDefinition upper = RelationalTableDefinition.builder("Orders").build();
        SchemaMetadata schema = SchemaMetadata.builder("sales").addTable(lower).addTable(upper).build();
        SchemaMetadata other = SchemaMetadata.builder("Sales").build();
        DatabaseMetadata database = DatabaseMetadata.builder("main").addSchema(schema).addSchema(other).build();
        RelationalSchemaDefinition relational = RelationalSchemaDefinition.of(List.of(lower, upper));

        assertSame(schema, database.findSchema(" sales ").orElseThrow());
        assertSame(other, database.findSchema("Sales").orElseThrow());
        assertTrue(database.findSchema("SALES").isEmpty());
        assertSame(lower, schema.findTable(" orders ").orElseThrow());
        assertSame(upper, relational.findTable("Orders").orElseThrow());
        assertTrue(relational.findTable("ORDERS").isEmpty());
        assertSame(lower, SchemaMetadata.builder("single").addTable(lower).build()
                .findTable(" ORDERS ").orElseThrow());
        assertThrows(IllegalArgumentException.class, () -> schema.findTable(" "));
    }

    @Test
    void keepsCaseDistinctColumnsAndPrefersExactLookup() {
        ColumnDefinition lower = ColumnDefinition.builder("customerId", "BIGINT").build();
        ColumnDefinition upper = ColumnDefinition.builder("CustomerId", "BIGINT").build();
        RelationalTableDefinition metadata = RelationalTableDefinition.builder("customers")
                                              .addColumn(lower)
                                              .addColumn(upper)
                                              .build();

        assertSame(lower, metadata.findColumn("customerId").orElseThrow());
        assertSame(upper, metadata.findColumn("CustomerId").orElseThrow());
        assertTrue(metadata.findColumn("CUSTOMERID").isEmpty());
    }

    @Test
    void keepsCaseDistinctIndexesAndForeignKeysWithAmbiguousFoldedLookup() {
        IndexDefinition lowerIndex = IndexDefinition.builder("idx_customer").addColumn("customerId").build();
        IndexDefinition upperIndex = IndexDefinition.builder("IDX_CUSTOMER").addColumn("CustomerId").build();
        ForeignKeyDefinition lowerKey = ForeignKeyDefinition.builder("fk_customer").addColumn("customerId")
                .reference(RelationIdentity.table("accounts")).addReferenceColumn("id").build();
        ForeignKeyDefinition upperKey = ForeignKeyDefinition.builder("FK_CUSTOMER").addColumn("CustomerId")
                .reference(RelationIdentity.table("accounts")).addReferenceColumn("ID").build();

        RelationalTableDefinition metadata = RelationalTableDefinition.builder("customers")
                                              .addColumn(ColumnDefinition.builder("customerId", "BIGINT").build())
                                              .addColumn(ColumnDefinition.builder("CustomerId", "BIGINT").build())
                                              .addIndex(lowerIndex)
                                              .addIndex(upperIndex)
                                              .addForeignKey(lowerKey)
                                              .addForeignKey(upperKey)
                                              .build();

        assertSame(lowerIndex, metadata.findIndex("idx_customer").orElseThrow());
        assertSame(upperIndex, metadata.findIndex("IDX_CUSTOMER").orElseThrow());
        assertTrue(metadata.findIndex("idx_CUSTOMER").isEmpty());
        assertSame(lowerKey, metadata.findForeignKey("fk_customer").orElseThrow());
        assertSame(upperKey, metadata.findForeignKey("FK_CUSTOMER").orElseThrow());
        assertTrue(metadata.findForeignKey("fk_CUSTOMER").isEmpty());
    }

    @Test
    void rejectsCaseOnlyPhysicalFieldChangeFromDirectDiff() {
        DynamicForm source = DynamicForm.builder("source", "customers")
                                        .addField(DynamicField.of("customerId", "BIGINT"))
                                        .build();
        DynamicForm target = DynamicForm.builder("target", "customers")
                                        .addField(DynamicField.of("CustomerId", "BIGINT"))
                                        .build();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> source.diffTo(target));
        assertEquals("case-only physical field rename is not an in-place field change", error.getMessage());
    }
}
