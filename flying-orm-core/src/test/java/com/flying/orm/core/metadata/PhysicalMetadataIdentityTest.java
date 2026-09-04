package com.flying.orm.core.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 物理元数据保留精确名称，同时只在折叠名称唯一时提供兼容查找。 */
class PhysicalMetadataIdentityTest {

    @Test
    void keepsCaseDistinctColumnsAndPrefersExactLookup() {
        ColumnMetadata lower = ColumnMetadata.of("customerId", "BIGINT");
        ColumnMetadata upper = ColumnMetadata.of("CustomerId", "BIGINT");
        TableMetadata metadata = TableMetadata.builder("customers")
                                              .addColumn(lower)
                                              .addColumn(upper)
                                              .build();

        assertSame(lower, metadata.findColumn("customerId").orElseThrow());
        assertSame(upper, metadata.findColumn("CustomerId").orElseThrow());
        assertTrue(metadata.findColumn("CUSTOMERID").isEmpty());
    }

    @Test
    void keepsCaseDistinctIndexesAndForeignKeysWithAmbiguousFoldedLookup() {
        IndexMetadata lowerIndex = IndexMetadata.builder("idx_customer").addColumn("customerId").build();
        IndexMetadata upperIndex = IndexMetadata.builder("IDX_CUSTOMER").addColumn("CustomerId").build();
        ForeignKeyMetadata lowerKey = new ForeignKeyMetadata(
                "fk_customer", List.of("customerId"), "accounts", List.of("id"));
        ForeignKeyMetadata upperKey = new ForeignKeyMetadata(
                "FK_CUSTOMER", List.of("CustomerId"), "accounts", List.of("ID"));

        TableMetadata metadata = TableMetadata.builder("customers")
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
        assertEquals("case-only physical field rename must use reviewed schema migration", error.getMessage());
    }
}
