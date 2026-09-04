package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.dialect.OracleVersion;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityGeneratedPrimaryKeyNameTest {

    @Test
    void generatedPrimaryKeyNameFitsEverySupportedDialect() {
        String first = EntitySchemaDescriptor.builder(LongTableNameEntity.class)
                .build()
                .table()
                .primaryKey()
                .orElseThrow()
                .name();
        String second = EntitySchemaDescriptor.builder(LongTableNameEntity.class)
                .build()
                .table()
                .primaryKey()
                .orElseThrow()
                .name();

        assertEquals(first, second);
        for (RdbDialect dialect : List.of(
                RdbDialect.postgresql(),
                RdbDialect.mysql(),
                RdbDialect.oracle(OracleVersion.V12C),
                RdbDialect.oracle(),
                RdbDialect.sqlServer())) {
            assertTrue(first.codePointCount(0, first.length()) <= dialect.maxIdentifierLength(),
                       () -> dialect.name() + " primary key name exceeds its identifier limit: " + first);
        }
    }

    @Test
    void generatedPrimaryKeyNameKeepsLiteralDotsInsideOneUnambiguousIdentifier() {
        String dotted = EntitySchemaDescriptor.builder(DottedTableNameEntity.class)
                .build()
                .table()
                .primaryKey()
                .orElseThrow()
                .name();
        String underscored = EntitySchemaDescriptor.builder(UnderscoredTableNameEntity.class)
                .build()
                .table()
                .primaryKey()
                .orElseThrow()
                .name();

        assertFalse(dotted.contains("."));
        assertNotEquals(underscored, dotted);
        assertTrue(dotted.matches("[A-Za-z_][A-Za-z0-9_]*"));
    }

    @TableName("orders_with_a_deliberately_long_but_postgresql_valid_physical_name")
    private static final class LongTableNameEntity {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;
    }

    @TableName(value = "order.items", schema = "sales.data")
    private static final class DottedTableNameEntity {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;
    }

    @TableName(value = "order_items", schema = "sales.data")
    private static final class UnderscoredTableNameEntity {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;
    }
}
