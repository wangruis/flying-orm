package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class EntityTableCommentCompilationTest {

    @Test
    void compilesTheEntityTableCommentIntoCanonicalRelationalMetadata() {
        EntitySchemaDescriptor<CommentedOrder> commented = EntitySchemaDescriptor.builder(CommentedOrder.class).build();
        EntitySchemaDescriptor<PlainOrder> plain = EntitySchemaDescriptor.builder(PlainOrder.class).build();

        assertEquals("订单主表", commented.table().comment());
        assertNotEquals(plain.relationalFingerprint(), commented.relationalFingerprint());
    }

    @TableName(value = "orders", comment = " 订单主表 ")
    private static final class CommentedOrder {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;
    }

    @TableName("orders")
    private static final class PlainOrder {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;
    }
}
