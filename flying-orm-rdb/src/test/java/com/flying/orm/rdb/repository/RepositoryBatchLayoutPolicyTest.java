package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 批量实体写入只能把列布局在 SQL 执行前固定下来。 */
class RepositoryBatchLayoutPolicyTest {

    @Test
    void rejectsConditionalInsertColumnsBeforeAnyRowIsConsumed() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> RepositoryBatchLayoutPolicy.requireStableInsertLayout(
                        EntityMetadataResolver.createUncached(ConditionalInsertEntity.class)));

        assertTrue(error.getMessage().contains("optional_name"));
        assertTrue(error.getMessage().contains("stable column layout"));
    }

    @Test
    void rejectsConditionalUpsertColumnsWhenNeitherStageAlwaysIncludesTheField() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> RepositoryBatchLayoutPolicy.requireStableUpsertLayout(
                        EntityMetadataResolver.createUncached(ConditionalUpsertEntity.class)));

        assertTrue(error.getMessage().contains("optional_name"));
    }

    @Test
    void acceptsLayoutsThatAreIndependentOfEntityValues() {
        assertDoesNotThrow(() -> RepositoryBatchLayoutPolicy.requireStableInsertLayout(
                EntityMetadataResolver.createUncached(StableEntity.class)));
        assertDoesNotThrow(() -> RepositoryBatchLayoutPolicy.requireStableUpsertLayout(
                EntityMetadataResolver.createUncached(StableEntity.class)));
    }

    @TableName("batch_layout_insert")
    static final class ConditionalInsertEntity {
        String id;

        @TableField(value = "optional_name", insertStrategy = FieldStrategy.NOT_NULL)
        String optionalName;
    }

    @TableName("batch_layout_upsert")
    static final class ConditionalUpsertEntity {
        String id;

        @TableField(value = "optional_name", insertStrategy = FieldStrategy.NEVER,
                updateStrategy = FieldStrategy.NOT_EMPTY)
        String optionalName;
    }

    @TableName("batch_layout_stable")
    static final class StableEntity {
        String id;

        @TableField(value = "included_name", insertStrategy = FieldStrategy.ALWAYS,
                updateStrategy = FieldStrategy.DEFAULT)
        String includedName;

        @TableField(value = "ignored_name", insertStrategy = FieldStrategy.NEVER,
                updateStrategy = FieldStrategy.NEVER)
        String ignoredName;
    }
}
