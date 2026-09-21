package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaLegacyPhysicalColumnRegressionTest {

    @Test
    void forwardAndRollbackKeepTheSamePhysicalDefaultCommentAndCollation() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.mysql());
        TableMetadata current = current();
        DynamicForm target = target();
        SchemaSnapshot snapshot = SchemaSnapshot.builder(RelationIdentity.of(null, "app", "items"))
                .tablePresent()
                .physicalColumns(List.of(ColumnDefinition.builder("code", "VARCHAR").length(16).nullable(false)
                        .defaultValue(ColumnDefault.literal("pending")).comment("keep")
                        .charset("utf8mb4").collation("utf8mb4_bin").build()))
                .unknown(SchemaSnapshot.UnknownAttribute.PRIMARY_KEY_NAME,
                         SchemaSnapshot.UnknownAttribute.INDEX_DIRECTION).build();
        assertTrue(snapshot.completeTable().isEmpty());
        SchemaMigrationPlan migration = renderer.migrationPlanner().migrateSafelyPlan(
                current, target, List.of(), List.of(), SchemaMigrationOptions.safe(), snapshot);
        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer).review(
                current, migration, SchemaMigrationReviewPolicy.allowBlocking(), snapshot);
        assertEquals(1, migration.requests().size());
        assertEquals(1, reviewed.rollback().requests().size());
        String forward = migration.requests().getFirst().sql().toLowerCase(Locale.ROOT);
        String rollback = reviewed.rollback().requests().getFirst().sql().toLowerCase(Locale.ROOT);
        assertTrue(forward.contains("varchar(32)"), forward);
        assertTrue(rollback.contains("varchar(16)"), rollback);
        for (String sql : List.of(forward, rollback)) {
            assertTrue(sql.contains("default 'pending'"), sql);
            assertTrue(sql.contains("comment 'keep'"), sql);
            assertTrue(sql.contains("utf8mb4_bin"), sql);
            assertTrue(sql.contains("character set"), sql);
        }
    }

    @Test
    void incompleteLegacyMetadataCannotAuthorizeAFullColumnRewrite() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.mysql());
        assertThrows(IllegalStateException.class,
                () -> renderer.migrateSafelyPlan(current(), target(), List.of()));
    }

    @Test
    void completeReplacementRetainsIdentityAndCommentWithoutReaddingPrimaryKey() {
        SchemaTableSqlRenderer renderer = new SchemaTableSqlRenderer(RdbDialect.mysql().schema());
        DynamicField target = DynamicField.primaryKey("id", "BIGINT")
                .withGeneration(ValueGeneration.identity()).withComment("keep");
        ColumnDefinition physical = ColumnDefinition.builder("id", "INT").nullable(false)
                .generation(ValueGeneration.identity()).comment("keep").build();
        String sql = renderer.replacementColumnDefinition(target, physical).toLowerCase(Locale.ROOT);
        assertTrue(sql.contains("auto_increment"), sql);
        assertTrue(sql.contains("comment 'keep'"), sql);
        assertEquals(-1, sql.indexOf("primary key"));
    }

    private static TableMetadata current() {
        return TableMetadata.builder("app.items")
                .addColumn(ColumnMetadata.of("code", "VARCHAR")
                        .withLength(16).withNullable(false).withComment("keep")).build();
    }

    private static DynamicForm target() {
        return DynamicForm.builder("items", "app.items")
                .addField(DynamicField.of("code", "VARCHAR")
                        .withLength(32).withNullable(false).withComment("keep")).build();
    }
}
