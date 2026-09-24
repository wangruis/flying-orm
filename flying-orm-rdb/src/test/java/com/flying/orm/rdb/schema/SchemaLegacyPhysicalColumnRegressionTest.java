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
    void sqlServerLogicalTypesStoredAsTextRetainCollationOnNullableChanges() {
        for (String type : List.of("VARCHAR", "JSON", "OFFSET_TIME")) {
            FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());
            ColumnMetadata before = ColumnMetadata.of("code", type).withNullable(false);
            DynamicField after = DynamicField.of("code", type).withNullable(true);
            TableMetadata current = TableMetadata.builder("items").addColumn(before).build();
            DynamicForm target = DynamicForm.builder("items", "items").addField(after).build();
            SchemaSnapshot snapshot = sqlServerSnapshot(type);
            SchemaMigrationPlan migration = renderer.migrationPlanner().migrateSafelyPlan(
                    current, target, List.of(), List.of(), SchemaMigrationOptions.safe(), snapshot);
            ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer).review(
                    current, migration, SchemaMigrationReviewPolicy.allowBlocking(), snapshot);
            assertTrue(migration.requests().getFirst().sql().contains("collate Latin1_General_100_BIN2"), type);
            assertTrue(reviewed.rollback().requests().getFirst().sql().contains("collate Latin1_General_100_BIN2"), type);
        }
    }

    @Test
    void sqlServerReadsSourceCharacterFactsForRollbackToText() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());
        TableMetadata current = TableMetadata.builder("items")
                .addColumn(ColumnMetadata.of("code", "VARCHAR").withNullable(false)).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("code", "INTEGER").withNullable(false)).build();
        java.util.concurrent.atomic.AtomicInteger reads = new java.util.concurrent.atomic.AtomicInteger();
        com.flying.orm.rdb.metadata.ReactiveFormMetadataReader reader = new com.flying.orm.rdb.metadata.ReactiveFormMetadataReader() {
            public reactor.core.publisher.Mono<DynamicForm> readForm(String id, String table) {
                throw new AssertionError("unexpected form read");
            }
            public reactor.core.publisher.Mono<DynamicForm> readForm(String id, String schema, String table) {
                throw new AssertionError("unexpected form read");
            }
            public reactor.core.publisher.Mono<SchemaSnapshot> readSnapshot(String table) {
                reads.incrementAndGet();
                return reactor.core.publisher.Mono.just(sqlServerSnapshot("VARCHAR"));
            }
        };
        SchemaSnapshot snapshot = renderer.migrationPlanner().physicalSnapshot(
                current, target, SchemaMigrationOptions.safe().allowColumnChange(), reader).block();
        assertEquals(1, reads.get());
        SchemaMigrationPlan migration = renderer.migrationPlanner().migrateSafelyPlan(
                current, target, List.of(), List.of(), SchemaMigrationOptions.safe().allowColumnChange(), snapshot);
        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer).review(
                current, migration, SchemaMigrationReviewPolicy.allowBlocking(), snapshot);
        org.junit.jupiter.api.Assertions.assertFalse(migration.requests().getFirst().sql().contains("collate"));
        assertTrue(reviewed.rollback().requests().getFirst().sql().contains("collate Latin1_General_100_BIN2"));
    }

    @Test
    void directCharacterChangesAcceptExplicitPhysicalFacts() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());
        DynamicForm source = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("code", "VARCHAR").withLength(16).withNullable(false)).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("code", "VARCHAR").withLength(32).withNullable(false)).build();
        assertThrows(IllegalStateException.class, () -> renderer.migrate(source.diffTo(target)));
        assertTrue(renderer.migrate(source.diffTo(target), sqlServerSnapshot("VARCHAR"))
                .getFirst().sql().contains("collate Latin1_General_100_BIN2"));
    }

    private static SchemaSnapshot sqlServerSnapshot(String type) {
        return SchemaSnapshot.builder(RelationIdentity.table("items")).tablePresent()
                .physicalColumns(List.of(ColumnDefinition.builder("code", type).nullable(false)
                        .collation("Latin1_General_100_BIN2").build())).build();
    }

    @Test
    void sqlServerForwardAndRollbackPreservePhysicalCharacterCollation() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());
        TableMetadata current = current();
        SchemaSnapshot snapshot = SchemaSnapshot.builder(RelationIdentity.of(null, "app", "items"))
                .tablePresent()
                .physicalColumns(List.of(ColumnDefinition.builder("code", "VARCHAR").length(16).nullable(false)
                        .collation("Latin1_General_100_BIN2").comment("keep").build())).build();
        SchemaMigrationPlan migration = renderer.migrationPlanner().migrateSafelyPlan(
                current, target(), List.of(), List.of(), SchemaMigrationOptions.safe(), snapshot);
        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer).review(
                current, migration, SchemaMigrationReviewPolicy.allowBlocking(), snapshot);
        assertEquals(1, migration.requests().size());
        assertEquals(1, reviewed.rollback().requests().size());
        for (String sql : List.of(migration.requests().getFirst().sql(),
                reviewed.rollback().requests().getFirst().sql())) {
            assertTrue(sql.toLowerCase(Locale.ROOT).contains("collate Latin1_General_100_BIN2".toLowerCase(Locale.ROOT)), sql);
        }
    }

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
