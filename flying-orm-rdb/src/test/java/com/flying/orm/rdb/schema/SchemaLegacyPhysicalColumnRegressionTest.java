package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import reactor.core.publisher.Mono;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
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
    void combinedTypeAndNullabilityChangesPreservePhysicalFacts() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            for (boolean nullable : List.of(true, false)) {
                FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);
                RelationalTableDefinition current = RelationalTableDefinition.builder("items").addColumn(
                        DynamicField.of("code", "VARCHAR").withLength(16).withNullable(!nullable).column()).build();
                DynamicForm target = DynamicForm.builder("items", "items").addField(
                        DynamicField.of("code", "VARCHAR").withLength(32).withNullable(nullable)).build();
                boolean oracle = dialect.name().equals("oracle");
                boolean mysql = dialect.name().equals("mysql");
                boolean sqlServer = dialect.name().equals("sqlserver");
                ColumnDefinition physical = ColumnDefinition.builder("code", oracle ? "VARCHAR2(16 CHAR)" : "VARCHAR")
                        .length(oracle ? null : 16).nullable(!nullable).defaultValue(ColumnDefault.literal("pending"))
                        .charset(mysql ? "utf8mb4" : null)
                        .collation(mysql ? "utf8mb4_bin" : sqlServer ? "Latin1_General_100_BIN2" : null).build();
                SchemaSnapshot snapshot = SchemaSnapshot.builder(RelationIdentity.table("items"))
                        .tablePresent().physicalColumns(List.of(physical)).build();
                SchemaMigrationOptions options = SchemaMigrationOptions.safe().allowColumnChange();
                java.util.concurrent.atomic.AtomicInteger reads = new java.util.concurrent.atomic.AtomicInteger();
                com.flying.orm.rdb.metadata.ReactiveFormMetadataReader reader = new com.flying.orm.rdb.metadata.ReactiveFormMetadataReader() {
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                throw new AssertionError("unexpected form read");
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return Mono.just(current);
            }

            public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
                reads.incrementAndGet();
                return reactor.core.publisher.Mono.just(snapshot);
            }
                };
                SchemaMigrationPlan migration = renderer.migrationPlanner()
                        .plan(target, List.of(), List.of(), reader, options).block();
                assertEquals(dialect.name().equals("postgresql") ? 0 : 1, reads.get(), dialect.name());
                assertTrue(migration.skippedChanges().isEmpty(), dialect.name() + migration.skippedChanges());
                int statements = mysql || sqlServer ? 1 : 2;
                assertEquals(statements, migration.requests().size(), dialect.name());
                String forward = migration.requests().toString().toLowerCase(Locale.ROOT);
                assertTrue(forward.contains(oracle ? "varchar2(32 char)" : "varchar(32)"), forward);
                String last = migration.requests().getLast().sql().toLowerCase(Locale.ROOT);
                if (mysql && nullable) {
                    org.junit.jupiter.api.Assertions.assertFalse(last.contains("not null"), last);
                } else {
                    assertTrue(last.contains(nullable
                            ? oracle ? " null)" : sqlServer ? " null" : "drop not null" : "not null"), last);
                }
                if (mysql) {
                    for (String sql : List.of(forward)) {
                        assertTrue(sql.contains("default 'pending'"), sql);
                        assertTrue(sql.contains("character set `utf8mb4`"), sql);
                        assertTrue(sql.contains("collate `utf8mb4_bin`"), sql);
                    }
                } else if (sqlServer) {
                    assertTrue(forward.contains("collate latin1_general_100_bin2"), forward);
                }
            }
        }
    }

    @Test
    void sqlServerLogicalTypesStoredAsTextRetainCollationOnNullableChanges() {
        for (String type : List.of("VARCHAR", "JSON", "OFFSET_TIME")) {
            FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());
            DynamicField before = DynamicField.of("code", type).withNullable(false);
            DynamicField after = DynamicField.of("code", type).withNullable(true);
            RelationalTableDefinition current = RelationalTableDefinition.builder("items").addColumn(before.column()).build();
            DynamicForm target = DynamicForm.builder("items", "items").addField(after).build();
            SchemaSnapshot snapshot = sqlServerSnapshot(type);
            SchemaMigrationPlan migration = renderer.migrationPlanner().migrateSafelyPlan(
                    current, target, List.of(), List.of(), SchemaMigrationOptions.safe(), snapshot);
            SchemaMigrationPlan reviewed = renderer.renderPlan(current, snapshot, migration, SchemaMigrationOptions.safe());
            assertTrue(migration.requests().getFirst().sql().contains("collate Latin1_General_100_BIN2"), type);
        }
    }

    @Test
    void sqlServerTypeConversionDoesNotApplyTextCollationToInteger() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());
        RelationalTableDefinition current = RelationalTableDefinition.builder("items")
                .addColumn(DynamicField.of("code", "VARCHAR").withNullable(false).column()).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("code", "INTEGER").withNullable(false)).build();
        java.util.concurrent.atomic.AtomicInteger reads = new java.util.concurrent.atomic.AtomicInteger();
        com.flying.orm.rdb.metadata.ReactiveFormMetadataReader reader = new com.flying.orm.rdb.metadata.ReactiveFormMetadataReader() {
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                throw new AssertionError("unexpected form read");
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return Mono.just(current);
            }

            public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
                reads.incrementAndGet();
                return reactor.core.publisher.Mono.just(sqlServerSnapshot("VARCHAR"));
            }
        };
        SchemaMigrationPlan migration = renderer.migrationPlanner().plan(
                target, List.of(), List.of(), reader, SchemaMigrationOptions.safe().allowColumnChange()).block();
        assertEquals(1, reads.get());
        org.junit.jupiter.api.Assertions.assertFalse(migration.requests().getFirst().sql().contains("collate"));
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
    void sqlServerPreservesPhysicalCharacterCollation() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());
        RelationalTableDefinition current = current();
        SchemaSnapshot snapshot = SchemaSnapshot.builder(RelationIdentity.of(null, "app", "items"))
                .tablePresent()
                .physicalColumns(List.of(ColumnDefinition.builder("code", "VARCHAR").length(16).nullable(false)
                        .collation("Latin1_General_100_BIN2").comment("keep").build())).build();
        SchemaMigrationPlan migration = renderer.migrationPlanner().migrateSafelyPlan(
                current, target(), List.of(), List.of(), SchemaMigrationOptions.safe(), snapshot);
        SchemaMigrationPlan reviewed = renderer.renderPlan(current, snapshot, migration, SchemaMigrationOptions.safe());
        assertEquals(1, migration.requests().size());
        for (String sql : List.of(migration.requests().getFirst().sql())) {
            assertTrue(sql.toLowerCase(Locale.ROOT).contains("collate Latin1_General_100_BIN2".toLowerCase(Locale.ROOT)), sql);
        }
    }

    @Test
    void columnRewritePreservesPhysicalDefaultCommentAndCollation() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.mysql());
        RelationalTableDefinition current = current();
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
        SchemaMigrationPlan reviewed = renderer.renderPlan(current, snapshot, migration, SchemaMigrationOptions.safe());
        assertEquals(1, migration.requests().size());
        String forward = migration.requests().getFirst().sql().toLowerCase(Locale.ROOT);
        assertTrue(forward.contains("varchar(32)"), forward);
        for (String sql : List.of(forward)) {
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
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.mysql());
        DynamicField target = DynamicField.primaryKey("id", "BIGINT")
                .withGeneration(ValueGeneration.identity()).withComment("keep");
        ColumnDefinition physical = ColumnDefinition.builder("id", "INT").nullable(false)
                .generation(ValueGeneration.identity()).comment("keep").build();
        SchemaSnapshot snapshot = SchemaSnapshot.builder(RelationIdentity.table("items")).tablePresent()
                .physicalColumns(List.of(physical)).build();
        DynamicForm sourceForm = DynamicForm.builder("items", "items")
                .addField(new DynamicField(physical, true, false)).build();
        DynamicForm targetForm = DynamicForm.builder("items", "items").addField(target).build();
        String sql = renderer.migrate(sourceForm.diffTo(targetForm), snapshot)
                .getFirst().sql().toLowerCase(Locale.ROOT);
        assertTrue(sql.contains("auto_increment"), sql);
        assertTrue(sql.contains("comment 'keep'"), sql);
        assertEquals(-1, sql.indexOf("primary key"));
    }

    private static RelationalTableDefinition current() {
        return RelationalTableDefinition.builder("app.items")
                .addColumn(DynamicField.of("code", "VARCHAR")
                        .withLength(16).withNullable(false).withComment("keep").column()).build();
    }

    private static DynamicForm target() {
        return DynamicForm.builder("items", "app.items")
                .addField(DynamicField.of("code", "VARCHAR")
                        .withLength(32).withNullable(false).withComment("keep")).build();
    }
}
