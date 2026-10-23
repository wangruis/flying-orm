package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaColumnRenderingParityTest {
    @Test
    void equivalentDeclaredColumnTypesAreComparedOnlyOnce() {
        AtomicInteger mappings = new AtomicInteger();
        SchemaDialect dialect = SchemaDialect.builder().mapType("label", type -> {
            mappings.incrementAndGet();
            return "VARCHAR(32)";
        }).build();
        DynamicForm source = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("name", "VARCHAR").withLength(32)).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("name", "label")).build();

        assertTrue(FormSchemaSqlRenderer.create(dialect).migrate(source.diffTo(target)).isEmpty());
        assertEquals(1, mappings.get(), "an equivalent declaration must not enter column operation planning");
    }

    @Test
    void directChangesUseTheSharedNullabilityDdl() {
        DynamicField field = DynamicField.of("code", "VARCHAR").withLength(32);
        DynamicForm source = DynamicForm.builder("items", "items").addField(field).build();
        DynamicForm target = DynamicForm.builder("items", "items").addField(field.withNullable(false)).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());

        assertEquals(List.of("alter table \"items\" alter column \"code\" set not null"),
                renderer.migrate(source.diffTo(target)).stream().map(com.flying.orm.core.sql.render.SqlRequest::sql).toList());
        assertEquals(List.of("alter table \"items\" alter column \"code\" drop not null"),
                renderer.migrate(target.diffTo(source)).stream().map(com.flying.orm.core.sql.render.SqlRequest::sql).toList());
    }

    @Test
    void directChangesAddAndRemoveTheDeclaredUniqueIndex() {
        DynamicField field = DynamicField.of("code", "VARCHAR").withLength(32);
        DynamicForm source = DynamicForm.builder("items", "items").addField(field).build();
        DynamicForm target = DynamicForm.builder("items", "items").addField(field.withUnique(true)).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        IndexDefinition index = target.toTableDefinition().indexes().getFirst();

        assertEquals(List.of("create unique index \"" + index.name() + "\" on \"items\" (\"code\" asc)"),
                renderer.migrate(source.diffTo(target)).stream().map(com.flying.orm.core.sql.render.SqlRequest::sql).toList());
        assertEquals(List.of("drop index \"" + index.name() + "\""),
                renderer.migrate(target.diffTo(source)).stream().map(com.flying.orm.core.sql.render.SqlRequest::sql).toList());
    }

    @Test
    void directChangesDropTheUniqueIndexBeforeItsColumn() {
        DynamicField id = DynamicField.of("id", "BIGINT");
        DynamicForm source = DynamicForm.builder("items", "items").addField(id)
                .addField(DynamicField.of("code", "VARCHAR").withLength(32).withUnique(true)).build();
        DynamicForm target = DynamicForm.builder("items", "items").addField(id).build();
        String index = source.toTableDefinition().indexes().getFirst().name();

        assertEquals(List.of("drop index \"" + index + "\"", "alter table \"items\" drop column \"code\""),
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()).migrate(source.diffTo(target)).stream()
                        .map(com.flying.orm.core.sql.render.SqlRequest::sql).toList());
    }

    @Test
    void incrementalPlanningAppliesAnExplicitDefaultAndPreservesAnOmittedOne() {
        ColumnDefinition current = ColumnDefinition.builder("status", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("OLD")).build();
        com.flying.orm.core.metadata.RelationalTableDefinition table =
                com.flying.orm.core.metadata.RelationalTableDefinition.builder("items").addColumn(current).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(new DynamicField(current.toBuilder().defaultValue(ColumnDefault.literal("NEW")).build(),
                        false, false)).build();
        assertEquals(List.of("alter table \"items\" alter column \"status\" set default E'NEW'"),
                renderer.migrateSafelyPlan(table, target, List.of()).sqlTexts());
        DynamicForm unchanged = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("status", "VARCHAR").withLength(32)).build();
        assertTrue(renderer.migrateSafelyPlan(table, unchanged, List.of()).requests().isEmpty());
    }

    @Test
    void fullColumnRewriteStillUsesAConfiguredSeparateCommentStatement() {
        SchemaDialect dialect = SchemaDialect.builder().quoteIdentifiers('`')
                .generatedValues(SchemaDialect.GeneratedValueStyle.MYSQL).commentOnColumn().build();
        DynamicField field = DynamicField.of("code", "VARCHAR").withLength(32).withComment("before");
        DynamicForm source = DynamicForm.builder("items", "items").addField(field).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(field.withLength(64).withComment("after")).build();

        assertEquals(List.of("alter table `items` modify column `code` VARCHAR(64)",
                        "comment on column `items`.`code` is 'after'"),
                FormSchemaSqlRenderer.create(dialect)
                        .migrate(source.diffTo(target), SchemaSnapshot.present(source.toTableDefinition()))
                        .stream().map(com.flying.orm.core.sql.render.SqlRequest::sql).toList());
    }

    @Test
    void directMigrationAppliesDefaultOnlyChangesIncludingRemoval() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        ColumnDefinition empty = ColumnDefinition.builder("status", "VARCHAR").length(32).build();
        ColumnDefinition first = empty.toBuilder().defaultValue(ColumnDefault.literal("NEW")).build();
        ColumnDefinition second = empty.toBuilder().defaultValue(ColumnDefault.literal("READY")).build();
        for (ColumnDefinition[] pair : List.of(new ColumnDefinition[]{empty, first},
                new ColumnDefinition[]{first, second}, new ColumnDefinition[]{second, empty})) {
            DynamicForm source = DynamicForm.builder("items", "items")
                    .addField(new DynamicField(pair[0], false, false)).build();
            DynamicForm target = DynamicForm.builder("items", "items")
                    .addField(new DynamicField(pair[1], false, false)).build();
            String suffix = pair[1] == empty ? "drop default" : pair[1] == first
                    ? "set default E'NEW'" : "set default E'READY'";
            assertEquals(List.of("alter table \"items\" alter column \"status\" " + suffix),
                    renderer.migrate(source.diffTo(target)).stream()
                            .map(com.flying.orm.core.sql.render.SqlRequest::sql).toList());
        }
    }

    @Test
    void observedPrimaryKeyDoesNotInventANullabilityMigration() {
        com.flying.orm.core.metadata.RelationalTableDefinition current =
                com.flying.orm.core.metadata.RelationalTableDefinition.builder("items")
                        .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                        .primaryKey(new com.flying.orm.core.metadata.PrimaryKeyDefinition("pk_items", List.of("id")))
                        .build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                .migrateSafelyPlan(current, target, List.of());

        assertTrue(plan.requests().isEmpty());
        assertTrue(plan.skippedChanges().isEmpty());
    }

    @Test
    void ordinaryIndexCreationUsesCanonicalDirectionAndOracleNamespace() {
        IndexDefinition index = IndexDefinition.builder("ix_items").unique()
                .addColumn("id").addKey(IndexKeyPart.desc("code")).build();
        assertEquals("create unique index \"tenant\".\"ix_items\" on \"tenant\".\"items\" "
                        + "(\"id\" asc, \"code\" desc)",
                FormSchemaSqlRenderer.create(RdbDialect.oracle())
                        .createIndexes("tenant.items", List.of(index)).getFirst().sql());
    }

    @Test
    void ordinaryCreationRetainsUnqualifiedIndexNameContract() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        IndexDefinition index = IndexDefinition.builder("other.ix_items").addColumn("id").build();
        assertThrows(IllegalArgumentException.class, () -> FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                .createTablePlan(form, List.of(index)));
    }

    @Test
    void ordinarySelfReferenceWaitsForItsUniqueIndex() {
        DynamicForm form = DynamicForm.builder("items", "tenant.items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("code", "BIGINT"))
                .addField(DynamicField.of("parent_code", "BIGINT")).build();
        IndexDefinition index = IndexDefinition.builder("uk_code").unique().addColumn("code").build();
        ForeignKeyDefinition foreignKey = ForeignKeyDefinition.builder("fk_parent")
                .addColumn("parent_code").reference(RelationIdentity.of(null, "tenant", "items"))
                .addReferenceColumn("code").build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                .createTablePlan(form, List.of(index), List.of(foreignKey));

        assertTrue(plan.skippedChanges().isEmpty());
        assertEquals(List.of(
                "create table \"tenant\".\"items\" (\"id\" BIGINT not null, \"code\" BIGINT, \"parent_code\" BIGINT, "
                        + "constraint \"pk_items\" primary key (\"id\"))",
                "create unique index \"uk_code\" on \"tenant\".\"items\" (\"code\" asc)",
                "alter table \"tenant\".\"items\" add constraint \"fk_parent\" foreign key (\"parent_code\") "
                        + "references \"tenant\".\"items\" (\"code\")"), plan.sqlTexts());
    }

    @Test
    void ordinaryCreationIncludesExplicitForeignKeyInsteadOfSkippingIt() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("parent_id", "BIGINT")).build();
        ForeignKeyDefinition foreignKey = ForeignKeyDefinition.builder("fk_parent")
                .addColumn("parent_id").reference(RelationIdentity.table("parents"))
                .addReferenceColumn("id").build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                .createTablePlan(form, List.of(), List.of(foreignKey));

        assertTrue(plan.skippedChanges().isEmpty());
        assertEquals(List.of("create table \"items\" (\"id\" BIGINT not null, \"parent_id\" BIGINT, "
                + "constraint \"pk_items\" primary key (\"id\"), "
                + "constraint \"fk_parent\" foreign key (\"parent_id\") references \"parents\" (\"id\"))"),
                plan.requests().stream().map(com.flying.orm.core.sql.render.SqlRequest::sql).toList());
    }

    @Test
    void dynamicFieldDdlUsesItsDeclaredDefault() {
        DynamicField field = new DynamicField(ColumnDefinition.builder("status", "VARCHAR")
                .length(32).defaultValue(ColumnDefault.literal("NEW")).build(), false, false);
        String sql = RelationalSchemaSqlRenderer.create(RdbDialect.postgresql().schema()).columnDefinition(field.column());

        assertEquals("\"status\" VARCHAR(32) default E'NEW'", sql);
    }
    @Test
    void creatingOrdinaryColumnsDoesNotRepeatTypeMappingForSequencePlanning() {
        AtomicInteger calls = new AtomicInteger();
        SchemaDialect dialect = SchemaDialect.builder().mapType("label", type -> {
            calls.incrementAndGet();
            return "VARCHAR(80)";
        }).build();
        DynamicForm.Builder form = DynamicForm.builder("sample", "sample");
        for (int index = 0; index < 32; index++) {
            form.addField(DynamicField.of("label_" + index, "label"));
        }

        List<com.flying.orm.core.sql.render.SqlRequest> requests =
                FormSchemaSqlRenderer.create(dialect).createTable(form.build());

        assertEquals(1, requests.size());
        assertEquals(32, calls.get());
        assertTrue(requests.getFirst().sql().contains("label_31 VARCHAR(80)"));
    }

    @Test
    void sequencePlanningDoesNotInspectTypesOfOrdinaryOrIdentityColumns() {
        AtomicInteger calls = new AtomicInteger();
        List<ColumnDefinition> fields = List.of(DynamicField.of("ordinary", "VARCHAR").column(),
                DynamicField.of("identity", "BIGINT").generatedByIdentity().column());
        java.util.Map<String, SchemaSequenceSqlRenderer.Definition> sequences = SchemaSequenceSqlRenderer.definitions(
                RdbDialect.postgresql().schema(), fields, field -> {
            calls.incrementAndGet();
            return "BIGINT";
        }, true);

        assertTrue(sequences.isEmpty());
        assertEquals(0, calls.get());
    }

    @Test
    void ordinaryCreationPreservesColumnTypesNullabilityGenerationAndComments() {
        DynamicForm form = DynamicForm.builder("sample", "sample")
                .addField(DynamicField.primaryKey("id", "BIGINT").generatedByIdentity())
                .addField(DynamicField.of("text", "VARCHAR").withLength(80).withComment("用户'说明"))
                .addField(DynamicField.of("amount", "DECIMAL").withPrecision(18, 4).withNullable(false))
                .addField(DynamicField.of("created", "TIMESTAMP").withPrecision(0, null)).build();
        List<String> sql = FormSchemaSqlRenderer.create(RdbDialect.postgresql()).createTable(form)
                .stream().map(com.flying.orm.core.sql.render.SqlRequest::sql).toList();

        assertTrue(sql.getFirst().contains("\"id\" BIGINT generated by default as identity"));
        assertTrue(sql.getFirst().contains("primary key"));
        assertTrue(sql.getFirst().contains("\"text\" VARCHAR(80)"));
        assertTrue(sql.getFirst().contains("\"amount\" NUMERIC(18,4) not null"), sql::toString);
        assertTrue(sql.getFirst().contains("\"created\" TIMESTAMP(0)"));
        assertEquals("comment on column \"sample\".\"text\" is E'用户''说明'", sql.getLast());
    }

    @Test
    void legacyTypeMappingRunsOnlyOnceAndAddedSequenceDefaultsStayNamed() {
        AtomicInteger calls = new AtomicInteger();
        SchemaDialect dialect = SchemaDialect.builder().mapType("label", type -> {
            calls.incrementAndGet();
            return "VARCHAR(80)";
        }).build();
        String column = RelationalSchemaSqlRenderer.create(dialect).columnDefinition(DynamicField.of("name", "label").column());
        assertTrue(column.contains("VARCHAR(80)"));
        assertEquals(1, calls.get());
        RelationalSchemaSqlRenderer sqlServer = RelationalSchemaSqlRenderer.create(RdbDialect.sqlServer().schema());
        DynamicField generated = DynamicField.of("id", "BIGINT").generatedBySequence("id_seq");
        String added = sqlServer.addColumn(RelationIdentity.table("records"), generated, new java.util.LinkedHashMap<>()).getLast().sql();
        assertTrue(added.contains(" constraint "), added);
        assertTrue(added.contains(" default next value for "), added);
        assertTrue(added.endsWith(" null"), added);
    }
}
