package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaTypeRendererTest {

    @Test
    void undeclaredObservedColumnOptionsDoNotTurnValueInequalityIntoAlter() {
        RelationIdentity relation = RelationIdentity.table("labels");
        ColumnDefinition desired = ColumnDefinition.builder("status", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("NEW")).build();
        ColumnDefinition observed = ColumnDefinition.builder("status", "NVARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("NEW")).defaultConstraintName("df_status")
                .collation("Latin1_General_100_BIN2").build();
        org.junit.jupiter.api.Assertions.assertNotEquals(desired, observed);
        RelationalTableDefinition target = RelationalTableDefinition.builder(relation).addColumn(desired).build();
        SchemaSnapshot actual = SchemaSnapshot.builder(relation).tablePresent()
                .physicalColumns(List.of(observed)).tableCommentAbsent().primaryKeyAbsent()
                .uniqueConstraints(List.of()).indexes(List.of()).foreignKeys(List.of())
                .checks(List.of()).partitionAbsent().build();
        RdbDialect dialect = RdbDialect.sqlServer();
        SchemaCompatibilityReport report = SchemaDiffer.diff(target, actual, dialect.capabilities(),
                SchemaCompatibilityMode.EXACT, "column-options", dialect.schema());
        assertTrue(report.operations().isEmpty(), report.operations().toString());
    }

    @Test
    void mapsInlineAndSeparateDimensionsUsingTheSameParsedInput() {
        SchemaDialect dialect = SchemaDialect.builder()
                .mapType("varchar", type -> Integer.parseInt(type.arguments().getFirst()) > 100
                        ? "TEXT" : "VARCHAR(" + type.arguments().getFirst() + ")")
                .mapType("money", type -> "DECIMAL(" + String.join(",", type.arguments()) + ")")
                .build();

        assertEquals("TEXT", dialect.dataType("VARCHAR(200)"));
        assertEquals("TEXT", dialect.dataType("VARCHAR", 200, null, null));
        assertEquals("VARCHAR(80)", dialect.dataType("VARCHAR(200)", 80, null, null));
        assertEquals("DECIMAL(18,4)", dialect.dataType("money", null, 18, 4));
        assertEquals("VARCHAR(200)[]", dialect.dataType("VARCHAR(200)[]"));
        assertEquals("VARCHAR(200)", dialect.physicalDataType("VARCHAR", 200, null, null));
    }

    @Test
    void snapshotsRegistrationsAndLetsTheLastMappingWin() {
        SchemaDialect.Builder builder = SchemaDialect.builder().mapType("token", "INTEGER");
        SchemaDialect fixed = builder.build();
        builder.mapType("token", type -> "BIGINT");
        SchemaDialect computed = builder.build();
        builder.mapType("token", "TEXT");

        assertEquals("INTEGER", fixed.dataType("token"));
        assertEquals("BIGINT", computed.dataType("token"));
        assertEquals("TEXT", builder.build().dataType("token"));
        assertEquals("token", SchemaDialect.standard().dataType("token"));
    }

    @Test
    void prefersExactDeclarationsAndDoesNotOverrideExplicitFixedMappings() {
        SchemaDialect dialect = SchemaDialect.builder()
                .mapType("varchar", type -> "TEXT")
                .mapType("varchar(80)", type -> "VARCHAR(80)")
                .mapType("varchar(40)", "CHAR(40)").build();
        assertEquals("VARCHAR(80)", dialect.dataType("varchar(80)"));
        assertEquals("VARCHAR(80)", dialect.dataType("varchar", 80, null, null));
        assertEquals("TEXT", dialect.dataType("varchar(80)", 20, null, null));
        assertEquals("CHAR(40)", dialect.dataType("varchar(40)"));
        assertEquals("TEXT", dialect.dataType("varchar(20)"));
    }

    @Test
    void comparesMappedTargetsWithPhysicalSnapshotsWithoutRepeatedChanges() {
        SchemaDialect dialect = SchemaDialect.builder()
                .mapType("label", type -> "VARCHAR(" + type.arguments().getFirst() + ")").build();
        RelationIdentity relation = RelationIdentity.table("labels");
        RelationalTableDefinition target = RelationalTableDefinition.builder(relation)
                .addColumn(ColumnDefinition.builder("name", "label").length(80).build()).build();
        SchemaSnapshot actual = SchemaSnapshot.builder(relation).tablePresent()
                .physicalColumns(List.of(ColumnDefinition.builder("name", "VARCHAR(80)").build()))
                .tableCommentAbsent().primaryKeyAbsent().uniqueConstraints(List.of())
                .indexes(List.of()).foreignKeys(List.of()).checks(List.of()).partitionAbsent().build();
        SchemaCompatibilityReport report = SchemaDiffer.diff(target, actual,
                RdbDialect.postgresql().capabilities(), SchemaCompatibilityMode.EXACT, "custom", dialect);
        assertTrue(report.operations().isEmpty(), report.operations().toString());
    }

    @Test
    void doesNotApplyCallbacksToObservedLegacyColumns() {
        SchemaDialect dialect = SchemaDialect.builder().mapType("varchar", type ->
                "VARCHAR(" + (Integer.parseInt(type.arguments().getFirst()) * 2) + ")").build();
        DynamicForm target = DynamicForm.builder("labels", "labels")
                .addField(DynamicField.of("name", "VARCHAR").withLength(80)).build();
        RelationalTableDefinition actual = RelationalTableDefinition.builder("labels")
                .addColumn(DynamicField.of("name", "VARCHAR").withLength(160).column()).build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(dialect)
                .migrateSafelyPlan(actual, target, List.of());
        assertTrue(plan.requests().isEmpty(), plan.requests().toString());
        assertTrue(plan.skippedChanges().isEmpty(), plan.skippedChanges().toString());
    }



    @Test
    void rejectsUnsafeInputAndOutputDeclarations() {
        SchemaDialect dialect = SchemaDialect.builder()
                .mapType("text", type -> "TEXT); DROP TABLE users; --").build();
        assertThrows(IllegalArgumentException.class, () -> dialect.dataType("text"));
        assertThrows(IllegalArgumentException.class,
                () -> dialect.dataType("text; drop table users"));
    }

    @Test
    void letsTheMappedPhysicalTypeDecideWhetherDimensionsAreSupported() {
        SchemaDialect dialect = SchemaDialect.builder()
                .mapType("uuid", "CHAR(36)")
                .mapType("label", type -> "TEXT").build();
        assertEquals("CHAR(36)", dialect.dataType("uuid", 36, null, null));
    }

    @Test
    void usesTheSameMappingForBothSchemaModels() {
        SchemaDialect dialect = SchemaDialect.builder()
                .mapType("label", type -> "VARCHAR(" + type.arguments().getFirst() + ")").build();
        DynamicForm form = DynamicForm.builder("labels", "labels")
                .addField(DynamicField.of("name", "label").withLength(80)).build();
        assertTrue(FormSchemaSqlRenderer.create(dialect).createTable(form).getFirst().sql()
                .contains("name VARCHAR(80)"));

        RelationIdentity relation = RelationIdentity.table("labels");
        ColumnDefinition column = ColumnDefinition.builder("name", "label").length(80).build();
        RelationalTableDefinition table = RelationalTableDefinition.builder(relation).addColumn(column).build();
        SchemaOperation create = SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, relation,
                "labels", null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);
        assertTrue(RelationalSchemaSqlRenderer.create(dialect).render(create).getFirst().sql()
                .contains("name VARCHAR(80)"));
        SchemaOperation add = SchemaOperation.of(SchemaOperation.Kind.ADD_COLUMN, relation,
                "name", null, column, SchemaOperation.Compatibility.REQUIRES_REVIEW);
        assertTrue(RelationalSchemaSqlRenderer.create(dialect).render(add).getFirst().sql()
                .contains("name VARCHAR(80)"));
    }
}
