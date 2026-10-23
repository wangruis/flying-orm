package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.type.DatabaseTypes;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnumSchemaTypeTest {

    @Test
    void rendersBothSchemaModelsThroughTheExistingTypeOwner() {
        SchemaDialect dialect = RdbDialect.mysql().schema();
        String type = "ENUM('Active','It''s (Active)','A , B')";
        DynamicForm form = DynamicForm.builder("status", "accounts")
                .addField(DynamicField.of("status", type)).build();
        assertEquals("create table `accounts` (`status` " + type + ")",
                FormSchemaSqlRenderer.create(RdbDialect.mysql()).createTable(form).getFirst().sql());
        ColumnDefinition column = ColumnDefinition.builder("status", type).nullable(false)
                .defaultValue(ColumnDefault.literal("Active"))
                .charset("utf8mb4").collation("utf8mb4_bin").comment("状态").build();
        SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.ADD_COLUMN,
                RelationIdentity.table("accounts"), "status", null, column,
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        SqlRequest request = RelationalSchemaSqlRenderer.create(dialect).render(operation).getFirst();
        assertEquals("alter table `accounts` add column `status` " + type
                + " character set `utf8mb4` collate `utf8mb4_bin` not null default 'Active' comment '状态'",
                request.sql());
        assertTrue(request.parameters().isEmpty());
    }

    @Test
    void preservesLiteralContentsInMetadataAndPhysicalComparison() {
        SchemaDialect dialect = RdbDialect.mysql().schema();
        for (String type : List.of("enum('Active','disabled')", "set('Read  Write','A , B')")) {
            assertEquals(type, DatabaseTypes.logicalDeclaration(type, "mysql"));
            assertEquals(type, dialect.dataType(type));
            assertEquals(type, dialect.physicalDataType(type, null, null, null));
            assertTrue(dialect.sameDataType(type, type.substring(0, 3).toUpperCase(java.util.Locale.ROOT)
                    + type.substring(3)));
        }
        assertFalse(dialect.sameDataType("enum('Active')", "enum('active')"));
        assertFalse(dialect.sameDataType("set('A  B')", "set('A B')"));
        assertFalse(dialect.sameDataType("enum('A','B')", "enum('B','A')"));
        assertFalse(SchemaDialectTypeSupport.safeWideningDataType("enum('1')", "enum('2')"));
    }

    @Test
    void doesNotReplaceEnumMembersWithColumnLengthOrPrecision() {
        SchemaDialect dialect = RdbDialect.mysql().schema();
        for (String type : List.of("enum('A','B')", "set('Read','Write')")) {
            assertThrows(IllegalArgumentException.class, () -> dialect.dataType(type, 64, null, null));
            assertThrows(IllegalArgumentException.class, () -> dialect.dataType(type, null, 10, 2));
            // Physical metadata lengths describe storage, not new ENUM members.
            assertEquals(type, dialect.physicalDataType(type, 64, null, null));
        }
    }

    @Test
    void doesNotGuessHexTextEquivalenceWithoutColumnCharset() {
        SchemaDialect dialect = RdbDialect.mysql().schema();
        assertTrue(dialect.sameDataType("enum(X'c3a9')", "ENUM(x'C3A9')"));
        assertFalse(dialect.sameDataType("enum(X'4142')", "enum('AB')"));
        assertFalse(dialect.sameDataType("set(x'52656164','Write')", "SET('Read','Write')"));
        assertFalse(dialect.sameDataType("enum(X'41')", "enum('a')"));
        assertFalse(dialect.sameDataType("enum(X'41',X'42')", "enum('B','A')"));
        assertFalse(dialect.sameDataType("enum(X'c3a9')", "enum('é')"));
    }

    @Test
    void equivalentQuotedMembersDoNotCauseASchemaMigration() {
        RdbDialect dialect = RdbDialect.mysql();
        RelationIdentity table = RelationIdentity.table("accounts");
        RelationalTableDefinition desired = RelationalTableDefinition.builder(table)
                .addColumn(ColumnDefinition.builder("status", "ENUM ( 'Active', 'It''s done' )").build()).build();
        RelationalTableDefinition actual = RelationalTableDefinition.builder(table)
                .addColumn(ColumnDefinition.builder("status", "enum('Active','It''s done')").build()).build();
        SchemaMigrationPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of("MySQL", "8.4", dialect), desired, SchemaSnapshot.present(actual),
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
        assertTrue(plan.requests().isEmpty());
    }
}
