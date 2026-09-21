package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.type.DatabaseTypes;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaUuidTypeMappingTest {

    @Test
    void preservesUuidIdentityForCertifiedBuiltInNativeUuidDialects() {
        assertEquals("UUID", RdbDialect.h2().schema().dataType("UUID"));
        assertEquals("UUID", RdbDialect.postgresql().schema().dataType("UUID"));

        assertEquals("UUID", DatabaseTypes.logicalDeclaration("uuid", "h2"));
        assertEquals("UUID", DatabaseTypes.logicalDeclaration("uuid", "postgresql"));
    }

    @Test
    void mapsUuidToTheBuiltInPhysicalStorageType() {
        assertEquals("CHAR(36)", RdbDialect.mysql().schema().dataType("UUID"));
        assertEquals("VARCHAR2(36)", RdbDialect.oracle().schema().dataType("UUID"));
        assertEquals("UNIQUEIDENTIFIER", RdbDialect.sqlServer().schema().dataType("UUID"));
        assertTrue(createTableSql(RdbDialect.mysql(), "UUID").contains("CHAR(36)"));
        assertTrue(createTableSql(RdbDialect.oracle(), "UUID").contains("VARCHAR2(36)"));
        assertTrue(createTableSql(RdbDialect.sqlServer(), "UUID").contains("UNIQUEIDENTIFIER"));
    }

    @Test
    void physicalUuidStorageDoesNotProduceARepeatedSchemaTypeChange() {
        RelationIdentity identity = RelationIdentity.table("uuid_values");
        RelationalTableDefinition desired = RelationalTableDefinition.builder(identity)
                .addColumn(ColumnDefinition.builder("value", "UUID").build()).build();
        Map<RdbDialect, String> physicalTypes = Map.of(
                RdbDialect.h2(), "UUID", RdbDialect.postgresql(), "UUID",
                RdbDialect.mysql(), "CHAR(36)", RdbDialect.oracle(), "VARCHAR2(36)",
                RdbDialect.sqlServer(), "UNIQUEIDENTIFIER");
        physicalTypes.forEach((dialect, physicalType) -> {
            SchemaSnapshot actual = SchemaSnapshot.builder(identity).tablePresent()
                    .physicalColumns(List.of(ColumnDefinition.builder("value", physicalType).build()))
                    .tableCommentAbsent().primaryKeyAbsent().uniqueConstraints(List.of())
                    .indexes(List.of()).foreignKeys(List.of()).checks(List.of()).partitionAbsent().build();
            SchemaCompatibilityReport report = SchemaDiffer.diff(desired, actual, dialect.capabilities(),
                    SchemaCompatibilityMode.EXACT, dialect.name(), dialect.schema());
            assertTrue(report.operations().isEmpty(), dialect.name() + ": " + report.operations());
        });
    }

    @Test
    void permitsAnExplicitUuidMappingForAQualifiedCustomDialect() {
        SchemaDialect dialect = SchemaDialect.builder()
                                               .mapType("UUID", "CHAR(36)")
                                               .build();

        assertEquals("CHAR(36)", dialect.dataType("UUID"));
    }

    @Test
    void permitsPhysicalUuidNamesWithoutTreatingThemAsUnmappedLogicalUuid() {
        assertEquals("UNIQUEIDENTIFIER", RdbDialect.sqlServer().schema().dataType("UNIQUEIDENTIFIER"));
        assertEquals("pg_catalog.uuid", RdbDialect.postgresql().schema().dataType("pg_catalog.uuid"));
        assertEquals("pg_catalog.uuid[]", RdbDialect.postgresql().schema().dataType("pg_catalog.uuid[]"));

        assertTrue(createTableSql(RdbDialect.sqlServer(), "UNIQUEIDENTIFIER")
                .contains("UNIQUEIDENTIFIER"));
        assertTrue(createTableSql(RdbDialect.postgresql(), "pg_catalog.uuid[]")
                .contains("pg_catalog.uuid[]"));
    }

    @Test
    void rejectsUnmappedPlainUuidArrays() {
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.mysql().schema().dataType("UUID[]"));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.oracle().schema().dataType("UUID[]"));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.sqlServer().schema().dataType("UUID[]"));
    }

    private static String createTableSql(RdbDialect dialect, String dataType) {
        RelationIdentity identity = RelationIdentity.table("uuid_values");
        RelationalTableDefinition table = RelationalTableDefinition.builder(identity)
                .addColumn(ColumnDefinition.builder("value", dataType).build())
                .build();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, identity, identity.table(), null, table,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
        return RelationalSchemaSqlRenderer.create(dialect.schema()).render(operation).getFirst().sql();
    }
}
