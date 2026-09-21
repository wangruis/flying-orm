package com.flying.orm.rdb.metadata;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgreSqlPhysicalTypeFidelityTest {

    @Test
    void readsLogicalAndNamespaceStablePhysicalPostgreSqlTypesSeparately() {
        String sql = PostgreSqlMetadataQueries.queries().columnQuery().create(null, "type_fidelity").sql();

        assertTrue(sql.contains("case when c.data_type in ('ARRAY', 'USER-DEFINED'"));
        assertTrue(sql.contains("c.data_type as LOGICAL_DATA_TYPE"));
        assertTrue(sql.contains("pg_catalog.format_type(column_attribute.atttypid,"));
        assertTrue(sql.contains("as PHYSICAL_DATA_TYPE"));
        assertTrue(sql.contains("as PHYSICAL_TYPE_SCHEMA"));
        assertTrue(sql.contains("as PHYSICAL_TYPE_NAME"));
        assertTrue(sql.contains("join pg_catalog.pg_type column_type"));
        assertTrue(sql.contains("left join pg_catalog.pg_type element_type"));
        assertTrue(sql.contains("column_type.typcategory = 'A'"));
        assertTrue(sql.contains("element_type.typarray = column_type.oid"));
        assertTrue(sql.contains("as PHYSICAL_TYPE_EXTENSION"));
    }

    @Test
    void separatesCrudLogicalTypesFromSchemaPhysicalDeclarations() {
        InformationSchemaFormMetadataReader.Queries queries = PostgreSqlMetadataQueries.queries();

        assertEquals("BLOB[]", queries.typeMapper().apply("bytea[]"));
        for (String declaration : List.of(
                "bytea[]", "json[]", "jsonb", "character varying", "numeric",
                "customer_email", "vector(3)", "citext", "order_state",
                "interval", "interval day to second(3)")) {
            assertEquals(declaration, queries.snapshotTypeMapper().apply(declaration));
        }
        assertThrows(IllegalArgumentException.class,
                     () -> queries.snapshotTypeMapper().apply("vendor_type; drop table accounts"));

        assertEquals("pg_catalog.int2", physical("smallint", "pg_catalog", "int2", false));
        assertEquals("pg_catalog.int2vector",
                     physical("int2vector", "pg_catalog", "int2vector", false));
        assertEquals("pg_catalog.oidvector",
                     physical("oidvector", "pg_catalog", "oidvector", false));
        assertEquals("pg_catalog.varchar(32)[]",
                     physical("character varying(32)[]", "pg_catalog", "varchar", true));
        assertEquals("app.customer_email", physical("app.customer_email", "app", "customer_email", false));
        assertEquals("_flying_orm_pgvector_extensions.vector(3)",
                     physical("vector(3)", "extensions", "vector", false, "vector"));
        assertEquals("extensions.vector(3)",
                     physical("vector(3)", "extensions", "vector", false, null));
        assertEquals("pg_catalog.interval day to second(3)",
                     physical("interval day to second(3)", "pg_catalog", "interval", false));
        assertThrows(IllegalStateException.class,
                     () -> physical("QuotedType", "app", "QuotedType", false));
    }

    private static String physical(String formatted, String schema, String name, boolean array) {
        return physical(formatted, schema, name, array, null);
    }

    private static String physical(String formatted,
                                   String schema,
                                   String name,
                                   boolean array,
                                   String extension) {
        Map<String, Object> row = new java.util.HashMap<>(Map.of(
                "PHYSICAL_DATA_TYPE", formatted,
                "PHYSICAL_TYPE_SCHEMA", schema,
                "PHYSICAL_TYPE_NAME", name,
                "PHYSICAL_ARRAY", array));
        if (extension != null) {
            row.put("PHYSICAL_TYPE_EXTENSION", extension);
        }
        return FormMetadataRowConverter.postgresqlPhysicalType(
                row,
                PostgreSqlMetadataQueries.queries().snapshotTypeMapper());
    }
}
