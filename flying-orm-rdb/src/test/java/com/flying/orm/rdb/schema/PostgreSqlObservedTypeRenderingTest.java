package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PostgreSqlObservedTypeRenderingTest {

    private final SchemaDialect dialect = RdbDialect.postgresql().schema();
    private final RelationIdentity relation = RelationIdentity.table("type_fidelity");

    @Test
    void rebuildsIndependentPhysicalArgumentsWithoutLogicalRemapping() {
        assertEquals("pg_catalog.varchar(32)",
                     observed(column("pg_catalog.varchar", 32, null, null, null)));
        assertEquals("pg_catalog.varchar(32)[]",
                     observed(column("pg_catalog.varchar[]", 32, null, null, null)));
        assertEquals("pg_catalog.numeric(12,3)",
                     observed(column("pg_catalog.numeric", null, 12, 3, null)));
        assertEquals("pg_catalog.timestamp(3)",
                     observed(column("pg_catalog.timestamp", null, null, null, 3)));
    }

    @Test
    void keepsInlinePhysicalArgumentsAuthoritativeWhenMetadataAlsoProjectsThem() {
        assertEquals("pg_catalog.numeric(12,3)",
                     observed(column("pg_catalog.numeric(12,3)", null, 12, 3, null)));
        assertEquals("pg_catalog.timestamptz(3)[]",
                     observed(column("pg_catalog.timestamptz(3)[]", null, null, null, null)));
    }

    @Test
    void comparesEveryTruePostgreSqlAliasWithoutLosingArgumentsOrArrays() {
        assertEquivalent("pg_catalog.int2", "SMALLINT", null);
        assertEquivalent("pg_catalog.int4[]", "INTEGER[]", null);
        assertEquivalent("pg_catalog.int4[]", "INTEGER[][]", null);
        assertEquivalent("int", "INTEGER", null);
        assertEquivalent("pg_catalog.int8", "BIGINT", null);
        assertEquivalent("decimal(12,3)[]", "NUMERIC(12,3)[]", null);
        assertEquivalent("float4", "REAL", null);
        assertEquivalent("float8[]", "DOUBLE PRECISION[]", null);
        assertEquivalent("bool", "BOOLEAN", null);
        assertEquivalent("character varying(32)", "VARCHAR", 32);
        assertEquivalent("national character varying(32)[]", "VARCHAR[]", 32);
        assertEquivalent("bpchar(16)[]", "CHAR[]", 16);
        assertEquivalent("character(16)", "CHAR", 16);
        assertEquivalent("varbit(8)[]", "BIT VARYING(8)[]", null);
        assertEquivalent("timestamp(3) with time zone[]", "TIMESTAMPTZ(3)[]", null);
        assertEquivalent("timestamp(2) without time zone", "TIMESTAMP(2)", null);
        assertEquivalent("time(1) with time zone[]", "TIMETZ(1)[]", null);
        assertEquivalent("time(4) without time zone", "TIME(4)", null);
    }

    @Test
    void exposesNonEquivalentPhysicalFamiliesToSchemaDiff() {
        assertChange("pg_catalog.int2", "INTEGER", null);
        assertChange("pg_catalog.bpchar(32)[]", "VARCHAR[]", 32);
        assertChange("pg_catalog.varchar", "VARCHAR", null);
        assertChange("pg_catalog.numeric", "DECIMAL", null);
        assertChange("pg_catalog.json", "JSON", null);
        assertChange("app.customer_email", "VARCHAR", null);
        assertChange("extensions.citext", "citext", null);
        assertChange("app.order_state", "audit.order_state", null);
        assertChange("serial", "INTEGER", null);
    }

    @Test
    void keepsPublicLogicalSnapshotsOnTheExistingDialectMappingPath() {
        for (String logical : List.of("VARCHAR", "DECIMAL", "JSON", "BLOB", "TIMESTAMP")) {
            assertEquivalent(logical, logical, null);
        }
    }

    @Test
    void recognizesEverySupportedCatalogBuiltInWithoutWeakeningQualifiedIdentity() {
        for (String type : List.of(
                "aclitem", "bit", "bool", "box", "bytea", "cid", "cidr", "circle",
                "date", "daterange", "datemultirange", "float4", "float8", "inet", "int2",
                "int2vector", "int4", "int4range", "int4multirange", "int8", "int8range",
                "int8multirange", "interval", "jsonb", "jsonpath", "line", "lseg",
                "macaddr", "macaddr8", "money", "name", "nummultirange", "numrange",
                "oid", "oidvector", "path", "pg_lsn", "pg_snapshot", "point", "polygon",
                "refcursor", "regclass", "regcollation", "regconfig", "regdictionary", "regnamespace",
                "regoper", "regoperator", "regproc", "regprocedure", "regrole", "regtype", "text",
                "tid", "time", "timestamp", "timestamptz", "timetz", "tsmultirange", "tsquery",
                "tsrange", "tstzmultirange", "tstzrange", "tsvector", "txid_snapshot", "uuid",
                "varbit", "xid", "xid8", "xml")) {
            assertEquivalent("pg_catalog." + type, type, null);
            assertEquivalent("pg_catalog." + type + "[]", type + "[][]", null);
        }
        assertChange("pg_catalog.custom_domain", "custom_domain", null);
    }

    @Test
    void normalizesPostgreSqlAliasesAndDefaultTypmodsWithoutErasingPhysicalDifferences() {
        assertEquivalent("pg_catalog.money", "MONEY", null);
        assertEquivalent("pg_catalog.money[]", "MONEY[][]", null);
        assertEquivalent("pg_catalog.numeric", "DEC", null);
        assertEquivalent("pg_catalog.numeric(10,0)", "NUMERIC(10)", null);
        assertEquivalent("pg_catalog.numeric(10,0)[]", "DEC(10)[][]", null);
        assertEquivalent("pg_catalog.bpchar", "BPCHAR", null);
        assertEquivalent("pg_catalog.bpchar[]", "BPCHAR[][]", null);
        assertEquivalent("pg_catalog.bpchar(1)", "CHAR", null);
        assertEquivalent("pg_catalog.bpchar(1)[]", "NCHAR[][]", null);
        assertEquivalent("pg_catalog.bpchar(4)", "NCHAR(4)", null);
        assertEquivalent("pg_catalog.bit(1)", "BIT", null);
        assertEquivalent("pg_catalog.bit(1)[]", "BIT[][]", null);
        assertEquivalent("pg_catalog.float8", "FLOAT", null);
        assertEquivalent("pg_catalog.float4", "FLOAT(1)", null);
        assertEquivalent("pg_catalog.float4", "FLOAT(24)", null);
        assertEquivalent("pg_catalog.float8", "FLOAT(25)", null);
        assertEquivalent("pg_catalog.float8[]", "FLOAT(53)[][]", null);

        assertChange("pg_catalog.bpchar", "CHAR", null);
        assertChange("pg_catalog.bpchar", "NCHAR(1)", null);
        assertChange("pg_catalog.numeric", "NUMERIC", null);
        assertChange("pg_catalog.float4", "FLOAT", null);
        assertChange("pg_catalog.float8", "FLOAT(24)", null);
        assertChange("pg_catalog.float4", "FLOAT(25)", null);
        assertChange("pg_catalog.float4", "FLOAT(0)", null);
        assertChange("pg_catalog.float8", "FLOAT(54)", null);
        assertChange("pg_catalog.float", "FLOAT", null);
        assertChange("pg_catalog.float8", "DOUBLE", null);
        assertChange("pg_catalog.int2", "SMALLSERIAL", null);
        assertChange("pg_catalog.int2", "SERIAL2", null);
        assertChange("pg_catalog.int4", "SERIAL", null);
        assertChange("pg_catalog.int4", "SERIAL4", null);
        assertChange("pg_catalog.int8", "BIGSERIAL", null);
        assertChange("pg_catalog.int8", "SERIAL8", null);
        assertChange("pg_catalog.json", "JSONB", null);
        assertChange("pg_catalog.custom_domain", "custom_domain", null);
    }

    @Test
    void preservesDomainsUserTypesIntervalsAndRecursivelyMapsDesiredArrays() {
        assertEquivalent("app.customer_email", "app.customer_email", null);
        assertEquivalent("extensions.vector(3)", "extensions.vector(3)", null);
        assertEquivalent("extensions.citext", "extensions.citext", null);
        assertEquivalent("app.order_state", "app.order_state", null);
        assertEquivalent("vendor.vendor_type[]", "vendor.vendor_type[]", null);
        assertEquivalent("vendor.vendor_type[]", "vendor.vendor_type[][]", null);
        assertEquivalent("pg_catalog.interval", "interval", null);
        assertEquivalent("pg_catalog.interval year to month", "interval year to month", null);
        assertEquivalent("pg_catalog.interval day to second(3)[]", "interval day to second(3)[]", null);
        assertEquivalent("pg_catalog.bytea[]", "BLOB[]", null);
        assertEquivalent("pg_catalog.jsonb[]", "JSON[]", null);
        assertEquivalent("pg_catalog.varchar(32)[]", "VARCHAR[]", 32);
        assertChange("pg_catalog.json[]", "JSON[]", null);
        assertChange("pg_catalog.custom_domain", "custom_domain", null);
        assertEquivalent("_flying_orm_pgvector_extensions.vector(3)", "VECTOR(3)", null);
        assertEquivalent("_flying_orm_pgvector_extensions.vector(3)",
                         "extensions.vector(3)", null);
        assertEquivalent("_flying_orm_pgvector_extensions.vector(3)[]",
                         "extensions.vector(3)[][]", null);
        assertChange("_flying_orm_pgvector_extensions.vector(3)",
                     "extensions.vector(4)", null);
        assertChange("_flying_orm_pgvector_extensions.vector(3)",
                     "audit.vector(3)", null);
        assertChange("_flying_orm_pgvector_extensions.vector(3)",
                     "_flying_orm_pgvector_audit.vector(3)", null);
        assertChange("extensions.vector(3)", "VECTOR(3)", null);
        assertEquals("extensions.vector(3)", SchemaDefinitionEquality.actualColumnDdlType(
                dialect, column("_flying_orm_pgvector_extensions.vector(3)",
                                null, null, null, null)));
    }

    private String observed(ColumnDefinition column) {
        return SchemaDefinitionEquality.observedColumnType(dialect, column);
    }

    private void assertEquivalent(String observedType, String desiredType, Integer length) {
        assertOperations(observedType, desiredType, length, List.of());
    }

    private void assertChange(String observedType, String desiredType, Integer length) {
        assertOperations(observedType, desiredType, length, List.of(SchemaOperation.Kind.CHANGE_COLUMN));
    }

    private void assertOperations(String observedType,
                                  String desiredType,
                                  Integer length,
                                  List<SchemaOperation.Kind> expected) {
        RelationalTableDefinition actual = table(observedType, length);
        RelationalTableDefinition desired = table(desiredType, length);
        List<SchemaOperation.Kind> operations = SchemaDiffer.diff(
                        desired,
                        SchemaSnapshot.present(actual),
                        RdbDialect.postgresql().capabilities(),
                        SchemaCompatibilityMode.EXACT,
                        "postgresql",
                        dialect)
                .operations().stream()
                .map(SchemaOperation::kind)
                .toList();
        assertEquals(expected, operations, observedType + " -> " + desiredType);
    }

    private RelationalTableDefinition table(String type, Integer length) {
        ColumnDefinition.Builder builder = ColumnDefinition.builder("value", type);
        if (length != null) {
            builder.length(length);
        }
        return RelationalTableDefinition.builder(relation).addColumn(builder.build()).build();
    }

    private static ColumnDefinition column(String type,
                                           Integer length,
                                           Integer precision,
                                           Integer scale,
                                           Integer temporalPrecision) {
        return ColumnDefinition.builder("value", type)
                .length(length)
                .precision(precision)
                .scale(scale)
                .temporalPrecision(temporalPrecision)
                .build();
    }
}
