package com.flying.orm.rdb.dialect;

import com.flying.orm.rdb.json.JsonDialect;
import com.flying.orm.rdb.schema.SchemaDialect;

import java.util.Objects;
import java.util.Set;

/** 五个内置方言的静态 SQL 能力装配；公开入口仍由 {@link RdbDialect} 提供。 */
final class BuiltInRdbDialects {

    private BuiltInRdbDialects() {
    }

    static RdbDialect h2() {
        return RdbDialect.builtIn(
                "h2",
                SchemaDialect.builder()
                        .mapType("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE")
                        .mapType("OFFSET_TIME", "TIME WITH TIME ZONE")
                        .mapType("NCLOB", "CLOB")
                        .mapType("PROTECTED_BINARY", "BLOB")
                        .mapType("PROTECTED_HASH", "BINARY(32)")
                        .inlineColumnComment()
                        .commentOnTable()
                        .generatedValues(SchemaDialect.GeneratedValueStyle.H2)
                        .build(),
                PaginationDialect.limitOffset(),
                UpsertDialect.h2(),
                JsonDialect.h2(),
                "default",
                Set.of(DialectFeature.IDENTITY_COLUMNS,
                       DialectFeature.SEQUENCES,
                       DialectFeature.MERGE_UPSERT),
                256);
    }

    static RdbDialect mysql() {
        return RdbDialect.builtIn(
                "mysql",
                SchemaDialect.builder()
                        .quoteIdentifiers('`')
                        .mapType("BIGINT", "BIGINT")
                        .mapType("INTEGER", "INT")
                        .mapType("INT", "INT")
                        .mapType("VARCHAR", "VARCHAR(255)")
                        .mapType("TEXT", "TEXT")
                        .mapType("CLOB", "LONGTEXT")
                        .mapType("NCLOB", "LONGTEXT")
                        .mapType("BOOLEAN", "BOOLEAN")
                        .mapType("BOOL", "BOOLEAN")
                        .mapType("DECIMAL", "DECIMAL(38,10)")
                        .mapType("NUMERIC", "DECIMAL(38,10)")
                        .mapType("BLOB", "LONGBLOB")
                        .mapType("BINARY", "LONGBLOB")
                        .mapType("MYSQL_BLOB", "BLOB")
                        .mapType("MYSQL_BINARY", "BINARY")
                        .mapType("PROTECTED_BINARY", "LONGBLOB")
                        .mapType("PROTECTED_HASH", "BINARY(32)")
                        .mapType("JSON", "JSON")
                        .mapType("TIMESTAMP", "DATETIME")
                        .mapType("TIMESTAMPTZ", "TIMESTAMP(6)")
                        .mapType("DATETIME", "DATETIME")
                        .mapType("DATE", "DATE")
                        .mapType("TIME", "TIME")
                        .mapType("OFFSET_TIME", "VARCHAR(32)")
                        .inlineColumnComment()
                        .mysqlTableComment()
                        .dropIndexOnTable()
                        .generatedValues(SchemaDialect.GeneratedValueStyle.MYSQL)
                        .operationDependentOnlineDdl()
                        .mysqlLockTimeout()
                        .build(),
                PaginationDialect.limitOffset(),
                UpsertDialect.mysql(),
                JsonDialect.plain(),
                "default",
                Set.of(DialectFeature.IDENTITY_COLUMNS,
                       DialectFeature.MYSQL_RELATIONAL_METADATA),
                64);
    }

    static RdbDialect postgresql() {
        return RdbDialect.builtIn(
                "postgresql",
                SchemaDialect.builder()
                        .quoteIdentifiers('"')
                        .mapType("BIGINT", "BIGINT")
                        .mapType("INTEGER", "INTEGER")
                        .mapType("INT", "INTEGER")
                        .mapType("VARCHAR", "VARCHAR(255)")
                        .mapType("TEXT", "TEXT")
                        .mapType("CLOB", "TEXT")
                        .mapType("NCLOB", "TEXT")
                        .mapType("BOOLEAN", "BOOLEAN")
                        .mapType("DECIMAL", "NUMERIC(38,10)")
                        .mapType("NUMERIC", "NUMERIC(38,10)")
                        .mapType("BLOB", "BYTEA")
                        .mapType("BINARY", "BYTEA")
                        .mapType("PROTECTED_BINARY", "BYTEA")
                        .mapType("PROTECTED_HASH", "BYTEA")
                        .mapType("JSON", "JSONB")
                        .mapType("TIMESTAMP", "TIMESTAMP")
                        .mapType("TIMESTAMPTZ", "TIMESTAMPTZ")
                        .mapType("DATETIME", "TIMESTAMP")
                        .mapType("DATE", "DATE")
                        .mapType("TIME", "TIME")
                        .mapType("OFFSET_TIME", "TIME WITH TIME ZONE")
                        .mapType("VECTOR", "VECTOR")
                        .commentOnColumn()
                        .commentOnTable()
                        .generatedValues(SchemaDialect.GeneratedValueStyle.POSTGRESQL)
                        .concurrentIndexOnlineDdl()
                        .postgresqlLockTimeout()
                        .build(),
                PaginationDialect.limitOffset(),
                UpsertDialect.postgresql(),
                JsonDialect.postgresql(),
                "default",
                Set.of(DialectFeature.JSON_FUNCTIONS,
                       DialectFeature.NATIVE_JSON,
                       DialectFeature.NATIVE_BOOLEAN,
                       DialectFeature.LARGE_OBJECTS,
                       DialectFeature.IDENTITY_COLUMNS,
                       DialectFeature.SEQUENCES,
                       DialectFeature.POSTGRESQL_VECTOR),
                63);
    }

    static RdbDialect oracle(OracleVersion version) {
        OracleVersion safeVersion = Objects.requireNonNull(
                version, "oracle version must not be null");
        String booleanType = safeVersion.nativeBoolean() ? "BOOLEAN" : "NUMBER(1)";
        String jsonType = safeVersion.nativeJson() ? "JSON" : "CLOB";
        return RdbDialect.builtIn(
                "oracle",
                SchemaDialect.builder()
                        .quoteIdentifiers('"')
                        .mapType("BIGINT", "NUMBER(19)")
                        .mapType("INTEGER", "NUMBER(10)")
                        .mapType("INT", "NUMBER(10)")
                        .mapType("VARCHAR", "VARCHAR2(255)")
                        .mapType("TEXT", "CLOB")
                        .mapType("CLOB", "CLOB")
                        .mapType("NCLOB", "NCLOB")
                        .mapType("BOOLEAN", booleanType)
                        .mapType("DECIMAL", "NUMBER(38,10)")
                        .mapType("NUMERIC", "NUMBER(38,10)")
                        .mapType("BLOB", "BLOB")
                        .mapType("BINARY", "BLOB")
                        .mapType("PROTECTED_BINARY", "BLOB")
                        .mapType("PROTECTED_HASH", "RAW(32)")
                        .mapType("JSON", jsonType)
                        .mapType("TIMESTAMP", "TIMESTAMP")
                        .mapType("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE")
                        .mapType("DATETIME", "TIMESTAMP")
                        .mapType("ORACLE_DATE", "DATE")
                        .mapType("DATE", "DATE")
                        .mapType("TIME", "VARCHAR2(16)")
                        .mapType("OFFSET_TIME", "VARCHAR2(32)")
                        .commentOnColumn()
                        .commentOnTable()
                        .generatedValues(SchemaDialect.GeneratedValueStyle.ORACLE)
                        .oracleColumnChanges()
                        .licenseOrEditionDependentOnlineDdl()
                        .oracleLockTimeout()
                        .build(),
                PaginationDialect.offsetFetch(),
                UpsertDialect.oracle(),
                JsonDialect.plain(),
                safeVersion.label(),
                oracleFeatures(safeVersion),
                safeVersion == OracleVersion.V12C ? 30 : 128);
    }

    static RdbDialect sqlServer(SqlServerVersion version) {
        SqlServerVersion safeVersion = Objects.requireNonNull(
                version, "sql server version must not be null");
        return RdbDialect.builtIn(
                "sqlserver",
                SchemaDialect.builder()
                        .quoteIdentifiers('[', ']')
                        .mapType("BIGINT", "BIGINT")
                        .mapType("INTEGER", "INT")
                        .mapType("INT", "INT")
                        .mapType("VARCHAR", "NVARCHAR(255)")
                        .mapType("TEXT", "NVARCHAR(max)")
                        .mapType("CLOB", "NVARCHAR(max)")
                        .mapType("NCLOB", "NVARCHAR(max)")
                        .mapType("BOOLEAN", "BIT")
                        .mapType("DECIMAL", "DECIMAL(38,10)")
                        .mapType("NUMERIC", "DECIMAL(38,10)")
                        .mapType("BLOB", "VARBINARY(max)")
                        .mapType("BINARY", "VARBINARY(max)")
                        .mapType("PROTECTED_BINARY", "VARBINARY(max)")
                        .mapType("PROTECTED_HASH", "BINARY(32)")
                        .mapType("JSON", "NVARCHAR(max)")
                        .mapType("SQLSERVER_DATETIME", "DATETIME")
                        .mapType("SQLSERVER_SMALLDATETIME", "SMALLDATETIME")
                        .mapType("TIMESTAMP", "DATETIME2")
                        .mapType("TIMESTAMPTZ", "DATETIMEOFFSET")
                        .mapType("DATETIME", "DATETIME2")
                        .mapType("DATE", "DATE")
                        .mapType("TIME", "TIME")
                        .mapType("OFFSET_TIME", "VARCHAR(32)")
                        .sqlServerExtendedPropertyComment()
                        .sqlServerExtendedPropertyTableComment()
                        .dropIndexOnTable()
                        .sqlServerRenameColumn()
                        .generatedValues(SchemaDialect.GeneratedValueStyle.SQL_SERVER)
                        .sqlServerColumnChanges()
                        .licenseOrEditionDependentOnlineDdl()
                        .sqlServerLockTimeout()
                        .build(),
                PaginationDialect.sqlServerOffsetFetch(),
                UpsertDialect.sqlServer(),
                JsonDialect.plain(),
                safeVersion.label(),
                sqlServerFeatures(safeVersion),
                128);
    }

    private static Set<DialectFeature> oracleFeatures(OracleVersion version) {
        if (version.nativeBoolean()) {
            return Set.of(DialectFeature.OFFSET_FETCH_PAGINATION,
                          DialectFeature.MERGE_UPSERT,
                          DialectFeature.IDENTITY_COLUMNS,
                          DialectFeature.SEQUENCES,
                          DialectFeature.JSON_FUNCTIONS,
                          DialectFeature.NATIVE_JSON,
                          DialectFeature.NATIVE_BOOLEAN,
                          DialectFeature.LARGE_OBJECTS);
        }
        if (version.nativeJson()) {
            return Set.of(DialectFeature.OFFSET_FETCH_PAGINATION,
                          DialectFeature.MERGE_UPSERT,
                          DialectFeature.IDENTITY_COLUMNS,
                          DialectFeature.SEQUENCES,
                          DialectFeature.JSON_FUNCTIONS,
                          DialectFeature.NATIVE_JSON,
                          DialectFeature.LARGE_OBJECTS);
        }
        return Set.of(DialectFeature.OFFSET_FETCH_PAGINATION,
                      DialectFeature.MERGE_UPSERT,
                      DialectFeature.IDENTITY_COLUMNS,
                      DialectFeature.SEQUENCES,
                      DialectFeature.JSON_FUNCTIONS,
                      DialectFeature.LARGE_OBJECTS);
    }

    private static Set<DialectFeature> sqlServerFeatures(SqlServerVersion version) {
        return version.jsonFunctions()
                ? Set.of(DialectFeature.OFFSET_FETCH_PAGINATION,
                         DialectFeature.MERGE_UPSERT,
                         DialectFeature.IDENTITY_COLUMNS,
                         DialectFeature.SEQUENCES,
                         DialectFeature.JSON_FUNCTIONS,
                         DialectFeature.LARGE_OBJECTS)
                : Set.of(DialectFeature.OFFSET_FETCH_PAGINATION,
                         DialectFeature.MERGE_UPSERT,
                         DialectFeature.IDENTITY_COLUMNS,
                         DialectFeature.SEQUENCES,
                         DialectFeature.LARGE_OBJECTS);
    }
}
