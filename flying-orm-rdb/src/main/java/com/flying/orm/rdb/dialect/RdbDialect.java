package com.flying.orm.rdb.dialect;

import com.flying.orm.rdb.json.JsonDialect;
import com.flying.orm.rdb.schema.SchemaDialect;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * 聚合一个数据库在 DDL、分页、upsert 和 JSON 上的 SQL 能力。业务渲染器只依赖这个组合对象，
 * 不在各处用数据库名称写分支。
 *
 * <p>内置实例覆盖 H2、MySQL、PostgreSQL、Oracle 和 SQL Server；没有 OpenGauss 专用方言。
 * 上层应用可以根据连接工厂元数据或配置自动选择，独立使用时也允许显式传入。</p>
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
public final class RdbDialect {

    private final String name;

    private final SchemaDialect schema;

    private final PaginationDialect pagination;

    private final UpsertDialect upsert;

    private final JsonDialect json;

    private final String version;

    private final Set<DialectFeature> features;

    private RdbDialect(String name,
                       SchemaDialect schema,
                       PaginationDialect pagination,
                       UpsertDialect upsert,
                       JsonDialect json,
                       String version,
                       Set<DialectFeature> features) {
        this.name = requireText(name, "dialect name").toLowerCase(Locale.ROOT);
        this.schema = Objects.requireNonNull(schema, "schema dialect must not be null");
        this.pagination = Objects.requireNonNull(pagination, "pagination dialect must not be null");
        this.upsert = Objects.requireNonNull(upsert, "upsert dialect must not be null");
        this.json = Objects.requireNonNull(json, "json dialect must not be null");
        this.version = requireText(version, "dialect version");
        this.features = Set.copyOf(Objects.requireNonNull(features, "dialect features must not be null"));
    }

    /**
     * 创建 H2 方言。
     *
     * @return H2 方言
     */
    public static RdbDialect h2() {
        return of("h2",
                  SchemaDialect.builder()
                               .mapType("OFFSET_TIME", "TIME WITH TIME ZONE")
                               .mapType("PROTECTED_BINARY", "BLOB")
                               .mapType("PROTECTED_HASH", "BINARY(32)")
                               .inlineColumnComment()
                               .generatedValues(SchemaDialect.GeneratedValueStyle.H2)
                               .build(),
                  PaginationDialect.limitOffset(),
                  UpsertDialect.h2(),
                  JsonDialect.h2());
    }

    /**
     * 创建 MySQL 方言。
     *
     * @return MySQL 方言
     */
    public static RdbDialect mysql() {
        return of("mysql", SchemaDialect.builder()
                                        .quoteIdentifiers('`')
                                        .mapType("BIGINT", "BIGINT")
                                        .mapType("INTEGER", "INT")
                                        .mapType("INT", "INT")
                                        .mapType("VARCHAR", "VARCHAR(255)")
                                        .mapType("TEXT", "TEXT")
                                        .mapType("CLOB", "LONGTEXT")
                                        .mapType("NCLOB", "LONGTEXT")
                                        .mapType("BOOLEAN", "BOOLEAN")
                                        .mapType("DECIMAL", "DECIMAL(38,10)")
                                        .mapType("NUMERIC", "DECIMAL(38,10)")
                                        .mapType("BLOB", "LONGBLOB")
                                        .mapType("BINARY", "LONGBLOB")
                                        .mapType("PROTECTED_BINARY", "LONGBLOB")
                                        .mapType("PROTECTED_HASH", "BINARY(32)")
                                        .mapType("JSON", "JSON")
                                        .mapType("TIMESTAMP", "DATETIME")
                                        .mapType("DATETIME", "DATETIME")
                                        .mapType("DATE", "DATE")
                                        .mapType("TIME", "TIME")
                                        .mapType("OFFSET_TIME", "VARCHAR(32)")
                                        .inlineColumnComment()
                                        .dropIndexOnTable()
                                        .generatedValues(SchemaDialect.GeneratedValueStyle.MYSQL)
                                        .operationDependentOnlineDdl()
                                        .mysqlLockTimeout()
                                        .build(), PaginationDialect.limitOffset(), UpsertDialect.mysql());
    }

    /**
     * 创建 PostgreSQL 方言。
     *
     * @return PostgreSQL 方言
     */
    public static RdbDialect postgresql() {
        return of("postgresql",
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
                               .mapType("DATETIME", "TIMESTAMP")
                               .mapType("DATE", "DATE")
                               .mapType("TIME", "TIME")
                               .mapType("OFFSET_TIME", "TIME WITH TIME ZONE")
                               .mapType("VECTOR", "VECTOR")
                               .commentOnColumn()
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
                         DialectFeature.POSTGRESQL_VECTOR));
    }

    /**
     * 创建 Oracle 方言。
     *
     * @return Oracle 方言
     */
    public static RdbDialect oracle() {
        return oracle(OracleVersion.V19C);
    }

    /**
     * 按明确版本创建 Oracle 方言。默认 factory 仍使用稳定的 19c 基线；只有调用方声明 21c/23ai，
     * 才会启用原生 JSON 或 SQL BOOLEAN，避免在旧库上生成无法执行的 DDL。
     */
    public static RdbDialect oracle(OracleVersion version) {
        OracleVersion safeVersion = Objects.requireNonNull(version, "oracle version must not be null");
        String booleanType = safeVersion.nativeBoolean() ? "BOOLEAN" : "NUMBER(1)";
        String jsonType = safeVersion.nativeJson() ? "JSON" : "CLOB";
        Set<DialectFeature> features = safeVersion.nativeBoolean()
                ? Set.of(DialectFeature.OFFSET_FETCH_PAGINATION,
                         DialectFeature.MERGE_UPSERT,
                         DialectFeature.IDENTITY_COLUMNS,
                         DialectFeature.SEQUENCES,
                         DialectFeature.JSON_FUNCTIONS,
                         DialectFeature.NATIVE_JSON,
                         DialectFeature.NATIVE_BOOLEAN,
                         DialectFeature.LARGE_OBJECTS)
                : safeVersion.nativeJson()
                        ? Set.of(DialectFeature.OFFSET_FETCH_PAGINATION,
                                 DialectFeature.MERGE_UPSERT,
                                 DialectFeature.IDENTITY_COLUMNS,
                                 DialectFeature.SEQUENCES,
                                 DialectFeature.JSON_FUNCTIONS,
                                 DialectFeature.NATIVE_JSON,
                                 DialectFeature.LARGE_OBJECTS)
                        : Set.of(DialectFeature.OFFSET_FETCH_PAGINATION,
                                 DialectFeature.MERGE_UPSERT,
                                 DialectFeature.IDENTITY_COLUMNS,
                                 DialectFeature.SEQUENCES,
                                 DialectFeature.JSON_FUNCTIONS,
                                 DialectFeature.LARGE_OBJECTS);
        return of("oracle",
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
                               .mapType("DATETIME", "TIMESTAMP")
                               .mapType("DATE", "DATE")
                               .mapType("TIME", "VARCHAR2(16)")
                               .mapType("OFFSET_TIME", "VARCHAR2(32)")
                               .commentOnColumn()
                               .generatedValues(SchemaDialect.GeneratedValueStyle.ORACLE)
                               .oracleColumnChanges()
                               .licenseOrEditionDependentOnlineDdl()
                               .oracleLockTimeout()
                               .build(),
                  PaginationDialect.offsetFetch(),
                  UpsertDialect.oracle(),
                  JsonDialect.plain(),
                  safeVersion.label(),
                  features);
    }

    /**
     * 创建 SQL Server 方言。
     *
     * @return SQL Server 方言
     */
    public static RdbDialect sqlServer() {
        return sqlServer(SqlServerVersion.V2022);
    }

    /**
     * 按明确版本创建 SQL Server 方言。2012 是当前最低代码契约，2016 起额外声明 JSON 函数能力。
     */
    public static RdbDialect sqlServer(SqlServerVersion version) {
        SqlServerVersion safeVersion = Objects.requireNonNull(version, "sql server version must not be null");
        Set<DialectFeature> features = safeVersion.jsonFunctions()
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
        return of("sqlserver",
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
                               .mapType("TIMESTAMP", "DATETIME2")
                               .mapType("DATETIME", "DATETIME2")
                               .mapType("DATE", "DATE")
                               .mapType("TIME", "TIME")
                               .mapType("OFFSET_TIME", "VARCHAR(32)")
                               .sqlServerExtendedPropertyComment()
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
                  features);
    }

    /**
     * 自己组装一个数据库方言，并指定 upsert 写法。
     *
     * @param name       名字，比如 mysql
     * @param schema     建表、改表这类 SQL 的写法
     * @param pagination 分页 SQL 的写法
     * @param upsert     upsert SQL 的写法
     * @return 自定义 RDB 方言
     */
    public static RdbDialect of(String name, SchemaDialect schema, PaginationDialect pagination, UpsertDialect upsert) {
        return of(name, schema, pagination, upsert, JsonDialect.plain());
    }

    /**
     * 自己组装完整方言。数据库对 JSON 参数有特殊类型要求时，用 json 参数把写法交代清楚。
     */
    public static RdbDialect of(String name,
                                SchemaDialect schema,
                                PaginationDialect pagination,
                                UpsertDialect upsert,
                                JsonDialect json) {
        return new RdbDialect(name, schema, pagination, upsert, json, "unspecified", Set.of());
    }

    private static RdbDialect of(String name,
                                 SchemaDialect schema,
                                 PaginationDialect pagination,
                                 UpsertDialect upsert,
                                 JsonDialect json,
                                 String version,
                                 Set<DialectFeature> features) {
        return new RdbDialect(name, schema, pagination, upsert, json, version, features);
    }

    /**
     * 当前数据库方言的名字。
     *
     * @return 方言名称
     */
    public String name() {
        return name;
    }

    /**
     * 建表、改表这类 SQL 的写法。
     *
     * @return 结构 SQL 方言
     */
    public SchemaDialect schema() {
        return schema;
    }

    /**
     * 分页 SQL 的写法。
     *
     * @return 分页 SQL 方言
     */
    public PaginationDialect pagination() {
        return pagination;
    }

    /**
     * insert 遇到主键冲突时怎么改写成更新。
     *
     * @return upsert SQL 方言
     */
    public UpsertDialect upsert() {
        return upsert;
    }

    /**
     * JSON 参数在当前数据库里的绑定表达式。
     */
    public JsonDialect json() {
        return json;
    }

    /** @return 当前方言按哪个数据库版本边界生成 SQL */
    public String version() {
        return version;
    }

    /**
     * 判断当前版本配置是否明确支持某项能力。返回 false 表示 flying-orm 不会承诺生成可执行 SQL，
     * 不等于数据库厂商从未提供该功能。
     */
    public boolean supports(DialectFeature feature) {
        return features.contains(Objects.requireNonNull(feature, "dialect feature must not be null"));
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }
}
