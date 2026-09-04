package com.flying.orm.rdb.metadata;

import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;

import java.util.EnumSet;
import java.util.Objects;

/**
 * JDBC 与 R2DBC 共用的版本化元数据查询契约。
 *
 * <p>查询 SQL 和 coverage 必须一起选择，避免一条执行路径换成兼容旧版本的 SQL，另一条路径却仍宣称
 * 可以完整观察全部结构事实。这个对象只在 reader 装配时创建，不进入普通 CRUD 热路径。</p>
 */
record MetadataQueryProfile(InformationSchemaFormMetadataReader.Queries queries,
                            SchemaSnapshotCoverage coverage) {

    MetadataQueryProfile {
        Objects.requireNonNull(queries, "metadata queries must not be null");
        Objects.requireNonNull(coverage, "metadata coverage must not be null");
    }

    static MetadataQueryProfile resolve(RdbDialect dialect) {
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        return switch (safeDialect.name()) {
            case "h2" -> complete(H2ReactiveFormMetadataReader.queries());
            case "mysql" -> complete(MySqlReactiveFormMetadataReader.queries());
            case "postgresql" -> complete(PostgreSqlReactiveFormMetadataReader.queries());
            case "oracle" -> oracle(safeDialect.version());
            case "sqlserver" -> sqlServer(safeDialect.version());
            default -> null;
        };
    }

    private static MetadataQueryProfile oracle(String version) {
        if ("12c".equals(version)) {
            EnumSet<SchemaSnapshotCoverage.Fact> observed =
                    EnumSet.allOf(SchemaSnapshotCoverage.Fact.class);
            observed.remove(SchemaSnapshotCoverage.Fact.COLUMN_DEFAULT);
            observed.remove(SchemaSnapshotCoverage.Fact.COLUMN_GENERATION);
            observed.remove(SchemaSnapshotCoverage.Fact.COLUMN_COLLATION);
            return new MetadataQueryProfile(
                    OracleReactiveFormMetadataReader.queries12c(),
                    SchemaSnapshotCoverage.of(observed));
        }
        if ("19c".equals(version) || "21c".equals(version) || "23ai".equals(version)) {
            return complete(OracleReactiveFormMetadataReader.queries());
        }
        throw unsupportedVersion("oracle", version);
    }

    private static MetadataQueryProfile sqlServer(String version) {
        if ("2012".equals(version) || "2016".equals(version)
                || "2019".equals(version) || "2022".equals(version)) {
            return complete(SqlServerReactiveFormMetadataReader.queries());
        }
        throw unsupportedVersion("sqlserver", version);
    }

    private static MetadataQueryProfile complete(InformationSchemaFormMetadataReader.Queries queries) {
        return new MetadataQueryProfile(queries, InformationSchemaFormMetadataReader.coverage(queries));
    }

    private static UnsupportedOperationException unsupportedVersion(String dialect, String version) {
        return new UnsupportedOperationException(
                "metadata reader is not implemented for the requested dialect version: "
                        + dialect + ' ' + version);
    }
}
