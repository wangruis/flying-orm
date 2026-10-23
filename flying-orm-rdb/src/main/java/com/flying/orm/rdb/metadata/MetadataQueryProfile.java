package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

/**
 * JDBC 与 R2DBC 共用的版本化元数据查询契约。
 *
 * <p>查询 SQL 和 coverage 必须一起选择，避免一条执行路径换成兼容旧版本的 SQL，另一条路径却仍宣称
 * 可以完整观察全部结构事实。这个对象只在 reader 装配时创建，不进入普通 CRUD 热路径。</p>
 */
record MetadataQueryProfile(Queries queries,
                            SchemaSnapshotCoverage coverage) {

    MetadataQueryProfile {
        Objects.requireNonNull(queries, "metadata queries must not be null");
        Objects.requireNonNull(coverage, "metadata coverage must not be null");
    }

    MetadataQueryProfile(Queries queries) {
        this(queries, coverage(queries));
    }

    private static SchemaSnapshotCoverage coverage(Queries queries) {
        Queries safeQueries = Objects.requireNonNull(queries, "metadata queries must not be null");
        if (safeQueries.completeSnapshotQueries()) {
            return SchemaSnapshotCoverage.complete();
        }
        EnumSet<SchemaSnapshotCoverage.Fact> observed = EnumSet.of(
                SchemaSnapshotCoverage.Fact.TABLE_EXISTENCE,
                SchemaSnapshotCoverage.Fact.COLUMNS,
                SchemaSnapshotCoverage.Fact.PRIMARY_KEY);
        if (safeQueries.indexQuery() != null) {
            observed.add(SchemaSnapshotCoverage.Fact.INDEXES);
        }
        if (safeQueries.foreignKeyQuery() != null) {
            observed.add(SchemaSnapshotCoverage.Fact.FOREIGN_KEYS);
        }
        return SchemaSnapshotCoverage.of(observed);
    }

    static MetadataQueryProfile resolve(RdbDialect dialect) {
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        return switch (safeDialect.name()) {
            case "h2" -> new MetadataQueryProfile(H2MetadataQueries.queries());
            case "mysql" -> new MetadataQueryProfile(MySqlMetadataQueries.queries());
            case "postgresql" -> new MetadataQueryProfile(PostgreSqlMetadataQueries.queries());
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
                    OracleMetadataQueries.queries12c(),
                    SchemaSnapshotCoverage.of(observed));
        }
        if ("19c".equals(version) || "21c".equals(version) || "23ai".equals(version)) {
            return new MetadataQueryProfile(OracleMetadataQueries.queries());
        }
        throw unsupportedVersion("oracle", version);
    }

    private static MetadataQueryProfile sqlServer(String version) {
        if ("2012".equals(version) || "2016".equals(version)
                || "2019".equals(version) || "2022".equals(version)) {
            return new MetadataQueryProfile(SqlServerMetadataQueries.queries());
        }
        throw unsupportedVersion("sqlserver", version);
    }

    private static UnsupportedOperationException unsupportedVersion(String dialect, String version) {
        return new UnsupportedOperationException(
                "metadata reader is not implemented for the requested dialect version: "
                        + dialect + ' ' + version);
    }

    static String requireText(String value, String fieldName) {
        return FormMetadataRowConverter.requireText(value, fieldName);
    }

    @FunctionalInterface
    interface Query {

        SqlRequest create(String schema, String table);
    }

    enum SnapshotDialect {
        PARTIAL,
        POSTGRESQL,
        MYSQL,
        H2,
        ORACLE,
        SQL_SERVER
    }

    record Queries(Query columnQuery,
                   Query indexQuery,
                   Query foreignKeyQuery,
                   Function<String, String> typeMapper,
                   Function<String, String> snapshotTypeMapper,
                   Query tableQuery,
                   Query primaryKeyQuery,
                   Query uniqueConstraintQuery,
                   Query checkConstraintQuery,
                   SnapshotDialect snapshotDialect) {

        Queries(Query columnQuery,
                Query indexQuery,
                Query foreignKeyQuery,
                Function<String, String> typeMapper) {
            this(columnQuery, indexQuery, foreignKeyQuery, typeMapper, typeMapper,
                 null, null, null, null, SnapshotDialect.PARTIAL);
        }

        Queries {
            Objects.requireNonNull(columnQuery, "column query must not be null");
            Objects.requireNonNull(typeMapper, "type mapper must not be null");
            Objects.requireNonNull(snapshotTypeMapper, "snapshot type mapper must not be null");
            Objects.requireNonNull(snapshotDialect, "snapshot dialect must not be null");
            if (snapshotDialect != SnapshotDialect.PARTIAL && !hasAllSnapshotQueries(
                    tableQuery, primaryKeyQuery, uniqueConstraintQuery, indexQuery,
                    foreignKeyQuery, checkConstraintQuery)) {
                throw new IllegalArgumentException(
                        "complete schema metadata requires every structural query");
            }
        }

        static Queries complete(Query columnQuery,
                                Query indexQuery,
                                Query foreignKeyQuery,
                                Function<String, String> typeMapper,
                                Query tableQuery,
                                Query primaryKeyQuery,
                                Query uniqueConstraintQuery,
                                Query checkConstraintQuery,
                                SnapshotDialect snapshotDialect) {
            return complete(columnQuery, indexQuery, foreignKeyQuery, typeMapper,
                            Queries::physicalType,
                            tableQuery, primaryKeyQuery, uniqueConstraintQuery,
                            checkConstraintQuery, snapshotDialect);
        }

        private static String physicalType(String type) {
            String declaration = requireText(type, "physical data type");
            if (declaration.indexOf('\'') >= 0 || declaration.indexOf('"') >= 0 || declaration.indexOf('`') >= 0) {
                return declaration;
            }
            return declaration.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
        }

        static Queries complete(Query columnQuery,
                                Query indexQuery,
                                Query foreignKeyQuery,
                                Function<String, String> typeMapper,
                                Function<String, String> snapshotTypeMapper,
                                Query tableQuery,
                                Query primaryKeyQuery,
                                Query uniqueConstraintQuery,
                                Query checkConstraintQuery,
                                SnapshotDialect snapshotDialect) {
            if (snapshotDialect == SnapshotDialect.PARTIAL) {
                throw new IllegalArgumentException("complete schema metadata requires a concrete dialect");
            }
            return new Queries(columnQuery, indexQuery, foreignKeyQuery, typeMapper, snapshotTypeMapper,
                               tableQuery, primaryKeyQuery, uniqueConstraintQuery,
                               checkConstraintQuery, snapshotDialect);
        }

        boolean completeSnapshotQueries() {
            return snapshotDialect != SnapshotDialect.PARTIAL;
        }

        /** 查询顺序与结果槽位由同一描述持有；部分描述中的 null 表示未提供该项查询。 */
        List<Query> snapshotQueries() {
            return completeSnapshotQueries()
                    ? List.of(columnQuery, tableQuery, primaryKeyQuery, uniqueConstraintQuery,
                            indexQuery, foreignKeyQuery, checkConstraintQuery)
                    : Arrays.asList(columnQuery, indexQuery, foreignKeyQuery);
        }

        SchemaSnapshot toSnapshot(RelationIdentity identity, List<List<DynamicRow>> rows) {
            return completeSnapshotQueries()
                    ? FormMetadataRowConverter.toCompleteSchemaSnapshot(identity,
                            rows.get(0), rows.get(1), rows.get(2), rows.get(3),
                            rows.get(4), rows.get(5), rows.get(6),
                            typeMapper, snapshotTypeMapper, snapshotDialect)
                    : FormMetadataRowConverter.toSchemaSnapshot(identity,
                            rows.get(0), rows.get(1), indexQuery != null,
                            rows.get(2), foreignKeyQuery != null, typeMapper);
        }

        /** 当前字典 SQL 只查询连接所在 catalog，不把显式 catalog 静默丢弃。 */
        RelationIdentity requireRelation(RelationIdentity relation) {
            RelationIdentity target = Objects.requireNonNull(relation, "metadata relation must not be null");
            if (target.catalog().isPresent()) {
                throw new UnsupportedOperationException("catalog-qualified metadata is not supported by this query profile");
            }
            return target;
        }

        SqlRequest tableNames(String schema) {
            String currentSchema = switch (snapshotDialect) {
                case POSTGRESQL, H2 -> "current_schema()";
                case MYSQL -> "database()";
                case ORACLE -> "sys_context('USERENV', 'CURRENT_SCHEMA')";
                case SQL_SERVER -> "schema_name()";
                case PARTIAL -> throw new UnsupportedOperationException(
                        "metadata reader does not support table name enumeration");
            };
            String sql = snapshotDialect == SnapshotDialect.ORACLE
                    ? "select TABLE_NAME from all_tab_comments where OWNER = "
                    : "select TABLE_NAME from information_schema.tables where TABLE_SCHEMA = ";
            return new SqlRequest(sql + (schema == null ? currentSchema : "?") + " order by TABLE_NAME",
                    schema == null ? List.of() : List.of(schema));
        }

        private static boolean hasAllSnapshotQueries(Query tableQuery,
                                                     Query primaryKeyQuery,
                                                     Query uniqueConstraintQuery,
                                                     Query indexQuery,
                                                     Query foreignKeyQuery,
                                                     Query checkConstraintQuery) {
            return tableQuery != null
                    && primaryKeyQuery != null
                    && uniqueConstraintQuery != null
                    && indexQuery != null
                    && foreignKeyQuery != null
                    && checkConstraintQuery != null;
        }
    }

}
