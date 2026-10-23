package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.UniqueNullPolicy;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.DatabaseType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * 编译结构演进的方言 SQL；使用原生替换或直接删建，不接管多条 DDL 的事务与并发协调。
 *
 * @author wangr
 * @version v4.0
 */
final class RelationalSchemaEvolutionSqlRenderer {

    private static final Map<String, Integer> POSTGRESQL_INTEGER_RANKS = Map.of(
            "SMALLINT", 0,
            "INTEGER", 1,
            "BIGINT", 2);
    private static final Set<String> POSTGRESQL_PARAMETER_WIDENING_TYPES = Set.of(
            "VARCHAR", "CHARACTER VARYING", "NUMERIC", "DECIMAL");

    private final SchemaDialect dialect;
    private final RelationalSchemaSqlRenderer renderer;

    RelationalSchemaEvolutionSqlRenderer(SchemaDialect dialect, RelationalSchemaSqlRenderer renderer) {
        this.dialect = dialect;
        this.renderer = renderer;
    }


    SqlRequest drop(SchemaOperation operation) {
        if (operation.kind() != SchemaOperation.Kind.DROP_INDEX && operation.kind() != SchemaOperation.Kind.DROP_COLUMN) {
            requireBuiltIn(operation.kind());
        }
        return switch (operation.kind()) {
            case DROP_UNIQUE -> {
                UniqueConstraintDefinition unique = (UniqueConstraintDefinition) operation.actual();
                requireObjectName(operation, unique.name());
                yield dropUnique(operation.relation(), unique);
            }
            case DROP_PRIMARY_KEY, DROP_CHECK, DROP_FOREIGN_KEY -> {
                String name = switch (operation.actual()) {
                    case PrimaryKeyDefinition value -> value.name();
                    case CheckConstraintDefinition value -> value.name();
                    case ForeignKeyDefinition value -> value.name();
                    default -> throw new IllegalArgumentException("operation does not drop a constraint");
                };
                requireObjectName(operation, name);
                yield request(dialect.dropConstraintSql(operation.relation(), name, operation.kind()));
            }
            case DROP_INDEX -> {
                IndexDefinition index = (IndexDefinition) operation.actual();
                requireObjectName(operation, index.name());
                yield request(dialect.dropIndexSql(operation.relation(), index.name()));
            }
            case DROP_COLUMN -> {
                ColumnDefinition column = (ColumnDefinition) operation.actual();
                requireObjectName(operation, column.name());
                if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
                    yield request(sqlServerDropColumnSql(dialect, dialect.relationIdentifier(operation.relation()), column));
                }
                yield request(dialect.dropColumnSql(operation.relation(), column.name()));
            }
            case DROP_TABLE -> {
                RelationalTableDefinition table = (RelationalTableDefinition) operation.actual();
                requireObjectName(operation, table.identity().table());
                if (!operation.relation().equals(table.identity())) {
                    throw new IllegalArgumentException(
                            "drop-table relation must match its actual table payload");
                }
                yield request(dialect.dropTableSql(table.identity()));
            }
            default -> throw new IllegalArgumentException("operation does not drop a schema object");
        };
    }

    /** 同一条 ALTER 删除已知默认约束和所属列。 */
    static String sqlServerDropColumnSql(SchemaDialect dialect, String renderedTable, ColumnDefinition column) {
        String prefix = "alter table " + renderedTable + " drop ";
        if (column.defaultConstraintName() != null) {
            return prefix + "constraint " + dialect.identifier(column.defaultConstraintName())
                    + ", column " + dialect.identifier(column.name());
        }
        if (column.defaultValue().kind() != ColumnDefault.Kind.NONE
                || column.generation().strategy() == ValueGeneration.Strategy.SEQUENCE) {
            throw RelationalSchemaSqlRenderer.unsupported(SchemaOperation.Kind.DROP_COLUMN);
        }
        return prefix + "column " + dialect.identifier(column.name());
    }

    List<SqlRequest> changeConstraint(SchemaOperation operation,
                                      String actualName,
                                      String desiredName,
                                      String desiredDefinition) {
        requireObjectName(operation, desiredName);
        requireBuiltIn(operation.kind());
        if (operation.kind() == SchemaOperation.Kind.CHANGE_PRIMARY_KEY
                && dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE) {
            PrimaryKeyDefinition actual = (PrimaryKeyDefinition) operation.actual();
            PrimaryKeyDefinition desired = (PrimaryKeyDefinition) operation.desired();
            if (!desired.columns().containsAll(actual.columns())) {
                // 键载荷不含退出主键列的目标可空性，不能在删除 Oracle 隐含非空后猜测其定义。
                throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
            }
            return List.of(request(dialect.dropConstraintSql(operation.relation(), actualName) + " drop index"),
                    renderer.addConstraint(operation.relation(), desiredDefinition + " enable validate"));
        }
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.POSTGRESQL
                || dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.MYSQL
                    && operation.kind() != SchemaOperation.Kind.CHANGE_FOREIGN_KEY) {
            return List.of(request(dialect.changeConstraintSql(
                    operation.relation(), actualName, desiredDefinition, operation.kind())));
        }
        SchemaOperation.Kind dropKind = switch (operation.kind()) {
            case CHANGE_PRIMARY_KEY -> SchemaOperation.Kind.DROP_PRIMARY_KEY;
            case CHANGE_UNIQUE -> SchemaOperation.Kind.DROP_UNIQUE;
            case CHANGE_FOREIGN_KEY -> SchemaOperation.Kind.DROP_FOREIGN_KEY;
            case CHANGE_CHECK -> SchemaOperation.Kind.DROP_CHECK;
            default -> throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
        };
        return List.of(request(dialect.dropConstraintSql(operation.relation(), actualName, dropKind)),
                renderer.addConstraint(operation.relation(), desiredDefinition
                        + (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE
                            ? " enable validate" : "")));
    }

    List<SqlRequest> changeUnique(SchemaOperation operation) {
        UniqueConstraintDefinition actual = (UniqueConstraintDefinition) operation.actual();
        UniqueConstraintDefinition desired = (UniqueConstraintDefinition) operation.desired();
        requireObjectName(operation, desired.name());
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            if (actual.nullPolicy() == UniqueNullPolicy.DISTINCT && desired.nullPolicy() == UniqueNullPolicy.DISTINCT
                    && actual.name().equals(desired.name())) {
                return List.of(request(renderer.createUnique(operation.relation(), desired).sql()
                        + " with (drop_existing = on)"));
            }
            return List.of(dropUnique(operation.relation(), actual),
                    renderer.createUnique(operation.relation(), desired));
        }
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE) {
            return changeOracleUnique(operation, actual, desired);
        }
        if (!SchemaDefinitionEquality.ordinaryUnique(actual, dialect)
                || !SchemaDefinitionEquality.ordinaryUnique(desired, dialect)) {
            throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
        }
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.H2) {
            return List.of(dropUnique(operation.relation(), actual),
                    renderer.createUnique(operation.relation(), desired));
        }
        return changeConstraint(operation, actual.name(), desired.name(), renderer.unique(desired));
    }

    private List<SqlRequest> changeOracleUnique(SchemaOperation operation,
                                                UniqueConstraintDefinition actual,
                                                UniqueConstraintDefinition desired) {
        RelationIdentity relation = operation.relation();
        if (actual.nullPolicy() == desired.nullPolicy()
                && actual.columns().equals(desired.columns()) && !actual.name().equals(desired.name())) {
            SqlRequest renameIndex = request(dialect.renameIndexSql(relation, actual.name(), desired.name()));
            return desired.nullPolicy() == UniqueNullPolicy.DEFAULT
                    ? List.of(request(dialect.renameConstraintSql(relation, actual.name(), desired.name())), renameIndex)
                    : List.of(renameIndex);
        }
        return List.of(dropUnique(relation, actual), renderer.createUnique(relation, desired));
    }

    private SqlRequest dropUnique(RelationIdentity relation, UniqueConstraintDefinition unique) {
        if (!SchemaDefinitionEquality.ordinaryUnique(unique, dialect)) {
            if (dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.SQL_SERVER
                    && dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.ORACLE) {
                throw RelationalSchemaSqlRenderer.unsupported(SchemaOperation.Kind.DROP_UNIQUE);
            }
            return request(dialect.dropIndexSql(relation, unique.name()));
        }
        String drop = dialect.dropConstraintSql(relation, unique.name(), SchemaOperation.Kind.DROP_UNIQUE);
        return request(dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE
                ? drop + " drop index" : drop);
    }

    List<SqlRequest> changeIndex(SchemaOperation operation) {
        IndexDefinition actual = (IndexDefinition) operation.actual();
        IndexDefinition desired = (IndexDefinition) operation.desired();
        requireObjectName(operation, desired.name());
        if ((actual.unique() || desired.unique())
                && dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.NONE) {
            if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.MYSQL) {
                StringJoiner keys = new StringJoiner(", ");
                desired.keys().forEach(key -> keys.add(dialect.identifier(key.column()) + ' '
                        + key.direction().name().toLowerCase(java.util.Locale.ROOT)));
                return List.of(request("alter table " + dialect.relationIdentifier(operation.relation())
                        + " drop index " + dialect.identifier(actual.name()) + ", add "
                        + (desired.unique() ? "unique " : "") + "index " + dialect.identifier(desired.name())
                        + " (" + keys + ')'));
            }
            if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
                if (!actual.name().equals(desired.name())) {
                    return rebuildIndex(operation.relation(), actual, desired);
                }
                return List.of(request(renderer.createIndex(operation.relation(), desired).sql()
                        + " with (drop_existing = on)"));
            }
            if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE
                    && actual.unique() == desired.unique()
                    && actual.keys().equals(desired.keys()) && !actual.name().equals(desired.name())) {
                return List.of(request(dialect.renameIndexSql(operation.relation(), actual.name(), desired.name())));
            }
        }
        return rebuildIndex(operation.relation(), actual, desired);
    }

    private List<SqlRequest> rebuildIndex(RelationIdentity relation, IndexDefinition actual, IndexDefinition desired) {
        return List.of(request(dialect.dropIndexSql(relation, actual.name())), renderer.createIndex(relation, desired));
    }

    List<SqlRequest> changeColumn(SchemaOperation operation, String actualType,
                                DatabaseType logicalActualType, boolean explicitTypeChange) {
        if (!explicitTypeChange) requireBuiltIn(operation.kind());
        ColumnDefinition actual = (ColumnDefinition) operation.actual();
        ColumnDefinition desired = (ColumnDefinition) operation.desired();
        requireObjectName(operation, desired.name());

        if (!explicitTypeChange && SchemaColumnCommentCodec.logicalTypeChangeRequiresReview(
                dialect, actual, logicalActualType, desired.databaseType())) {
            throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
        }
        String actualStorageComment = logicalActualType == null ? renderer.storageComment(actual)
                : SchemaColumnCommentCodec.encode(dialect, logicalActualType, actual.generation(), actual.comment());
        String desiredStorageComment = renderer.storageComment(desired);
        String desiredType = SchemaTypeMapping.preserveCharacterLengthUnit(dialect, actualType,
                SchemaDefinitionEquality.desiredColumnType(dialect, desired));
        boolean typeChanged = !dialect.sameDataType(actualType, desiredType);
        boolean nullableChanged = actual.nullable() != desired.nullable();
        boolean defaultChanged = !SchemaDefinitionEquality.sameDefault(
                actual.defaultValue(), desired.defaultValue(), dialect)
                || desired.defaultConstraintName() != null
                    && !desired.defaultConstraintName().equals(actual.defaultConstraintName());
        boolean commentChanged = !Objects.equals(actualStorageComment, desiredStorageComment);
        boolean generationChanged = !SchemaDefinitionEquality.sameGeneration(
                actual.generation(), desired.generation(), dialect, operation.relation());
        boolean charsetChanged = desired.charset() != null && !desired.charset().equals(actual.charset());
        boolean collationChanged = desired.collation() != null && !desired.collation().equals(actual.collation());
        String charset = desired.charset() == null ? actual.charset() : desired.charset();
        String collation = desired.collation() == null ? actual.collation() : desired.collation();
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER
                && !SchemaDialectTypeSupport.sqlServerCharacterType(desiredType)) {
            collation = null;
        }
        if (generationChanged || charsetChanged || collationChanged) {
            throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
        }
        if (!typeChanged && !nullableChanged && !defaultChanged && !commentChanged) {
            return List.of();
        }
        if (typeChanged && !explicitTypeChange
                && (actual.generation().strategy() != ValueGeneration.Strategy.NONE
                    || desired.generation().strategy() != ValueGeneration.Strategy.NONE
                    || !safeWidening(actualType, desiredType))) {
            throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
        }
        String definition = renderer.columnDefinition(desired, charset, collation);
        // 完整列重写包含可空性、默认值及内联注释；独立注释仍交给方言生成。
        boolean fullRewrite = dialect.rewritesFullColumnDefinition()
                && (typeChanged || nullableChanged || commentChanged && dialect.inlineColumnComment());
        List<SqlRequest> requests = new ArrayList<>();
        if (typeChanged || fullRewrite) {
            requests.add(request(dialect.alterColumnTypeSql(
                    operation.relation(), desired.name(), desiredType,
                    definition, desired.nullable(), collation)));
        }
        if (nullableChanged && !fullRewrite) {
            SqlRequest nullabilityChange = request(dialect.alterColumnNullabilitySql(
                    operation.relation(), desired.name(), desiredType,
                    definition, desired.nullable(), collation));
            if (requests.isEmpty() || !requests.getLast().sql().equals(nullabilityChange.sql())) {
                requests.add(nullabilityChange);
            }
        }
        if (defaultChanged && !fullRewrite) {
            requests.addAll(changeDefault(operation));
        }
        if (commentChanged && (!fullRewrite || !dialect.inlineColumnComment())) {
            requests.add(request(dialect.columnCommentChangeSql(
                    operation.relation(), desired.name(), actualStorageComment, desiredStorageComment)
                    .orElseThrow(() -> RelationalSchemaSqlRenderer.unsupported(operation.kind()))));
        }
        return requests;
    }

    private List<SqlRequest> changeDefault(SchemaOperation operation) {
        ColumnDefinition actual = (ColumnDefinition) operation.actual();
        ColumnDefinition desired = (ColumnDefinition) operation.desired();
        if (dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            return List.of(request(dialect.alterColumnDefaultSql(
                    operation.relation(), desired.name(), renderer.defaultExpression(desired))));
        }
        // 序列 DEFAULT 属于 generation，不能按普通默认值删除；约束名只取实际元数据。
        String actualName = actual.defaultConstraintName();
        if (desired.generation().strategy() != ValueGeneration.Strategy.NONE
                || actualName == null && renderer.defaultExpression(actual) != null) {
            throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
        }
        List<SqlRequest> requests = new ArrayList<>();
        if (actualName != null) {
            requests.add(request(dialect.dropConstraintSql(operation.relation(), actualName)));
        }
        String expression = renderer.defaultExpression(desired);
        if (expression != null) {
            String name = desired.defaultConstraintName() == null ? actualName : desired.defaultConstraintName();
            String definition = (name == null ? "" : "constraint " + dialect.identifier(name) + ' ')
                    + "default " + expression + " for " + dialect.identifier(desired.name());
            requests.add(renderer.addConstraint(operation.relation(), definition));
        }
        return requests;
    }

    private boolean safeWidening(String actual, String desired) {
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.POSTGRESQL) {
            return safePostgreSqlWidening(actual, desired);
        }
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.H2) {
            actual = SchemaTypeComparison.h2Comparable(actual);
            desired = SchemaTypeComparison.h2Comparable(desired);
        }
        DatabaseType actualType = DatabaseType.of(actual).requireSafe("current data type");
        DatabaseType desiredType = DatabaseType.of(desired).requireSafe("target data type");
        Set<String> types = switch (dialect.generatedValueStyle()) {
            case MYSQL -> Set.of("VARCHAR");
            case H2 -> Set.of("VARCHAR", "CHARACTER VARYING");
            case ORACLE -> Set.of("VARCHAR2", "NVARCHAR2");
            case SQL_SERVER -> Set.of("VARCHAR", "NVARCHAR");
            default -> Set.of();
        };
        return !actualType.isArray() && !desiredType.isArray()
                && types.contains(actualType.baseName()) && types.contains(desiredType.baseName())
                && SchemaDialectTypeSupport.safeWideningDataType(actual, desired);
    }

    private boolean safePostgreSqlWidening(String actual, String desired) {
        String comparableActual = SchemaTypeComparison.postgresqlComparable(actual);
        String comparableDesired = SchemaTypeComparison.postgresqlComparable(desired);
        DatabaseType actualType = DatabaseType.of(comparableActual).requireSafe("current data type");
        DatabaseType desiredType = DatabaseType.of(comparableDesired).requireSafe("target data type");
        if (actualType.isArray() || desiredType.isArray()) {
            return false;
        }
        Integer actualInteger = POSTGRESQL_INTEGER_RANKS.get(actualType.baseName());
        Integer desiredInteger = POSTGRESQL_INTEGER_RANKS.get(desiredType.baseName());
        if (actualInteger != null || desiredInteger != null) {
            return actualInteger != null && desiredInteger != null
                    && actualType.arguments().isEmpty() && desiredType.arguments().isEmpty()
                    && actualInteger < desiredInteger;
        }
        return POSTGRESQL_PARAMETER_WIDENING_TYPES.contains(actualType.baseName())
                && POSTGRESQL_PARAMETER_WIDENING_TYPES.contains(desiredType.baseName())
                && SchemaDialectTypeSupport.safeWideningDataType(comparableActual, comparableDesired);
    }

    private void requireBuiltIn(SchemaOperation.Kind kind) {
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.NONE) {
            throw RelationalSchemaSqlRenderer.unsupported(kind);
        }
    }

    private void requireObjectName(SchemaOperation operation, String payloadName) {
        if (!operation.objectName().equals(payloadName)) {
            throw new IllegalArgumentException(
                    "schema operation object name must match its frozen payload");
        }
    }

    private SqlRequest request(String sql) {
        return new SqlRequest(sql, List.of());
    }
}
