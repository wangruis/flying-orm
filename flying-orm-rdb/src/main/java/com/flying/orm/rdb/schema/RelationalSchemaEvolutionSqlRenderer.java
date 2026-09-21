package com.flying.orm.rdb.schema;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.UniqueNullPolicy;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.DatabaseType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * 仅编译已证明安全的内置方言演进；候选键替换仍必须保持连续的约束保护。
 *
 * @author wangr
 * @version v4.0
 */
final class RelationalSchemaEvolutionSqlRenderer {

    private static final StableDigest.Domain GUARD_NAMES = StableDigest.domain("relational-schema-guard/v1");
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
        if (operation.kind() != SchemaOperation.Kind.DROP_INDEX) {
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
                    if (column.defaultConstraintName() != null) {
                        // 同一条 ALTER 删除已回读的默认约束和所属列，不在执行中临时查询或猜名称。
                        yield request(dialect.dropConstraintSql(operation.relation(), column.defaultConstraintName())
                                + ", column " + dialect.identifier(column.name()));
                    }
                    if (column.defaultValue().kind() != ColumnDefault.Kind.NONE
                            || column.generation().strategy() == ValueGeneration.Strategy.SEQUENCE) {
                        throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
                    }
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

    List<SqlRequest> changeConstraint(SchemaOperation operation,
                                      String actualName,
                                      String desiredName,
                                      String desiredDefinition) {
        requireObjectName(operation, desiredName);
        if (operation.kind() == SchemaOperation.Kind.CHANGE_PRIMARY_KEY
                && dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE) {
            return expandOraclePrimaryKey(operation, desiredDefinition);
        }
        if (operation.kind() == SchemaOperation.Kind.CHANGE_FOREIGN_KEY
                && (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE
                    || dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.MYSQL)) {
            // Oracle 不能原子替换；MySQL 同名 DROP/ADD 不能合并。两者统一要求精确的静止写入批准。
            // 新约束必须验证既有数据；失败保留真实部分完成状态，不自动恢复或重试。
            return List.of(request(dialect.dropConstraintSql(
                    operation.relation(), actualName, SchemaOperation.Kind.DROP_FOREIGN_KEY)),
                    renderer.addConstraint(operation.relation(), desiredDefinition
                            + (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE
                                ? " enable validate" : "")));
        }
        if (operation.kind() == SchemaOperation.Kind.CHANGE_FOREIGN_KEY
                && (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.H2
                    || dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER)) {
            ForeignKeyDefinition desired = (ForeignKeyDefinition) operation.desired();
            ForeignKeyDefinition.Builder guard = ForeignKeyDefinition.builder(
                    guardName(operation, desired.columns(), UniqueNullPolicy.DEFAULT)).reference(desired.reference());
            desired.columns().forEach(guard::addColumn);
            desired.referenceColumns().forEach(guard::addReferenceColumn);
            ForeignKeyDefinition protection = guard.build();
            // 临时外键只保护引用完整性，不复制级联动作，避免 SQL Server 的重复级联路径。
            // 后续步骤失败时保留保护约束，执行报告如实记录部分完成，不擅自清理或补偿。
            return List.of(renderer.addConstraint(operation.relation(), renderer.foreignKey(protection)),
                    request(dialect.dropConstraintSql(operation.relation(), actualName)),
                    renderer.addConstraint(operation.relation(), desiredDefinition),
                    request(dialect.dropConstraintSql(operation.relation(), protection.name())));
        }
        if ((dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.H2
                || dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER)
                && operation.kind() == SchemaOperation.Kind.CHANGE_PRIMARY_KEY) {
            PrimaryKeyDefinition desired = (PrimaryKeyDefinition) operation.desired();
            UniqueConstraintDefinition guard = new UniqueConstraintDefinition(
                    guardName(operation, desired.columns(), UniqueNullPolicy.DEFAULT), desired.columns());
            return List.of(renderer.createUnique(operation.relation(), guard),
                    request(dialect.dropConstraintSql(operation.relation(), actualName)),
                    renderer.addConstraint(operation.relation(), desiredDefinition),
                    dropUnique(operation.relation(), guard));
        }
        if (dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.POSTGRESQL
                && dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.MYSQL) {
            throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
        }
        return List.of(request(dialect.changeConstraintSql(
                operation.relation(), actualName, desiredDefinition, operation.kind())));
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
            UniqueConstraintDefinition guard = new UniqueConstraintDefinition(
                    guardName(operation, desired.columns(), desired.nullPolicy()),
                    desired.columns(), desired.nullPolicy());
            return List.of(renderer.createUnique(operation.relation(), guard),
                    dropUnique(operation.relation(), actual),
                    renderer.createUnique(operation.relation(), desired),
                    dropUnique(operation.relation(), guard));
        }
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE) {
            return changeOracleUnique(operation, actual, desired);
        }
        if (!SchemaDefinitionEquality.ordinaryUnique(actual, dialect)
                || !SchemaDefinitionEquality.ordinaryUnique(desired, dialect)) {
            throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
        }
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.H2) {
            UniqueConstraintDefinition guard = new UniqueConstraintDefinition(
                    guardName(operation, desired.columns(), desired.nullPolicy()), desired.columns(), desired.nullPolicy());
            return List.of(renderer.createUnique(operation.relation(), guard),
                    dropUnique(operation.relation(), actual),
                    request(dialect.renameConstraintSql(operation.relation(), guard.name(), desired.name())));
        }
        return changeConstraint(operation, actual.name(), desired.name(), renderer.unique(desired));
    }

    private List<SqlRequest> expandOraclePrimaryKey(SchemaOperation operation, String desiredDefinition) {
        PrimaryKeyDefinition actual = (PrimaryKeyDefinition) operation.actual();
        PrimaryKeyDefinition desired = (PrimaryKeyDefinition) operation.desired();
        if (desired.columns().size() <= actual.columns().size() || !desired.columns().containsAll(actual.columns())) {
            // 旧主键列退出目标键后，Oracle 可能同时撤销它的隐含非空；此处只接纳保留旧键列的扩展。
            throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
        }
        RelationIdentity relation = operation.relation();
        String indexName = guardName(operation, desired.columns(), UniqueNullPolicy.DEFAULT);
        String notNullName = "n" + indexName; // 两种保护对象名字不同，均不超过三十个字符。
        IndexDefinition.Builder index = IndexDefinition.builder(indexName).unique();
        desired.columns().forEach(column -> index.addKey(IndexKeyPart.asc(column)));
        CheckConstraintDefinition nonNull = CheckConstraintDefinition.of(notNullName,
                CheckPredicate.and(desired.columns().stream().map(CheckPredicate::isNotNull)
                        .toArray(CheckPredicate[]::new)));
        // 唯一索引不拒绝 NULL，必须先同时建立非空保护，再删除可能拥有隐含 NOT NULL 的旧主键。
        // 新主键显式接管该索引并验证成功后才能删除 CHECK；中途失败仍按已冻结步骤报告部分完成。
        return List.of(renderer.createIndex(relation, index.build()),
                renderer.addConstraint(relation, renderer.check(nonNull) + " enable validate"),
                request(dialect.dropConstraintSql(relation, actual.name()) + " drop index"),
                request(dialect.renameIndexSql(relation, indexName, desired.name())),
                renderer.addConstraint(relation, desiredDefinition + " using index "
                        + dialect.schemaObjectIdentifier(relation, desired.name()) + " enable validate"),
                request(dialect.dropConstraintSql(relation, notNullName)));
    }

    private List<SqlRequest> changeOracleUnique(SchemaOperation operation,
                                                UniqueConstraintDefinition actual,
                                                UniqueConstraintDefinition desired) {
        RelationIdentity relation = operation.relation();
        if (actual.nullPolicy() == desired.nullPolicy()
                && sameColumnSet(actual.columns(), desired.columns())) {
            if (!actual.columns().equals(desired.columns()) || actual.name().equals(desired.name())) {
                // Oracle 不允许同列、同唯一性的重复索引；排序变化不能试建 guard 后再碰运气。
                throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
            }
            SqlRequest renameIndex = request(dialect.renameIndexSql(relation, actual.name(), desired.name()));
            return desired.nullPolicy() == UniqueNullPolicy.DEFAULT
                    ? List.of(request(dialect.renameConstraintSql(relation, actual.name(), desired.name())), renameIndex)
                    : List.of(renameIndex);
        }
        UniqueConstraintDefinition guard = new UniqueConstraintDefinition(
                guardName(operation, desired.columns(), desired.nullPolicy()), desired.columns(), desired.nullPolicy());
        SqlRequest createGuard;
        if (desired.nullPolicy() == UniqueNullPolicy.DEFAULT) {
            // 显式固定支撑索引的列顺序与名称，不能让 Oracle 自动复用其它索引后破坏元数据回读合同。
            IndexDefinition.Builder index = IndexDefinition.builder(guard.name()).unique();
            desired.columns().forEach(column -> index.addKey(IndexKeyPart.asc(column)));
            createGuard = renderer.createIndex(relation, index.build());
        } else {
            createGuard = renderer.createUnique(relation, guard);
        }
        SqlRequest dropActual = dropUnique(relation, actual);
        SqlRequest renameGuard = request(dialect.renameIndexSql(relation, guard.name(), desired.name()));
        return desired.nullPolicy() == UniqueNullPolicy.DEFAULT
                ? List.of(createGuard, dropActual, renameGuard,
                        renderer.addConstraint(relation, renderer.unique(desired)
                                + " using index " + dialect.schemaObjectIdentifier(relation, desired.name())))
                : List.of(createGuard, dropActual, renameGuard);
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

    private String guardName(SchemaOperation operation, List<String> columns, UniqueNullPolicy policy) {
        RelationIdentity relation = operation.relation();
        StableEncoder encoded = StableDigest.sha256(GUARD_NAMES)
                .text("KIND", operation.kind().name())
                .nullableText("CATALOG", relation.catalog().orElse(null))
                .nullableText("SCHEMA", relation.schema().orElse(null))
                .text("TABLE", relation.table()).text("OBJECT", operation.objectName())
                .text("NULL_POLICY", policy.name()).integer("COLUMN_COUNT", columns.size());
        columns.forEach(column -> encoded.text("COLUMN", column));
        if (operation.desired() instanceof IndexDefinition index) {
            encoded.integer("UNIQUE", index.unique() ? 1 : 0);
            index.keys().forEach(key -> encoded.text("DIRECTION", key.direction().name()));
        } else if (operation.desired() instanceof ForeignKeyDefinition foreignKey) {
            RelationIdentity reference = foreignKey.reference();
            encoded.nullableText("REFERENCE_CATALOG", reference.catalog().orElse(null))
                    .nullableText("REFERENCE_SCHEMA", reference.schema().orElse(null))
                    .text("REFERENCE_TABLE", reference.table());
            foreignKey.referenceColumns().forEach(column -> encoded.text("REFERENCE_COLUMN", column));
        }
        return "fo_guard_" + encoded.finishHex().substring(0, 20);
    }

    List<SqlRequest> changeIndex(SchemaOperation operation) {
        requireBuiltIn(operation.kind());
        IndexDefinition actual = (IndexDefinition) operation.actual();
        IndexDefinition desired = (IndexDefinition) operation.desired();
        requireObjectName(operation, desired.name());
        if (actual.unique() || desired.unique()) {
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
                    throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
                }
                return List.of(request(renderer.createIndex(operation.relation(), desired).sql()
                        + " with (drop_existing = on)"));
            }
            List<String> desiredColumns = desired.keys().stream().map(IndexKeyPart::column).toList();
            if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE
                    && actual.unique() == desired.unique()
                    && sameColumnSet(actual.keys().stream().map(IndexKeyPart::column).toList(), desiredColumns)) {
                if (!actual.keys().equals(desired.keys()) || actual.name().equals(desired.name())) {
                    throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
                }
                return List.of(request(dialect.renameIndexSql(operation.relation(), actual.name(), desired.name())));
            }
            IndexDefinition.Builder guard = IndexDefinition.builder(
                    guardName(operation, desiredColumns, UniqueNullPolicy.DEFAULT)).unique(desired.unique());
            desired.keys().forEach(guard::addKey);
            IndexDefinition targetGuard = guard.build();
            return List.of(renderer.createIndex(operation.relation(), targetGuard),
                    request(dialect.dropIndexSql(operation.relation(), actual.name())),
                    request(dialect.renameIndexSql(operation.relation(), targetGuard.name(), desired.name())));
        }
        return List.of(
                request(dialect.dropIndexSql(operation.relation(), actual.name())),
                renderer.createIndex(operation.relation(), desired));
    }

    private static boolean sameColumnSet(List<String> actual, List<String> desired) {
        return actual.size() == desired.size() && actual.containsAll(desired);
    }

    List<SqlRequest> changeColumn(SchemaOperation operation, boolean physicalActualTypes,
                                DatabaseType observedActualType) {
        requireBuiltIn(operation.kind());
        ColumnDefinition actual = (ColumnDefinition) operation.actual();
        ColumnDefinition desired = (ColumnDefinition) operation.desired();
        requireObjectName(operation, desired.name());

        DatabaseType logicalActualType = physicalActualTypes ? observedActualType : actual.databaseType();
        if (SchemaColumnCommentCodec.logicalTypeChangeRequiresReview(
                dialect, actual, logicalActualType, desired.databaseType())) {
            throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
        }
        String actualStorageComment = logicalActualType == null ? renderer.storageComment(actual)
                : SchemaColumnCommentCodec.encode(dialect, logicalActualType, actual.generation(), actual.comment());
        String desiredStorageComment = renderer.storageComment(desired);
        String actualType = SchemaDefinitionEquality.actualColumnDdlType(dialect, actual, physicalActualTypes);
        String desiredType = SchemaDefinitionEquality.desiredColumnType(dialect, desired);
        boolean typeChanged = !SchemaDefinitionEquality.sameColumnType(
                actual, desired, dialect, physicalActualTypes);
        boolean nullableChanged = actual.nullable() != desired.nullable();
        boolean defaultChanged = !SchemaDefinitionEquality.sameDefault(
                actual.defaultValue(), desired.defaultValue())
                || desired.defaultConstraintName() != null
                    && !desired.defaultConstraintName().equals(actual.defaultConstraintName());
        boolean commentChanged = !Objects.equals(actualStorageComment, desiredStorageComment);
        boolean generationChanged = !SchemaDefinitionEquality.sameGeneration(
                actual.generation(), desired.generation(), dialect, operation.relation());
        boolean charsetChanged = desired.charset() != null && !desired.charset().equals(actual.charset());
        boolean collationChanged = desired.collation() != null && !desired.collation().equals(actual.collation());
        String charset = desired.charset() == null ? actual.charset() : desired.charset();
        String collation = desired.collation() == null ? actual.collation() : desired.collation();
        int changes = (typeChanged ? 1 : 0) + (nullableChanged ? 1 : 0)
                + (defaultChanged ? 1 : 0) + (commentChanged ? 1 : 0)
                + (generationChanged ? 1 : 0) + (charsetChanged ? 1 : 0)
                + (collationChanged ? 1 : 0);
        if (changes != 1 || generationChanged || charsetChanged || collationChanged) {
            throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
        }
        if (typeChanged) {
            if (actual.generation().strategy() != ValueGeneration.Strategy.NONE
                    || desired.generation().strategy() != ValueGeneration.Strategy.NONE
                    || !safeWidening(actualType, desiredType)) {
                throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
            }
            return List.of(request(dialect.alterColumnTypeSql(
                    operation.relation(), desired.name(), desiredType,
                    renderer.columnDefinition(desired, charset, collation), desired.nullable(), collation)));
        }
        if (nullableChanged) {
            return List.of(request(dialect.alterColumnNullabilitySql(
                    operation.relation(), desired.name(), desiredType,
                    renderer.columnDefinition(desired, charset, collation), desired.nullable(), collation)));
        }
        if (defaultChanged) {
            if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
                // 序列默认值由 generation 表达；不能把“没有普通默认表达式”误当成删除序列默认值。
                if (desired.generation().strategy() != ValueGeneration.Strategy.NONE) {
                    throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
                }
                if (renderer.defaultExpression(desired) == null && actual.defaultConstraintName() != null) {
                    return List.of(request(dialect.dropConstraintSql(
                            operation.relation(), actual.defaultConstraintName())));
                }
                if (renderer.defaultExpression(actual) == null && actual.defaultConstraintName() == null) {
                    String definition = (desired.defaultConstraintName() == null ? "" : "constraint "
                            + dialect.identifier(desired.defaultConstraintName()) + ' ')
                            + "default " + renderer.defaultExpression(desired)
                            + " for " + dialect.identifier(desired.name());
                    return List.of(renderer.addConstraint(operation.relation(), definition));
                }
                throw RelationalSchemaSqlRenderer.unsupported(operation.kind());
            }
            return List.of(request(dialect.alterColumnDefaultSql(
                    operation.relation(), desired.name(), renderer.defaultExpression(desired))));
        }
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.MYSQL) {
            return List.of(request(dialect.alterColumnTypeSql(operation.relation(), desired.name(), desiredType,
                    renderer.columnDefinition(desired, charset, collation), desired.nullable(), collation)));
        }
        return List.of(request(dialect.columnCommentChangeSql(
                operation.relation(), desired.name(), actualStorageComment, desiredStorageComment)
                .orElseThrow(() -> RelationalSchemaSqlRenderer.unsupported(operation.kind()))));
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
