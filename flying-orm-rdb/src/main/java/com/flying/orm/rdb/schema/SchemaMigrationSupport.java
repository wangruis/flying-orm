package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 迁移比较中反复使用的纯判断方法。
 *
 * <p>这些方法不创建数据库连接，也不持有渲染器状态。单独放出来是为了让迁移编排代码只表达
 * “先比较什么、再生成什么”，而不是被大小写和改名细节淹没。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class SchemaMigrationSupport {

    private SchemaMigrationSupport() {
    }

    static boolean sameColumnShape(ColumnMetadata column,
                                   DynamicField target,
                                   SchemaTableSqlRenderer tables) {
        return sameDataType(column, target, tables)
                && column.primaryKey() == target.primaryKey()
                && column.nullable() == target.nullable()
                && Objects.equals(column.generation(), target.generation());
    }

    /**
     * 判断一次类型参数变化是否只会扩大可存储范围。
     *
     * <p>这里只自动放行同一种数据库类型的长度或精度扩宽，不猜测 {@code INT -> BIGINT} 之类的跨类型转换。
     * DECIMAL 除了总精度不能缩小，小数位和整数位容量也都不能减少；任何元数据缺失都会保守返回 false。</p>
     */
    static boolean safeWidening(ColumnMetadata current,
                                DynamicField target,
                                SchemaTableSqlRenderer tables) {
        if (current.primaryKey() != target.primaryKey()
                || current.nullable() != target.nullable()
                || !Objects.equals(current.generation(), target.generation())) {
            return false;
        }
        return SchemaDialectTypeSupport.safeWideningDataType(tables.dataType(current),
                                                              tables.dataType(target));
    }

    private static boolean sameDataType(ColumnMetadata column,
                                        DynamicField target,
                                        SchemaTableSqlRenderer tables) {
        return SchemaDialectTypeSupport.sameDataType(tables.dataType(column), tables.dataType(target));
    }

    static List<String> currentPrimaryKeys(TableMetadata current) {
        return current.columns().stream()
                      .filter(ColumnMetadata::primaryKey)
                      .map(ColumnMetadata::name)
                      .toList();
    }

    static List<String> targetPrimaryKeys(DynamicForm target) {
        return target.fields().stream()
                     .filter(DynamicField::primaryKey)
                     .map(DynamicField::name)
                     .toList();
    }

    static void validateColumnRenames(TableMetadata current,
                                      DynamicForm target,
                                      Map<String, String> renames) {
        renames.forEach((oldName, newName) -> {
            if (current.findColumn(oldName).isEmpty()) {
                throw new IllegalArgumentException("rename source column does not exist");
            }
            if (target.findField(newName).isEmpty()) {
                throw new IllegalArgumentException("rename target field does not exist");
            }
            if (current.findColumn(newName).isPresent()) {
                throw new IllegalArgumentException("rename target column already exists");
            }
            if (target.findField(oldName).isPresent()) {
                throw new IllegalArgumentException("rename source is still present in target form");
            }
        });
    }

    static List<String> renamedNames(List<String> names, Map<String, String> renames) {
        return names.stream()
                    .map(name -> {
                        String renamed = renameTargetForSource(renames, name);
                        return renamed == null ? name : renamed;
                    })
                    .toList();
    }

    static String renameSourceForTarget(Map<String, String> renames, String targetName) {
        return renames.entrySet().stream()
                      .filter(entry -> entry.getValue().equalsIgnoreCase(targetName))
                      .map(Map.Entry::getKey)
                      .findFirst()
                      .orElse(null);
    }

    static String renameTargetForSource(Map<String, String> renames, String sourceName) {
        return renames.entrySet().stream()
                      .filter(entry -> entry.getKey().equalsIgnoreCase(sourceName))
                      .map(Map.Entry::getValue)
                      .findFirst()
                      .orElse(null);
    }

    static boolean sameNamesIgnoreCase(List<String> left, List<String> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (!left.get(i).equalsIgnoreCase(right.get(i))) {
                return false;
            }
        }
        return true;
    }

    static SkippedSchemaChange primaryKeyChange(String table,
                                                List<String> oldPrimaryKeys,
                                                List<String> newPrimaryKeys,
                                                SchemaMigrationOptions options) {
        return new SkippedSchemaChange(SkippedSchemaChange.Kind.CHANGE_PRIMARY_KEY,
                                       table,
                                       options.primaryKeyChangeAllowed()
                                               ? "primary key change is manual-only and is never executed automatically"
                                               : "SAFE mode does not change primary key columns",
                                       Map.of("table", table,
                                              "oldPrimaryKeys", List.copyOf(oldPrimaryKeys),
                                              "newPrimaryKeys", List.copyOf(newPrimaryKeys),
                                              "requestedExecution", options.primaryKeyChangeAllowed(),
                                              "executionMode", "MANUAL_ONLY",
                                              "requiresApprovalFingerprint", true),
                                       List.of("review data that depends on the old primary key",
                                               "write a checked migration script for the primary key change",
                                               "run it from the upper layer in a controlled transaction or maintenance window",
                                               "read table metadata again after the migration"));
    }

    static boolean sameIndexColumns(IndexMetadata current,
                                    IndexMetadata target,
                                    Map<String, String> columnRenames) {
        if (current.columns().size() != target.columns().size()) {
            return false;
        }
        for (int i = 0; i < current.columns().size(); i++) {
            String currentName = current.columns().get(i);
            String renamed = renameTargetForSource(columnRenames, currentName);
            if (!(renamed == null ? currentName : renamed).equalsIgnoreCase(target.columns().get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 在自动唯一索引因共同方言长度边界缩短时，识别唯一可证明为同一约束的历史名称。
     *
     * <p>只允许由 DynamicForm 元数据重新推导的单列 unique target 进入兼容；候选既不能是其他
     * target 的精确名称，也不能已被前一个 target 消费。多个同形候选一律返回空，调用方继续严格
     * 按名称规划，避免把显式索引或歧义历史索引误判为同一对象。</p>
     */
    static IndexMetadata findLegacyGeneratedUniqueIndex(List<IndexMetadata> currentIndexes,
                                                         List<IndexMetadata> generatedIndexes,
                                                         IndexMetadata targetIndex,
                                                         Set<String> targetIndexNames,
                                                         Set<String> consumedCurrentIndexNames) {
        if (!isGeneratedSingleColumnUniqueIndex(generatedIndexes, targetIndex)) {
            return null;
        }
        List<IndexMetadata> candidates = currentIndexes.stream()
                                                        .filter(IndexMetadata::unique)
                                                        .filter(index -> index.columns().size() == 1)
                                                        .filter(index -> !targetIndexNames.contains(
                                                                index.normalizedName()))
                                                        .filter(index -> !consumedCurrentIndexNames.contains(
                                                                index.normalizedName()))
                                                        .filter(index -> sameNormalizedSingleColumn(index,
                                                                                                    targetIndex))
                                                        .toList();
        return candidates.size() == 1 ? candidates.getFirst() : null;
    }

    private static boolean isGeneratedSingleColumnUniqueIndex(List<IndexMetadata> generatedIndexes,
                                                                IndexMetadata targetIndex) {
        return targetIndex.unique()
                && targetIndex.columns().size() == 1
                && generatedIndexes.stream().anyMatch(generatedIndex -> generatedIndex.unique()
                        && generatedIndex.columns().size() == 1
                        && generatedIndex.normalizedName().equals(targetIndex.normalizedName())
                        && sameNormalizedSingleColumn(generatedIndex, targetIndex));
    }

    /** 自动索引兼容只接受未改名的同一规范化单列，字段改名仍由既有严格迁移规则处理。 */
    private static boolean sameNormalizedSingleColumn(IndexMetadata left, IndexMetadata right) {
        return left.columns().getFirst().trim().equalsIgnoreCase(right.columns().getFirst().trim());
    }

    static void addShapeChange(List<SqlRequest> requests,
                               List<SkippedSchemaChange> skipped,
                               String rawTable,
                               ColumnMetadata column,
                               DynamicField target,
                               SchemaMigrationOptions options,
                               boolean primaryKeyChanged,
                               SchemaDialect dialect,
                               SchemaTableSqlRenderer tables) {
        // 主键角色和可空性变化不能伪装成普通类型变更，否则可能留下错误约束或破坏已有数据。
        if (column.primaryKey() != target.primaryKey()) {
            if (!primaryKeyChanged) {
                skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.CHANGE_PRIMARY_KEY,
                                                    target.name(),
                                                    options.primaryKeyChangeAllowed()
                                                            ? "primary key change is not executable yet"
                                                            : "SAFE mode does not change primary key columns"));
            }
            return;
        }
        if (column.nullable() != target.nullable()) {
            if (!sameStorageShape(column, target, tables)) {
                skipped.add(new SkippedSchemaChange(
                        SkippedSchemaChange.Kind.CHANGE_COLUMN,
                        target.name(),
                        "nullable and other column attributes changed together; split and review the migration"));
                return;
            }
            if (!target.nullable() && !options.columnChangeAllowed()) {
                skipped.add(new SkippedSchemaChange(
                        SkippedSchemaChange.Kind.CHANGE_COLUMN,
                        target.name(),
                        "SAFE mode does not make a nullable column NOT NULL",
                        Map.of("column", target.name(),
                               "currentNullable", true,
                               "targetNullable", false),
                        List.of("check how many existing rows contain null",
                                "backfill or reject those rows before changing the constraint",
                                "review and approve the exact migration plan")));
                return;
            }
            requests.add(new SqlRequest(dialect.alterColumnNullabilitySql(
                    rawTable,
                    target.name(),
                    tables.dataType(target),
                    tables.columnDefinition(target),
                    target.nullable()), List.of()));
            return;
        }
        if (ProtectedSchemaTarget.sameProtectedStorage(column, target)) {
            return;
        }
        if (!sameColumnShape(column, target, tables)) {
            if (safeWidening(column, target, tables)) {
                requests.add(new SqlRequest(dialect.alterColumnTypeSql(rawTable,
                                                                        target.name(),
                                                                        tables.dataType(target),
                                                                        tables.columnDefinition(target)),
                                            List.of()));
            } else if (options.columnChangeAllowed()) {
                if (!column.generation().equals(target.generation())) {
                    skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.CHANGE_COLUMN,
                                                        target.name(),
                                                        "generated value changes require a reviewed migration"));
                    return;
                }
                requests.add(new SqlRequest(dialect.alterColumnTypeSql(rawTable,
                                                                        target.name(),
                                                                        tables.dataType(target),
                                                                        tables.columnDefinition(target)),
                                            List.of()));
            } else {
                skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.CHANGE_COLUMN,
                                                    target.name(),
                                                    "SAFE mode does not change column type, length, precision, or scale"));
            }
        }
    }

    /** nullable 单独变化时才允许生成 SQL，避免只改一半后把类型差异漏在数据库里。 */
    private static boolean sameStorageShape(ColumnMetadata column,
                                            DynamicField target,
                                            SchemaTableSqlRenderer tables) {
        return sameDataType(column, target, tables)
                && column.primaryKey() == target.primaryKey()
                && Objects.equals(column.generation(), target.generation());
    }
}
