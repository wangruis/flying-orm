package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;

import java.util.HashSet;
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
                && SchemaGeneratedValueComparison.same(column, target);
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
                || !SchemaGeneratedValueComparison.same(current, target)) {
            return false;
        }
        return SchemaDialectTypeSupport.safeWideningDataType(tables.dataType(current),
                                                              tables.dataType(target));
    }

    private static boolean sameDataType(ColumnMetadata column,
                                        DynamicField target,
                                        SchemaTableSqlRenderer tables) {
        return tables.sameDataType(tables.dataType(column), tables.dataType(target));
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
            if (exactColumn(current, oldName) == null) {
                throw new IllegalArgumentException("rename source column does not exist");
            }
            if (exactField(target, newName) == null) {
                throw new IllegalArgumentException("rename target field does not exist");
            }
            if (exactColumn(current, newName) != null) {
                throw new IllegalArgumentException("rename target column already exists");
            }
            if (exactField(target, oldName) != null) {
                throw new IllegalArgumentException("rename source is still present in target form");
            }
        });
    }

    static ColumnMetadata exactColumn(TableMetadata table, String name) {
        String exactName = FieldIdentity.of(name).name();
        return table.columns().stream()
                   .filter(column -> column.name().equals(exactName))
                   .findFirst()
                   .orElse(null);
    }

    static DynamicField exactField(DynamicForm form, String name) {
        String exactName = FieldIdentity.of(name).name();
        return form.fields().stream()
                   .filter(field -> field.name().equals(exactName))
                   .findFirst()
                   .orElse(null);
    }

    static IndexMetadata exactIndex(TableMetadata table, String name) {
        String exactName = FieldIdentity.of(name).name();
        return table.indexes().stream()
                    .filter(index -> index.name().equals(exactName))
                    .findFirst()
                    .orElse(null);
    }

    static ForeignKeyMetadata exactForeignKey(TableMetadata table, String name) {
        String exactName = FieldIdentity.of(name).name();
        return table.foreignKeys().stream()
                    .filter(foreignKey -> foreignKey.name().equals(exactName))
                    .findFirst()
                    .orElse(null);
    }

    static Set<String> ambiguousFoldedNames(List<String> names) {
        Set<String> seen = new HashSet<>();
        Set<String> ambiguous = new HashSet<>();
        for (String name : names) {
            String folded = FieldIdentity.of(name).key();
            if (!seen.add(folded)) {
                ambiguous.add(folded);
            }
        }
        return Set.copyOf(ambiguous);
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
        String exact = renames.entrySet().stream()
                              .filter(entry -> entry.getValue().equals(targetName))
                              .map(Map.Entry::getKey)
                              .findFirst()
                              .orElse(null);
        if (exact != null) {
            return exact;
        }
        return uniqueFoldedRename(renames, targetName, false);
    }

    static String renameTargetForSource(Map<String, String> renames, String sourceName) {
        String exact = renames.entrySet().stream()
                              .filter(entry -> entry.getKey().equals(sourceName))
                              .map(Map.Entry::getValue)
                              .findFirst()
                              .orElse(null);
        if (exact != null) {
            return exact;
        }
        return uniqueFoldedRename(renames, sourceName, true);
    }

    private static String uniqueFoldedRename(Map<String, String> renames,
                                             String name,
                                             boolean source) {
        String folded = FieldIdentity.of(name).key();
        String match = null;
        for (Map.Entry<String, String> entry : renames.entrySet()) {
            String candidate = source ? entry.getKey() : entry.getValue();
            if (FieldIdentity.of(candidate).key().equals(folded)) {
                if (match != null) {
                    return null;
                }
                match = source ? entry.getValue() : entry.getKey();
            }
        }
        return match;
    }

    static boolean sameNames(List<String> left, List<String> right) {
        return left.equals(right);
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
            if (!(renamed == null ? currentName : renamed).equals(target.columns().get(i))) {
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

    static boolean logicalTemporalTypeChanged(DatabaseType currentType, DatabaseType targetType) {
        int currentKind = logicalTimeKind(currentType);
        int targetKind = logicalTimeKind(targetType);
        return currentKind != targetKind && (currentKind != 0 || targetKind != 0);
    }

    private static int logicalTimeKind(DatabaseType dataType) {
        if (dataType.logicalType() == LogicalType.OFFSET_TIME) {
            return 2;
        }
        return dataType.logicalType() == LogicalType.TIME ? 1 : 0;
    }

    /** nullable 单独变化时才允许生成 SQL，避免只改一半后把类型差异漏在数据库里。 */
    static boolean sameStorageShape(ColumnMetadata column,
                                    DynamicField target,
                                    SchemaTableSqlRenderer tables) {
        return sameDataType(column, target, tables)
                && column.primaryKey() == target.primaryKey()
                && SchemaGeneratedValueComparison.same(column, target);
    }

    /** 把已读取的列结构还原成渲染器可复用的字段描述，不丢失生成值和类型参数。 */
    static DynamicField toDynamicField(ColumnMetadata column) {
        ColumnMetadata safeColumn = Objects.requireNonNull(column, "column metadata must not be null");
        return new DynamicField(safeColumn.name(),
                                safeColumn.dataType(),
                                safeColumn.primaryKey(),
                                safeColumn.nullable(),
                                false,
                                safeColumn.length(),
                                safeColumn.precision(),
                                safeColumn.scale(),
                                safeColumn.comment(),
                                safeColumn.generation());
    }
}
