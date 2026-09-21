package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** 只负责索引差异的匹配、重建和删除规划。 */
final class SchemaIndexPlanner {

    private final SchemaTableSqlRenderer tables;

    SchemaIndexPlanner(SchemaTableSqlRenderer tables) {
        this.tables = Objects.requireNonNull(tables, "schema table renderer must not be null");
    }

    /** 多表冷规划路径只发布规范 operation，不提前渲染方言 SQL。 */
    static List<SchemaOperation> addOperations(RelationalTableDefinition target) {
        RelationalTableDefinition safeTarget = Objects.requireNonNull(
                target, "relational table definition must not be null");
        return safeTarget.indexes().stream()
                .sorted(java.util.Comparator.comparing(IndexDefinition::name))
                .map(index -> SchemaOperation.of(
                        SchemaOperation.Kind.ADD_INDEX,
                        safeTarget.identity(),
                        index.name(),
                        null,
                        index,
                        SchemaOperation.Compatibility.REQUIRES_REVIEW))
                .toList();
    }

    void addChanges(List<SqlRequest> requests,
                    List<SkippedSchemaChange> skipped,
                    TableMetadata current,
                    DynamicForm target,
                    List<IndexMetadata> indexes,
                    SchemaMigrationOptions options) {
        addChanges(requests, skipped, current, target, indexes, options, Set.of());
    }

    void addChanges(List<SqlRequest> requests,
                    List<SkippedSchemaChange> skipped,
                    TableMetadata current,
                    DynamicForm target,
                    List<IndexMetadata> indexes,
                    SchemaMigrationOptions options,
                    Set<String> alreadyDropped) {
        forEachChange(current, target, indexes, (source, index) -> {
            if (source == null) {
                requests.add(tables.createIndex(target.table(), index));
            } else if (alreadyDropped.contains(source.name())) {
                requests.add(tables.createIndex(target.table(), index));
            } else {
                addChange(requests, skipped, target.table(), source, index, options);
            }
        }, index -> {
            if (alreadyDropped.contains(index.name())) {
                return;
            }
            if (options.dropIndexAllowed()) {
                requests.add(tables.dropIndex(target.table(), index));
            } else {
                skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.DROP_INDEX,
                                                    index.name(),
                                                    "SAFE mode does not drop existing indexes"));
            }
        });
    }

    DependencyDrops addDependentDrops(List<SqlRequest> requests,
                                      TableMetadata current,
                                      DynamicForm target,
                                      List<IndexMetadata> indexes,
                                      SchemaMigrationOptions options,
                                      Set<String> removedColumns) {
        if (removedColumns.isEmpty()) {
            return DependencyDrops.NONE;
        }
        Set<String> dropped = new HashSet<>();
        Set<String> blockedColumns = new HashSet<>();
        forEachChange(current, target, indexes, (source, index) -> {
            if (source == null || !dependsOn(source, removedColumns)) {
                return;
            }
            if (!sameIndex(source, index, options.columnRenames())
                    && options.rebuildIndexAllowed()) {
                requests.add(tables.dropIndex(target.table(), source));
                dropped.add(source.name());
            } else {
                addDependencies(blockedColumns, source, removedColumns);
            }
        }, index -> {
            if (!dependsOn(index, removedColumns)) {
                return;
            }
            if (options.dropIndexAllowed()) {
                requests.add(tables.dropIndex(target.table(), index));
                dropped.add(index.name());
            } else {
                addDependencies(blockedColumns, index, removedColumns);
            }
        });
        return new DependencyDrops(Set.copyOf(dropped), Set.copyOf(blockedColumns));
    }

    private static boolean dependsOn(IndexMetadata index, Set<String> columns) {
        return index.columns().stream().anyMatch(columns::contains);
    }

    private static void addDependencies(Set<String> target,
                                        IndexMetadata index,
                                        Set<String> columns) {
        index.columns().stream().filter(columns::contains).forEach(target::add);
    }

    record DependencyDrops(Set<String> droppedIndexes, Set<String> blockedColumns) {
        private static final DependencyDrops NONE = new DependencyDrops(Set.of(), Set.of());
    }

    /**
     * 正向和回滚共用索引身份匹配，但各自决定已匹配索引是否需要重建。
     * 先按目标顺序交付匹配项（source 为 null 表示新增），再按当前顺序交付移除项；
     * 已有的等价旧版生成索引只保留，不交付变更，避免将兼容别名误当作新增或删除。
     */
    static void forEachChange(TableMetadata current,
                              DynamicForm target,
                              List<IndexMetadata> indexes,
                              BiConsumer<IndexMetadata, IndexMetadata> matched,
                              Consumer<IndexMetadata> removed) {
        List<IndexMetadata> generatedIndexes = target.toTableMetadata().indexes();
        Set<String> targetIndexNames = new HashSet<>();
        indexes.forEach(index -> targetIndexNames.add(index.normalizedName()));
        Set<String> targetPhysicalIndexNames = new HashSet<>();
        indexes.forEach(index -> targetPhysicalIndexNames.add(index.name()));
        Set<String> ambiguousTargetIndexNames = SchemaMigrationSupport.ambiguousFoldedNames(
                indexes.stream().map(IndexMetadata::name).toList());
        Set<String> consumedLegacyIndexNames = new HashSet<>();
        Set<String> matchedCurrentIndexNames = new HashSet<>();

        for (IndexMetadata index : indexes) {
            IndexMetadata currentIndex = matchingIndex(current, index, ambiguousTargetIndexNames);
            if (currentIndex != null) {
                matchedCurrentIndexNames.add(currentIndex.name());
                matched.accept(currentIndex, index);
                continue;
            }
            IndexMetadata legacyIndex = SchemaMigrationSupport.findLegacyGeneratedUniqueIndex(
                    current.indexes(),
                    generatedIndexes,
                    index,
                    targetIndexNames,
                    consumedLegacyIndexNames);
            if (legacyIndex != null) {
                consumedLegacyIndexNames.add(legacyIndex.normalizedName());
            } else {
                matched.accept(null, index);
            }
        }
        for (IndexMetadata index : current.indexes()) {
            boolean retained = consumedLegacyIndexNames.contains(index.normalizedName())
                    || matchedCurrentIndexNames.contains(index.name())
                    || targetPhysicalIndexNames.contains(index.name());
            if (!retained) {
                removed.accept(index);
            }
        }
    }

    private static IndexMetadata matchingIndex(TableMetadata current,
                                                IndexMetadata target,
                                                Set<String> ambiguousTargetNames) {
        IndexMetadata exact = SchemaMigrationSupport.exactIndex(current, target.name());
        if (exact != null || ambiguousTargetNames.contains(target.normalizedName())) {
            return exact;
        }
        return current.findIndex(target.name()).orElse(null);
    }

    private void addChange(List<SqlRequest> requests,
                           List<SkippedSchemaChange> skipped,
                           String table,
                           IndexMetadata current,
                           IndexMetadata target,
                           SchemaMigrationOptions options) {
        if (sameIndex(current, target, options.columnRenames())) {
            return;
        }
        if (options.rebuildIndexAllowed()) {
            requests.add(tables.dropIndex(table, current));
            requests.add(tables.createIndex(table, target));
        } else {
            skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.CHANGE_INDEX,
                                                target.name(),
                                                "SAFE mode does not rebuild existing indexes"));
        }
    }

    /** 正向和回滚必须按同一份列重命名事实判断索引是否真的发生变化。 */
    static boolean sameIndex(IndexMetadata current,
                             IndexMetadata target,
                             Map<String, String> columnRenames) {
        return current.name().equals(target.name())
                && current.unique() == target.unique()
                && SchemaMigrationSupport.sameIndexColumns(current, target, columnRenames);
    }
}
