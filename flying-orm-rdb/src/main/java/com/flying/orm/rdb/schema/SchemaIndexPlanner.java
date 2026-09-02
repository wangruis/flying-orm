package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** 只负责索引差异的匹配、重建和删除规划。 */
final class SchemaIndexPlanner {

    private final SchemaTableSqlRenderer tables;

    SchemaIndexPlanner(SchemaTableSqlRenderer tables) {
        this.tables = Objects.requireNonNull(tables, "schema table renderer must not be null");
    }

    void addChanges(List<SqlRequest> requests,
                    List<SkippedSchemaChange> skipped,
                    TableMetadata current,
                    DynamicForm target,
                    List<IndexMetadata> indexes,
                    SchemaMigrationOptions options) {
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
                addChange(requests, skipped, target.table(), currentIndex, index, options);
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
                requests.add(tables.createIndex(target.table(), index));
            }
        }
        addRemovals(requests,
                    skipped,
                    current,
                    target.table(),
                    options,
                    targetPhysicalIndexNames,
                    consumedLegacyIndexNames,
                    matchedCurrentIndexNames);
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
        if (current.name().equals(target.name())
                && current.unique() == target.unique()
                && SchemaMigrationSupport.sameIndexColumns(current, target, options.columnRenames())) {
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

    private void addRemovals(List<SqlRequest> requests,
                             List<SkippedSchemaChange> skipped,
                             TableMetadata current,
                             String table,
                             SchemaMigrationOptions options,
                             Set<String> targetPhysicalNames,
                             Set<String> consumedLegacyNames,
                             Set<String> matchedCurrentNames) {
        for (IndexMetadata index : current.indexes()) {
            boolean present = consumedLegacyNames.contains(index.normalizedName())
                    || matchedCurrentNames.contains(index.name())
                    || targetPhysicalNames.contains(index.name());
            if (present) {
                continue;
            }
            if (options.dropIndexAllowed()) {
                requests.add(tables.dropIndex(table, index));
            } else {
                skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.DROP_INDEX,
                                                    index.name(),
                                                    "SAFE mode does not drop existing indexes"));
            }
        }
    }
}
