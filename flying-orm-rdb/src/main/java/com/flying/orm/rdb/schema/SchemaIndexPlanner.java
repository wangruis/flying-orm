package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** 增量索引的输入适配与显式选项；差异规则归 SchemaDiffer。 */
final class SchemaIndexPlanner {

    private SchemaIndexPlanner() {
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

    static Changes planChanges(List<SkippedSchemaChange> skipped,
                               RelationalTableDefinition current,
                               DynamicForm target,
                               List<IndexDefinition> indexes,
                               SchemaMigrationOptions options,
                               Set<String> removedColumns,
                               List<ForeignKeyDefinition> retainedForeignKeys,
                               SchemaDialect dialect) {
        RelationIdentity relation = target.relationIdentity();
        List<SchemaOperation> operations = differences(current, indexes, options.columnRenames(), relation);
        Set<String> replaced = new HashSet<>();
        operations.removeIf(operation -> {
            if (operation.kind() == SchemaOperation.Kind.ADD_INDEX) return false;
            boolean drop = operation.kind() == SchemaOperation.Kind.DROP_INDEX;
            SkippedSchemaChange.Kind kind = drop
                    ? SkippedSchemaChange.Kind.DROP_INDEX : SkippedSchemaChange.Kind.CHANGE_INDEX;
            if (drop ? !options.dropIndexAllowed() : !options.rebuildIndexAllowed()) {
                skipped.add(new SkippedSchemaChange(kind, operation.objectName(), drop
                        ? "SAFE mode does not drop existing indexes" : "SAFE mode does not rebuild existing indexes"));
                return true;
            }
            List<String> candidate = SchemaForeignKeyPlanner.removedCandidateKey(operation);
            if (candidate == null || retainedForeignKeys.stream().noneMatch(key -> key.reference().equals(relation)
                    && SchemaDefinitionEquality.sameCandidateKeyColumns(candidate,
                        SchemaMigrationSupport.renamedNames(key.referenceColumns(), options.columnRenames()), dialect))) {
                replaced.add(((IndexDefinition) operation.actual()).name());
                return false;
            }
            skipped.add(new SkippedSchemaChange(kind,
                    operation.objectName(), "candidate index is still referenced by a retained foreign key"));
            return true;
        });
        Set<String> blockedColumns = new HashSet<>();
        if (!removedColumns.isEmpty()) {
            for (IndexDefinition index : current.indexes()) {
                if (!replaced.contains(index.name())) {
                    index.columns().stream().filter(removedColumns::contains).forEach(blockedColumns::add);
                }
            }
        }
        return new Changes(operations, blockedColumns);
    }

    record Changes(List<SchemaOperation> operations, Set<String> blockedColumns) {
    }

    /** 列改名已先执行，索引演进必须比较此时的物理列名。 */
    private static IndexDefinition renamedIndex(IndexDefinition index, Map<String, String> renames) {
        if (index.columns().stream().noneMatch(renames::containsKey)) {
            return index;
        }
        IndexDefinition.Builder renamed = IndexDefinition.builder(index.name()).unique(index.unique());
        index.keys().forEach(key -> renamed.addKey(new IndexKeyPart(
                renames.getOrDefault(key.column(), key.column()), key.direction())));
        return renamed.build();
    }

    /** 按声明的物理索引名称匹配，并使用列改名后的定义比较。 */
    private static List<SchemaOperation> differences(RelationalTableDefinition current,
                                                      List<IndexDefinition> indexes, Map<String, String> renames,
                                                      RelationIdentity relation) {
        Set<String> ambiguousTargetIndexNames = SchemaMigrationSupport.ambiguousFoldedNames(
                indexes.stream().map(IndexDefinition::name).toList());
        List<SchemaOperation> operations = new ArrayList<>();
        List<IndexDefinition> actual = renames.isEmpty() ? current.indexes()
                : current.indexes().stream().map(index -> renamedIndex(index, renames)).toList();
        SchemaDiffer.diffIndexes(relation, indexes, actual, index -> {
            IndexDefinition currentIndex = matchingIndex(current, index, ambiguousTargetIndexNames);
            return currentIndex == null ? null : renamedIndex(currentIndex, renames);
        }, (source, desired) -> SchemaDefinitionEquality.sameIndex(source, desired, null), operations);
        return operations;
    }

    private static IndexDefinition matchingIndex(RelationalTableDefinition current,
                                                IndexDefinition target,
                                                Set<String> ambiguousTargetNames) {
        IndexDefinition exact = SchemaMigrationSupport.exactIndex(current, target.name());
        if (exact != null || ambiguousTargetNames.contains(target.normalizedName())) {
            return exact;
        }
        return current.findIndex(target.name()).orElse(null);
    }

}
