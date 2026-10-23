package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * 适配增量外键的名称与改名事实；差异规则归 SchemaDiffer。
 */
final class SchemaForeignKeyPlanner {

    private SchemaForeignKeyPlanner() {
    }

    /**
     * 多表第二阶段发布稳定外键 operation。不能由当前事实安全闭合的外键明确转成人工项，
     * 不会退回到字符串说明或猜测执行。
     */
    static List<SchemaOperation> addOperations(
            RelationalTableDefinition target,
            Predicate<ForeignKeyDefinition> manualRequired) {
        RelationalTableDefinition safeTarget = Objects.requireNonNull(
                target, "relational table definition must not be null");
        Predicate<ForeignKeyDefinition> safeManualRequired = Objects.requireNonNull(
                manualRequired, "foreign key manual predicate must not be null");
        return safeTarget.foreignKeys().stream()
                .sorted(java.util.Comparator.comparing(ForeignKeyDefinition::name))
                .map(foreignKey -> SchemaOperation.of(
                        safeManualRequired.test(foreignKey)
                                ? SchemaOperation.Kind.VERIFY_MANUALLY
                                : SchemaOperation.Kind.ADD_FOREIGN_KEY,
                        safeTarget.identity(),
                        foreignKey.name(),
                        null,
                        foreignKey,
                        SchemaOperation.Compatibility.REQUIRES_REVIEW))
                .toList();
    }

    static Changes planChanges(RelationIdentity relation,
                               RelationalTableDefinition current,
                               List<ForeignKeyDefinition> targetForeignKeys,
                               Map<String, String> columnRenames) {
        List<SchemaOperation> operations = new ArrayList<>();
        Set<String> ambiguousTargetNames = SchemaMigrationSupport.ambiguousFoldedNames(
                targetForeignKeys.stream().map(ForeignKeyDefinition::name).toList());
        // 增量声明没有提到的外键继续保留，不提供删除候选；删除由完整目标替换表达。
        SchemaDiffer.diffMatchedList(relation, targetForeignKeys, List.of(), ForeignKeyDefinition::name, target -> {
            ForeignKeyDefinition currentForeignKey = SchemaMigrationSupport.exactForeignKey(current, target.name());
            if (currentForeignKey == null && !ambiguousTargetNames.contains(target.normalizedName())) {
                currentForeignKey = current.findForeignKey(target.name()).orElse(null);
            }
            return currentForeignKey;
        }, (source, target) -> same(source, target, columnRenames, relation),
                SchemaOperation.Kind.ADD_FOREIGN_KEY, SchemaOperation.Kind.CHANGE_FOREIGN_KEY,
                SchemaOperation.Kind.DROP_FOREIGN_KEY, operations);
        Set<String> replaced = new HashSet<>();
        for (SchemaOperation operation : operations) {
            if (operation.kind() == SchemaOperation.Kind.CHANGE_FOREIGN_KEY) {
                replaced.add(((ForeignKeyDefinition) operation.actual()).name());
            }
        }
        return new Changes(operations, current.foreignKeys().stream()
                .filter(key -> !replaced.contains(key.name())).toList());
    }

    record Changes(List<SchemaOperation> operations, List<ForeignKeyDefinition> retained) {
    }

    static List<String> removedCandidateKey(SchemaOperation operation) {
        return switch (operation.kind()) {
            case CHANGE_PRIMARY_KEY, DROP_PRIMARY_KEY -> ((PrimaryKeyDefinition) operation.actual()).columns();
            case CHANGE_UNIQUE, DROP_UNIQUE -> ((UniqueConstraintDefinition) operation.actual()).columns();
            case CHANGE_INDEX, DROP_INDEX -> {
                IndexDefinition index = (IndexDefinition) operation.actual();
                yield index.unique() ? index.columns() : null;
            }
            default -> null;
        };
    }

    private static boolean same(ForeignKeyDefinition current,
                                ForeignKeyDefinition target,
                                Map<String, String> columnRenames,
                                RelationIdentity relation) {
        return current.name().equals(target.name())
                && SchemaMigrationSupport.renamedNames(current.columns(), columnRenames).equals(target.columns())
                && current.reference().equals(target.reference())
                && current.onDelete() == target.onDelete()
                && current.onUpdate() == target.onUpdate()
                && (current.reference().equals(relation)
                    ? SchemaMigrationSupport.renamedNames(current.referenceColumns(), columnRenames)
                    : current.referenceColumns()).equals(target.referenceColumns());
    }
}
