package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/** 普通增量和完整关系输入共用的操作规划；不读取数据库，不决定应用失败策略。 */
final class SchemaOperationPlanner {
    private enum MigrationStage {
        EARLY_FOREIGN_KEY_DROP, RENAME_COLUMNS, EARLY_ADD, REMOVE_DISRUPTIVE_DEPENDENCY, ALTER_COLUMNS,
        REPLACE_BEFORE_COLUMN_DROP, DROP_COLUMNS, RESTORE_DEPENDENCY, LATE_DROP
    }

    private SchemaOperationPlanner() {
    }

    /** 输入适配只负责事实和 SQL；操作依赖、排序及最终步骤只有这一处实现。 */
    static List<SchemaPlanStep> plan(List<SchemaOperation> operations,
                                    RelationalTableDefinition actual,
                                    SchemaDialect dialect,
                                    Predicate<SchemaOperation> typeChanged,
                                    Function<SchemaOperation, List<SqlRequest>> render) {
        Set<String> changedColumns = changedColumns(operations);
        Set<String> earlyForeignKeyDrops = new HashSet<>();
        operations = expandForeignKeyChanges(operations, changedColumns, earlyForeignKeyDrops);
        Set<String> removedColumns = new HashSet<>();
        Set<String> disruptiveColumns = new HashSet<>();
        for (SchemaOperation operation : operations) {
            if (operation.kind() == SchemaOperation.Kind.DROP_COLUMN) {
                removedColumns.add(operation.objectName());
                disruptiveColumns.add(operation.objectName());
            } else if (operation.kind() == SchemaOperation.Kind.CHANGE_COLUMN && typeChanged.test(operation)) {
                disruptiveColumns.add(operation.objectName());
            }
        }
        List<ForeignKeyDefinition> actualReferences = selfReferences(actual);
        Set<String> retainedReferenceColumns = retainedSelfReferenceColumns(operations, actualReferences);
        collectEarlyForeignKeyDrops(operations, disruptiveColumns, dialect, earlyForeignKeyDrops);
        Set<String> dependencyReadyColumns = new HashSet<>();
        List<SchemaPlanStep> steps = new ArrayList<>();
        for (SchemaOperation operation : ordered(operations, changedColumns, disruptiveColumns,
                earlyForeignKeyDrops, removedColumns)) {
            if (hasUnresolvedDependency(operation, operations, changedColumns, disruptiveColumns,
                    dependencyReadyColumns, removedColumns, retainedReferenceColumns, actualReferences,
                    earlyForeignKeyDrops, dialect)) {
                throw new UnsupportedOperationException("unresolved schema dependency for "
                        + operation.kind() + ": " + operation.objectName());
            }
            List<SqlRequest> requests = render.apply(operation);
            if (!requests.isEmpty()) steps.add(new SchemaPlanStep(operation, requests));
            if (operation.kind() == SchemaOperation.Kind.ADD_COLUMN
                    || operation.kind() == SchemaOperation.Kind.CHANGE_COLUMN
                    || operation.kind() == SchemaOperation.Kind.RENAME_COLUMN) {
                dependencyReadyColumns.add(operation.objectName());
                if (operation.kind() == SchemaOperation.Kind.RENAME_COLUMN) {
                    dependencyReadyColumns.add(((ColumnDefinition) operation.actual()).name());
                }
            }
        }
        return steps;
    }

    /** 涉及列演进的外键替换必须拆开排序；独立外键修改仍使用方言的一次完整替换。 */
    private static List<SchemaOperation> expandForeignKeyChanges(List<SchemaOperation> operations,
                                                                 Set<String> changedColumns, Set<String> earlyDrops) {
        List<SchemaOperation> expanded = null;
        for (int index = 0; index < operations.size(); index++) {
            SchemaOperation operation = operations.get(index);
            if (operation.kind() == SchemaOperation.Kind.CHANGE_FOREIGN_KEY
                    && references(operation, changedColumns)) {
                if (expanded == null) expanded = new ArrayList<>(operations.subList(0, index));
                ForeignKeyDefinition actual = (ForeignKeyDefinition) operation.actual();
                earlyDrops.add(actual.name());
                expanded.add(SchemaOperation.of(SchemaOperation.Kind.DROP_FOREIGN_KEY, operation.relation(),
                        actual.name(), actual, null, operation.compatibility()));
                expanded.add(SchemaOperation.of(SchemaOperation.Kind.ADD_FOREIGN_KEY, operation.relation(),
                        operation.objectName(), null, operation.desired(), operation.compatibility()));
            } else if (expanded != null) {
                expanded.add(operation);
            }
        }
        return expanded == null ? operations : expanded;
    }

    private static void collectEarlyForeignKeyDrops(List<SchemaOperation> operations,
                                                     Set<String> disruptiveColumns, SchemaDialect dialect,
                                                     Set<String> drops) {
        Set<String> columns = new HashSet<>(disruptiveColumns);
        for (SchemaOperation operation : operations) {
            if (operation.kind() == SchemaOperation.Kind.RENAME_COLUMN) {
                columns.add(((ColumnDefinition) operation.actual()).name());
            }
        }
        for (SchemaOperation operation : operations) {
            if (operation.kind() != SchemaOperation.Kind.DROP_FOREIGN_KEY) continue;
            ForeignKeyDefinition key = (ForeignKeyDefinition) operation.actual();
            if (references(operation, columns) || operations.stream().anyMatch(candidate -> {
                if (candidate.kind() == SchemaOperation.Kind.ADD_FOREIGN_KEY
                        && SchemaDefinitionEquality.sameName(
                                candidate.objectName(), operation.objectName(), dialect)) return true;
                List<String> removed = SchemaForeignKeyPlanner.removedCandidateKey(candidate);
                return removed != null && key.reference().equals(operation.relation())
                        && SchemaDefinitionEquality.sameCandidateKeyColumns(key.referenceColumns(), removed, dialect);
            })) drops.add(operation.objectName());
        }
    }

    private static boolean hasUnresolvedDependency(SchemaOperation operation,
                                         List<SchemaOperation> operations,
                                         Set<String> changedColumns,
                                         Set<String> disruptiveColumns,
                                         Set<String> dependencyReadyColumns,
                                         Set<String> removedColumns,
                                         Set<String> retainedReferenceColumns,
                                         List<ForeignKeyDefinition> actualReferences,
                                         Set<String> earlyForeignKeyDrops,
                                         SchemaDialect dialect) {
        if (isChangedDependency(operation.kind()) && !changedColumns.isEmpty()) {
            boolean compatible = switch (operation.kind()) {
                case CHANGE_PRIMARY_KEY, CHANGE_UNIQUE, CHANGE_INDEX -> true;
                default -> false;
            };
            for (String column : referencedColumns(operation)) {
                if (changedColumns.contains(column)
                        && (!compatible || !removedColumns.contains(column) && !dependencyReadyColumns.contains(column))) {
                    return true;
                }
            }
        }
        if (operation.kind() == SchemaOperation.Kind.CHANGE_COLUMN
                && disruptiveColumns.contains(operation.objectName())
                && retainedReferenceColumns.contains(operation.objectName())) {
            return true;
        }
        List<String> removedCandidate = SchemaForeignKeyPlanner.removedCandidateKey(operation);
        if (removedCandidate != null && actualReferences.stream()
                .filter(reference -> SchemaDefinitionEquality.sameCandidateKeyColumns(
                        reference.referenceColumns(), removedCandidate, dialect))
                .anyMatch(reference -> !earlyForeignKeyDrops.contains(reference.name()))) {
            return true;
        }
        return (isAddDependency(operation.kind()) || isDropDependency(operation.kind()))
                && hasDisruptiveReplacement(operation, operations, disruptiveColumns);
    }

    private static List<ForeignKeyDefinition> selfReferences(RelationalTableDefinition table) {
        return table == null ? List.of() : table.foreignKeys().stream()
                .filter(key -> key.reference().equals(table.identity())).toList();
    }

    private static Set<String> retainedSelfReferenceColumns(
            List<SchemaOperation> operations, List<ForeignKeyDefinition> actual) {
        Set<String> columns = new HashSet<>();
        Set<String> replaced = new HashSet<>();
        Map<String, String> renames = new HashMap<>();
        for (SchemaOperation operation : operations) {
            if (operation.kind() == SchemaOperation.Kind.RENAME_COLUMN) {
                renames.put(((ColumnDefinition) operation.actual()).name(), operation.objectName());
            }
            // 新增外键在改列之后执行，不能用尚未存在的依赖拦截合法 DDL。
            if (operation.kind() == SchemaOperation.Kind.DROP_FOREIGN_KEY) replaced.add(operation.objectName());
        }
        for (ForeignKeyDefinition key : actual) {
            if (!replaced.contains(key.name())) {
                key.columns().forEach(column -> columns.add(renames.getOrDefault(column, column)));
                key.referenceColumns().forEach(column -> columns.add(renames.getOrDefault(column, column)));
            }
        }
        return columns;
    }

    private static boolean hasDisruptiveReplacement(SchemaOperation operation,
                                                    List<SchemaOperation> operations,
                                                    Set<String> disruptiveColumns) {
        if (disruptiveColumns.isEmpty()
                || operation.kind() == SchemaOperation.Kind.ADD_FOREIGN_KEY
                || operation.kind() == SchemaOperation.Kind.DROP_FOREIGN_KEY) {
            return false;
        }
        int family = dependencyFamily(operation.kind());
        boolean addition = isAddDependency(operation.kind());
        Set<String> ownColumns = referencedColumns(operation);
        return operations.stream().anyMatch(candidate -> {
            if (candidate == operation
                    || dependencyFamily(candidate.kind()) != family
                    || addition == isAddDependency(candidate.kind())) {
                return false;
            }
            Set<String> candidateColumns = referencedColumns(candidate);
            return ownColumns.stream().anyMatch(column ->
                    disruptiveColumns.contains(column) && candidateColumns.contains(column));
        });
    }


    private static List<SchemaOperation> ordered(List<SchemaOperation> operations, Set<String> changedColumns,
                                                Set<String> disruptiveColumns, Set<String> earlyForeignKeyDrops,
                                                Set<String> removedColumns) {
        return operations.stream().sorted(Comparator
                .comparing((SchemaOperation operation) -> migrationStage(
                        operation, changedColumns, disruptiveColumns, earlyForeignKeyDrops, removedColumns))
                .thenComparing(SchemaOperation::kind)
                // ADD COLUMN 必须保持目标声明的物理顺序。
                .thenComparing(operation -> operation.kind() == SchemaOperation.Kind.ADD_COLUMN
                        || operation.kind() == SchemaOperation.Kind.RENAME_COLUMN
                        ? "" : operation.objectName())).toList();
    }

    private static Set<String> changedColumns(List<SchemaOperation> operations) {
        Set<String> columns = new HashSet<>();
        for (SchemaOperation operation : operations) {
            if (operation.kind() == SchemaOperation.Kind.ADD_COLUMN
                    || operation.kind() == SchemaOperation.Kind.CHANGE_COLUMN
                    || operation.kind() == SchemaOperation.Kind.DROP_COLUMN
                    || operation.kind() == SchemaOperation.Kind.RENAME_COLUMN) {
                columns.add(operation.objectName());
                if (operation.kind() == SchemaOperation.Kind.RENAME_COLUMN) {
                    columns.add(((ColumnDefinition) operation.actual()).name());
                }
            }
        }
        return columns;
    }

    private static boolean references(SchemaOperation operation, Set<String> columns) {
        return !columns.isEmpty() && referencedColumns(operation).stream().anyMatch(columns::contains);
    }

    static Set<String> referencedColumns(SchemaOperation operation) {
        Set<String> columns = new HashSet<>();
        addReferencedColumns(operation.actual(), operation.relation(), columns);
        addReferencedColumns(operation.desired(), operation.relation(), columns);
        return columns;
    }

    private static void addReferencedColumns(Object definition,
                                             RelationIdentity relation,
                                             Set<String> columns) {
        switch (definition) {
            case PrimaryKeyDefinition value -> columns.addAll(value.columns());
            case UniqueConstraintDefinition value -> columns.addAll(value.columns());
            case IndexDefinition value -> value.keys().forEach(key -> columns.add(key.column()));
            case CheckConstraintDefinition value -> addReferencedColumns(value.predicate(), columns);
            case ForeignKeyDefinition value -> {
                columns.addAll(value.columns());
                if (value.reference().equals(relation)) {
                    columns.addAll(value.referenceColumns());
                }
            }
            case null, default -> {
            }
        }
    }

    private static void addReferencedColumns(CheckPredicate predicate, Set<String> columns) {
        switch (predicate) {
            case CheckPredicate.Comparison value -> columns.add(value.column());
            case CheckPredicate.Range value -> columns.add(value.column());
            case CheckPredicate.In value -> columns.add(value.column());
            case CheckPredicate.NullCheck value -> columns.add(value.column());
            case CheckPredicate.Logical value -> value.predicates()
                    .forEach(child -> addReferencedColumns(child, columns));
            case CheckPredicate.Negation value -> addReferencedColumns(value.predicate(), columns);
        }
    }

    private static MigrationStage migrationStage(SchemaOperation operation,
                                                  Set<String> changedColumns,
                                                  Set<String> disruptiveColumns,
                                                  Set<String> earlyForeignKeyDrops,
                                                  Set<String> removedColumns) {
        if (operation.kind() == SchemaOperation.Kind.DROP_FOREIGN_KEY
                && earlyForeignKeyDrops.contains(operation.objectName())) return MigrationStage.EARLY_FOREIGN_KEY_DROP;
        if (operation.kind() == SchemaOperation.Kind.RENAME_COLUMN) return MigrationStage.RENAME_COLUMNS;
        if (isAddDependency(operation.kind())) {
            return references(operation, changedColumns)
                    ? MigrationStage.RESTORE_DEPENDENCY : MigrationStage.EARLY_ADD;
        }
        if (isDropDependency(operation.kind())) {
            return references(operation, disruptiveColumns)
                    ? MigrationStage.REMOVE_DISRUPTIVE_DEPENDENCY : MigrationStage.LATE_DROP;
        }
        if (operation.kind() == SchemaOperation.Kind.DROP_COLUMN) return MigrationStage.DROP_COLUMNS;
        if (isChangedDependency(operation.kind()) && references(operation, removedColumns)) {
            return MigrationStage.REPLACE_BEFORE_COLUMN_DROP;
        }
        return isChangedDependency(operation.kind())
                ? MigrationStage.RESTORE_DEPENDENCY : MigrationStage.ALTER_COLUMNS;
    }

    static boolean isAddDependency(SchemaOperation.Kind kind) {
        return switch (kind) {
            case ADD_PRIMARY_KEY, ADD_UNIQUE, ADD_INDEX, ADD_CHECK, ADD_FOREIGN_KEY -> true;
            default -> false;
        };
    }

    static boolean isDropDependency(SchemaOperation.Kind kind) {
        return switch (kind) {
            case DROP_PRIMARY_KEY, DROP_UNIQUE, DROP_INDEX, DROP_CHECK, DROP_FOREIGN_KEY -> true;
            default -> false;
        };
    }

    static boolean isChangedDependency(SchemaOperation.Kind kind) {
        return switch (kind) {
            case CHANGE_PRIMARY_KEY, CHANGE_UNIQUE, CHANGE_INDEX, CHANGE_CHECK, CHANGE_FOREIGN_KEY -> true;
            default -> false;
        };
    }

    static int dependencyFamily(SchemaOperation.Kind kind) {
        return switch (kind) {
            case ADD_PRIMARY_KEY, DROP_PRIMARY_KEY -> 1;
            case ADD_UNIQUE, DROP_UNIQUE -> 2;
            case ADD_INDEX, DROP_INDEX -> 3;
            case ADD_CHECK, DROP_CHECK -> 4;
            case ADD_FOREIGN_KEY, DROP_FOREIGN_KEY -> 5;
            default -> 0;
        };
    }


}
