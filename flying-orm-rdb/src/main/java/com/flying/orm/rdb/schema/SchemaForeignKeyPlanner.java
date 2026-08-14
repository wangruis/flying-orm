package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.TableMetadata;

import java.util.List;
import java.util.Map;

/**
 * 只负责外键差异的报告。
 *
 * <p>外键变更目前不会直接生成执行 SQL，而是进入迁移计划供上层审核。这样能把历史数据、锁表和
 * 不同数据库的约束语法风险明确留在计划里，不会被普通字段迁移悄悄带过。</p>
 */
final class SchemaForeignKeyPlanner {

    private SchemaForeignKeyPlanner() {
    }

    static void addChanges(List<SkippedSchemaChange> skipped,
                           TableMetadata current,
                           List<ForeignKeyMetadata> targetForeignKeys,
                           Map<String, String> columnRenames) {
        for (ForeignKeyMetadata target : targetForeignKeys) {
            current.findForeignKey(target.name())
                   .ifPresentOrElse(currentForeignKey -> {
                       if (!same(currentForeignKey, target, columnRenames)) {
                           skipped.add(change(currentForeignKey, target));
                       }
                   }, () -> skipped.add(add(target)));
        }
        for (ForeignKeyMetadata currentForeignKey : current.foreignKeys()) {
            boolean present = targetForeignKeys.stream()
                                               .anyMatch(target -> target.normalizedName()
                                                                          .equals(currentForeignKey.normalizedName()));
            if (!present) {
                skipped.add(drop(currentForeignKey));
            }
        }
    }

    static SkippedSchemaChange addForeignKeyChange(ForeignKeyMetadata target) {
        return new SkippedSchemaChange(SkippedSchemaChange.Kind.ADD_FOREIGN_KEY,
                                       target.name(),
                                       "foreign key changes are planned only",
                                       Map.of("target", target),
                                       suggestedSteps());
    }

    private static SkippedSchemaChange add(ForeignKeyMetadata target) {
        return addForeignKeyChange(target);
    }

    private static SkippedSchemaChange change(ForeignKeyMetadata current, ForeignKeyMetadata target) {
        return new SkippedSchemaChange(SkippedSchemaChange.Kind.CHANGE_FOREIGN_KEY,
                                       target.name(),
                                       "foreign key changes are planned only",
                                       Map.of("current", current, "target", target),
                                       suggestedSteps());
    }

    private static SkippedSchemaChange drop(ForeignKeyMetadata current) {
        return new SkippedSchemaChange(SkippedSchemaChange.Kind.DROP_FOREIGN_KEY,
                                       current.name(),
                                       "foreign key changes are planned only",
                                       Map.of("current", current),
                                       suggestedSteps());
    }

    private static List<String> suggestedSteps() {
        return List.of("review data that depends on this relationship",
                       "write a checked migration script for the foreign key change",
                       "run it from the upper layer in a controlled transaction or maintenance window",
                       "read table metadata again after the migration");
    }

    private static boolean same(ForeignKeyMetadata current,
                                ForeignKeyMetadata target,
                                Map<String, String> columnRenames) {
        return SchemaMigrationSupport.sameNamesIgnoreCase(
                       SchemaMigrationSupport.renamedNames(current.columns(), columnRenames), target.columns())
                && current.normalizedReferenceTable().equals(target.normalizedReferenceTable())
                && SchemaMigrationSupport.sameNamesIgnoreCase(current.referenceColumns(), target.referenceColumns());
    }
}
