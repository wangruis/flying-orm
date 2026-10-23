package com.flying.orm.rdb.schema;

import java.util.List;
import java.util.Objects;

/**
 * 一次实体结构同步的结构化结果。
 *
 * @param mode 本次使用的同步模式
 * @param plans 每张目标表生成的差异计划
 * @param results 已经实际执行的迁移结果；VALIDATE 和 OFF 下为空
 * @author wangr
 * @version v2.0.0
 */
public record EntitySchemaSyncReport(EntitySchemaSyncMode mode,
                                     List<SchemaMigrationPlan> plans,
                                     List<SchemaMigrationResult> results) {

    public EntitySchemaSyncReport {
        mode = Objects.requireNonNull(mode, "entity schema sync mode must not be null");
        plans = List.copyOf(Objects.requireNonNull(plans, "schema sync plans must not be null"));
        results = List.copyOf(Objects.requireNonNull(results, "schema sync results must not be null"));
    }

    public static EntitySchemaSyncReport off() {
        return new EntitySchemaSyncReport(EntitySchemaSyncMode.OFF, List.of(), List.of());
    }

    /** @return 数据库结构与实体声明存在任何差异时返回 true */
    public boolean hasDifferences() {
        return plans.stream().anyMatch(plan -> !plan.steps().isEmpty());
    }

    /** @return 至少有一项不能按当前模式自动执行时返回 true */
    public boolean hasSkippedChanges() {
        return plans.stream().anyMatch(SchemaMigrationPlan::hasSkippedChanges);
    }

    /** 实际执行结果齐备；不代表外部事务已经提交。 */
    public boolean successful() {
        return switch (mode) {
            case OFF -> true;
            case VALIDATE -> !hasDifferences();
            default -> results.size() == plans.size();
        };
    }
}
