package com.flying.orm.rdb.schema;

import java.util.List;
import java.util.Objects;

/**
 * 实体完整关系同步的计划和验证证据。
 *
 * <p>该类型独立于 3.1 的轻量表单迁移报告，因此不会改变旧 record 的构造器、访问器或语义。</p>
 *
 * @param mode 本次同步模式
 * @param plans 每张实体表冻结后的完整关系计划
 * @param results 实际执行及执行后验证结果；OFF、VALIDATE 下为空
 * @author wangr
 * @version v3.2
 */
public record EntityRelationalSchemaSyncReport(
        EntitySchemaSyncMode mode,
        List<ReviewedSchemaPlan> plans,
        List<SchemaExecutionReport> results) {

    public EntityRelationalSchemaSyncReport {
        mode = Objects.requireNonNull(mode, "entity schema sync mode must not be null");
        plans = List.copyOf(Objects.requireNonNull(plans, "relational schema plans must not be null"));
        results = List.copyOf(Objects.requireNonNull(results, "relational schema results must not be null"));
    }

    public static EntityRelationalSchemaSyncReport off() {
        return new EntityRelationalSchemaSyncReport(EntitySchemaSyncMode.OFF, List.of(), List.of());
    }

    public boolean hasDifferences() {
        return plans.stream().anyMatch(plan -> !plan.steps().isEmpty());
    }

    public boolean requiresManualAction() {
        return plans.stream().anyMatch(ReviewedSchemaPlan::requiresManualAction);
    }

    public boolean successful() {
        if (mode == EntitySchemaSyncMode.OFF) {
            return true;
        }
        if (mode == EntitySchemaSyncMode.VALIDATE) {
            return !hasDifferences();
        }
        return results.size() == plans.size()
                && results.stream().allMatch(SchemaExecutionReport::successful);
    }
}
