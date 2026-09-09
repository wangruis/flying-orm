package com.flying.orm.rdb.schema;

import java.util.Objects;

/**
 * 对一份确定迁移计划的显式批准。
 *
 * <p>批准保存计划指纹而不是一个宽泛 boolean。计划里的 SQL、回滚步骤或风险缺口发生任何变化，旧批准都会失效。</p>
 *
 * @param planFingerprint 被批准计划的稳定指纹
 * @param reason 为什么接受这次不可自动回滚的风险
 * @param writesQuiesced 调用方确认相关写入已静止，并会保持至验证成功或完成失败恢复
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SchemaMigrationApproval(String planFingerprint, String reason, boolean writesQuiesced) {

    /** 原有批准只接受计划风险，不替调用方声明写入已经停止。 */
    public SchemaMigrationApproval(String planFingerprint, String reason) {
        this(planFingerprint, reason, false);
    }

    public SchemaMigrationApproval {
        planFingerprint = requireText(planFingerprint, "migration approval fingerprint");
        reason = requireText(reason, "migration approval reason");
    }

    /**
     * 为当前审核结果生成批准对象。调用方应把真实的备份单、变更单或人工确认原因传进来，
     * 不能只传一个没有意义的占位文本。计划内容一旦改变，新指纹会让这份批准自动失效。
     *
     * @param plan 已经展示并确认过的审核结果
     * @param reason 接受不可自动回滚风险的原因
     * @return 只能批准这份计划的对象
     */
    public static SchemaMigrationApproval approve(ReviewedSchemaMigrationPlan plan, String reason) {
        ReviewedSchemaMigrationPlan safePlan = Objects.requireNonNull(plan, "reviewed migration plan must not be null");
        return new SchemaMigrationApproval(safePlan.fingerprint(), reason);
    }

    /** 为 3.2 完整关系审核计划生成精确批准。 */
    public static SchemaMigrationApproval approve(ReviewedSchemaPlan plan, String reason) {
        ReviewedSchemaPlan safePlan = Objects.requireNonNull(
                plan, "reviewed relational schema plan must not be null");
        return new SchemaMigrationApproval(safePlan.fingerprint(), reason);
    }

    /**
     * 批准需要静止写入窗口的精确计划，例如 MySQL/Oracle 的非原子外键替换。
     *
     * <p>调用方须先阻止所有会影响旧、新外键关系的写入，包括子表写入和引用表的更新、删除及级联写入。
     * 新建且尚未交付写入者的数据库也满足该前提。该窗口必须持续至整个计划回读验证成功；失败或取消后，
     * 由调用方检查实际结构并恢复，不能直接重放旧计划。ORM 不停止写入、不探测集群，也不自动补偿 DDL。</p>
     *
     * @param plan 已审核的冻结计划
     * @param reason 静止写入窗口及变更批准的真实说明
     * @return 同时确认计划指纹与写入前提的批准
     */
    public static SchemaMigrationApproval approveWithWritesQuiesced(ReviewedSchemaPlan plan, String reason) {
        ReviewedSchemaPlan safePlan = Objects.requireNonNull(
                plan, "reviewed relational schema plan must not be null");
        return new SchemaMigrationApproval(safePlan.fingerprint(), reason, true);
    }

    private static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }
}
