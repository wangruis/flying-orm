package com.flying.orm.rdb.schema;

import java.util.Objects;

/**
 * 对一份确定迁移计划的显式批准。
 *
 * <p>批准保存计划指纹而不是一个宽泛 boolean。计划里的 SQL、回滚步骤或风险缺口发生任何变化，旧批准都会失效。</p>
 *
 * @param planFingerprint 被批准计划的稳定指纹
 * @param reason 为什么接受这次不可自动回滚的风险
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SchemaMigrationApproval(String planFingerprint, String reason) {

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

    private static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }
}
