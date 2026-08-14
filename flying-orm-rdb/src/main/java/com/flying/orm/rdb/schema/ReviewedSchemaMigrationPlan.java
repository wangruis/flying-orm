package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** 正向计划、反向结构计划和在线 DDL 审核合在一起的上线前结果。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record ReviewedSchemaMigrationPlan(SchemaMigrationPlan migration,
                                          SchemaRollbackPlan rollback,
                                          OnlineDdlReview onlineDdl) {

    public ReviewedSchemaMigrationPlan {
        migration = Objects.requireNonNull(migration, "migration plan must not be null");
        rollback = Objects.requireNonNull(rollback, "rollback plan must not be null");
        onlineDdl = Objects.requireNonNull(onlineDdl, "online DDL review must not be null");
    }

    /**
     * 交出允许执行的正向 SQL。在线强制模式发现潜在阻塞语句时直接拒绝，调用方不能误执行普通 DDL。
     */
    public List<SqlRequest> requestsForExecution() {
        return requestsForExecution(null);
    }

    /**
     * 校验在线要求和精确计划批准后交出正向 SQL。存在数据恢复缺口时，宽泛的“允许危险操作”不能代替批准。
     */
    public List<SqlRequest> requestsForExecution(SchemaMigrationApproval approval) {
        if (!onlineDdl.executionAllowed()) {
            throw new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.ONLINE_DDL_REQUIRED,
                    fingerprint(),
                    "online DDL is required, but the plan contains potentially blocking SQL");
        }
        boolean manualPrimaryKeyChange = migration.skippedChanges().stream()
                                                  .anyMatch(change -> change.kind()
                                                          == SkippedSchemaChange.Kind.CHANGE_PRIMARY_KEY);
        if (manualPrimaryKeyChange) {
            throw new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.MANUAL_ACTION_REQUIRED,
                    fingerprint(),
                    "primary key changes require a checked manual script; no schema SQL was executed");
        }
        if (requiresExplicitApproval()
                && (approval == null || !fingerprint().equals(approval.planFingerprint()))) {
            throw new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.APPROVAL_REQUIRED,
                    fingerprint(),
                    "migration has rollback gaps and requires approval for this exact plan");
        }
        return migration.requests();
    }

    /** @return 回滚存在数据或约束缺口时返回 true */
    public boolean requiresExplicitApproval() {
        return !rollback.complete();
    }

    /** 根据结构化回滚缺口、在线审核和跳过项给出稳定风险级别。 */
    public SchemaMigrationRiskLevel riskLevel() {
        boolean criticalGap = rollback.gaps().stream().anyMatch(gap ->
                gap.kind() == SchemaRollbackGap.Kind.DATA_CANNOT_BE_RESTORED
                        || gap.kind() == SchemaRollbackGap.Kind.PRIMARY_KEY_REQUIRES_REVIEW
                        || gap.kind() == SchemaRollbackGap.Kind.FOREIGN_KEY_REQUIRES_REVIEW);
        if (criticalGap) {
            return SchemaMigrationRiskLevel.CRITICAL;
        }
        if (!rollback.complete() || onlineDdl.requiresExternalOnlineTool()) {
            return SchemaMigrationRiskLevel.HIGH;
        }
        if (migration.requiresManualReview()) {
            return SchemaMigrationRiskLevel.MEDIUM;
        }
        return SchemaMigrationRiskLevel.LOW;
    }

    /**
     * 计算审核内容的稳定 SHA-256。这里把正向 SQL、回滚 SQL、缺口和在线要求都放进去，任何一项变化都会换指纹。
     */
    public String fingerprint() {
        StringBuilder content = new StringBuilder(migration.target().table())
                .append('|').append(migration.tableExists())
                .append('|').append(onlineDdl.mode());
        migration.requests().forEach(request -> content.append("\nforward:").append(request.sql()));
        migration.skippedChanges().forEach(change -> {
            content.append("\nskipped:").append(change.summary());
            // 主键计划没有通用 SQL，真正需要绑定的是旧键、新键和人工步骤，不能只对一行摘要做批准。
            change.details().entrySet().stream()
                  .sorted(java.util.Map.Entry.comparingByKey())
                  .forEach(entry -> content.append("\nskipped-detail:")
                                           .append(entry.getKey()).append('=').append(entry.getValue()));
            change.suggestedSteps().forEach(step -> content.append("\nskipped-step:").append(step));
        });
        rollback.requests().forEach(request -> content.append("\nrollback:").append(request.sql()));
        rollback.gaps().forEach(gap -> content.append("\ngap:")
                                              .append(gap.kind()).append(':')
                                              .append(gap.objectName()).append(':')
                                              .append(gap.reason()));
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                                         .digest(content.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is not available", error);
        }
    }
}
