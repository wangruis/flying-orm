package com.flying.orm.rdb.schema;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.sql.render.SqlRequest;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** 正向计划、反向结构计划和在线 DDL 审核合在一起的上线前结果。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record ReviewedSchemaMigrationPlan(SchemaMigrationPlan migration,
                                          SchemaRollbackPlan rollback,
                                          OnlineDdlReview onlineDdl) {

    private static final StableDigest.Domain FINGERPRINT_DOMAIN =
            StableDigest.domain("reviewed-schema-migration/v4");

    public ReviewedSchemaMigrationPlan {
        migration = Objects.requireNonNull(migration, "migration plan must not be null");
        rollback = Objects.requireNonNull(rollback, "rollback plan must not be null");
        onlineDdl = Objects.requireNonNull(onlineDdl, "online DDL review must not be null");
        requireLiteralDdl(migration.requests());
        requireLiteralDdl(rollback.requests());
        requireLiteralDdl(onlineDdl.potentiallyBlocking());
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
        return requestsForExecution(approval, null);
    }

    /** 执行器已经为同一次批准和观测计算指纹时复用它，避免在内部可信链重复摘要。 */
    List<SqlRequest> requestsForExecution(SchemaMigrationApproval approval, String knownFingerprint) {
        if (!onlineDdl.executionAllowed()) {
            String planFingerprint = fingerprint(knownFingerprint);
            throw new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.ONLINE_DDL_REQUIRED,
                    planFingerprint,
                    "online DDL is required, but the plan contains potentially blocking SQL");
        }
        boolean manualPrimaryKeyChange = migration.skippedChanges().stream()
                                                  .anyMatch(change -> change.kind()
                                                          == SkippedSchemaChange.Kind.CHANGE_PRIMARY_KEY);
        if (manualPrimaryKeyChange) {
            String planFingerprint = fingerprint(knownFingerprint);
            throw new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.MANUAL_ACTION_REQUIRED,
                    planFingerprint,
                    "primary key changes require a checked manual script; no schema SQL was executed");
        }
        if (requiresExplicitApproval()) {
            String planFingerprint = fingerprint(knownFingerprint);
            if (approval == null || !planFingerprint.equals(approval.planFingerprint())) {
                throw new SchemaMigrationRejectedException(
                        SchemaMigrationFailureCode.APPROVAL_REQUIRED,
                        planFingerprint,
                        "migration has rollback gaps and requires approval for this exact plan");
            }
        }
        return migration.requests();
    }

    private String fingerprint(String knownFingerprint) {
        return knownFingerprint == null ? fingerprint() : knownFingerprint;
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
        StableEncoder content = StableDigest.sha256(FINGERPRINT_DOMAIN)
                                             .text("TARGET_TABLE", migration.target().table())
                                             .bool("TABLE_EXISTS", migration.tableExists())
                                             .text("ONLINE_MODE", onlineDdl.mode().name())
                                             .text("ONLINE_SUPPORT", onlineDdl.support().name())
                                             .bool("NON_TRANSACTIONAL", onlineDdl.requiresNonTransactionalExecution())
                                             .integer("FORWARD_COUNT", migration.requests().size());
        migration.requests().forEach(request -> appendRequest(content, "forward", request));
        content.integer("SKIPPED_COUNT", migration.skippedChanges().size());
        migration.skippedChanges().forEach(change -> {
            content.marker("SKIPPED_CHANGE")
                   .text("SKIPPED_KIND", change.kind().name())
                   .text("SKIPPED_NAME", change.name())
                   .text("SKIPPED_REASON", change.reason())
                   .integer("DETAIL_COUNT", change.details().size());
            // 主键计划没有通用 SQL，真正需要绑定的是旧键、新键和人工步骤，不能只对一行摘要做批准。
            change.details().entrySet().stream()
                  .sorted(java.util.Map.Entry.comparingByKey())
                  .forEach(entry -> {
                      content.text("DETAIL_KEY", entry.getKey());
                      appendDetail(content, entry.getValue());
                  });
            content.integer("SUGGESTED_STEP_COUNT", change.suggestedSteps().size());
            change.suggestedSteps().forEach(step -> content.text("SUGGESTED_STEP", step));
        });
        content.integer("ROLLBACK_COUNT", rollback.requests().size());
        rollback.requests().forEach(request -> appendRequest(content, "rollback", request));
        content.integer("ROLLBACK_GAP_COUNT", rollback.gaps().size());
        rollback.gaps().forEach(gap -> {
            content.marker("ROLLBACK_GAP")
                   .text("ROLLBACK_GAP_KIND", gap.kind().name())
                   .text("ROLLBACK_GAP_OBJECT", gap.objectName())
                   .text("ROLLBACK_GAP_REASON", gap.reason());
        });
        content.integer("BLOCKING_COUNT", onlineDdl.potentiallyBlocking().size());
        onlineDdl.potentiallyBlocking().forEach(request -> appendRequest(content, "blocking", request));
        return content.finishHex();
    }

    private static void appendRequest(StableEncoder content, String role, SqlRequest request) {
        content.marker("REQUEST")
               .text("REQUEST_ROLE", role)
               .text("REQUEST_SQL", request.sql())
               .text("REQUEST_MARKERS", request.bindMarkerStyle().name())
               .integer("REQUEST_PARAMETER_COUNT", request.parameters().size());
    }

    private static void requireLiteralDdl(List<SqlRequest> requests) {
        for (SqlRequest request : requests) {
            if (!request.parameters().isEmpty()) {
                // DDL 标识符、类型和约束不能通过值参数绑定；允许参数会让人工批准无法稳定绑定完整语义。
                throw new IllegalArgumentException("reviewed schema DDL must not contain bound parameters");
            }
        }
    }

    /** 编码迁移规划器实际发布的封闭详情类型，不反射任意应用对象。 */
    private static void appendDetail(StableEncoder content, Object value) {
        if (value == null) {
            content.marker("DETAIL_NULL");
            return;
        }
        if (isScalar(value)) {
            content.marker("DETAIL_SCALAR")
                   .text("DETAIL_TYPE", value.getClass().getName())
                   .text("DETAIL_VALUE", scalarText(value));
            return;
        }
        if (value instanceof List<?> list) {
            content.marker("DETAIL_LIST")
                   .integer("DETAIL_SIZE", list.size());
            list.forEach(element -> appendDetail(content, element));
            return;
        }
        if (value instanceof ForeignKeyMetadata foreignKey) {
            content.marker("DETAIL_FOREIGN_KEY")
                   .text("DETAIL_FOREIGN_KEY_NAME", foreignKey.name())
                   .text("DETAIL_REFERENCE_TABLE", foreignKey.referenceTable())
                   .integer("DETAIL_FOREIGN_KEY_COLUMN_COUNT", foreignKey.columns().size());
            foreignKey.columns().forEach(column -> content.text("DETAIL_FOREIGN_KEY_COLUMN", column));
            foreignKey.referenceColumns().forEach(column -> content.text("DETAIL_REFERENCE_COLUMN", column));
            return;
        }
        throw new IllegalArgumentException(
                "unsupported schema migration detail value type: " + value.getClass().getName());
    }

    private static boolean isScalar(Object value) {
        return value instanceof String
                || value instanceof Boolean
                || value instanceof Character
                || value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Float
                || value instanceof Double
                || value instanceof BigInteger
                || value instanceof BigDecimal
                || value instanceof UUID
                || value instanceof Enum<?>
                || value instanceof Class<?>;
    }

    private static String scalarText(Object value) {
        if (value instanceof Enum<?> enumValue) {
            return enumValue.name();
        }
        if (value instanceof Class<?> type) {
            return type.getName();
        }
        return String.valueOf(value);
    }

}
