package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** 启动同步两条执行轨道共用的实体去重、报告校验和批准指纹检查。 */
final class EntitySchemaSyncSupport {

    private EntitySchemaSyncSupport() {
    }

    static List<EntitySchemaTarget> targets(EntityModelRegistry models, Collection<Class<?>> entityTypes) {
        Collection<Class<?>> safeTypes = Objects.requireNonNull(entityTypes, "entity types must not be null");
        LinkedHashMap<Class<?>, EntitySchemaTarget> byType = new LinkedHashMap<>();
        LinkedHashMap<String, Class<?>> byTable = new LinkedHashMap<>();
        for (Class<?> type : safeTypes) {
            Class<?> safeType = Objects.requireNonNull(type, "entity type must not be null");
            if (byType.containsKey(safeType)) {
                continue;
            }
            EntityMetadata<?> metadata = models.metadata(safeType);
            String tableKey = normalizeTable(metadata.table());
            Class<?> previous = byTable.putIfAbsent(tableKey, safeType);
            if (previous != null) {
                throw new IllegalArgumentException("multiple entity types map to the same table '"
                                                           + metadata.table() + "': " + previous.getName()
                                                           + " and " + safeType.getName());
            }
            byType.put(safeType, new EntitySchemaTarget(metadata));
        }
        return List.copyOf(byType.values());
    }

    static EntitySchemaSyncReport validate(List<SchemaMigrationPlan> plans) {
        EntitySchemaSyncReport report = new EntitySchemaSyncReport(
                EntitySchemaSyncMode.VALIDATE, plans, List.of(), List.of());
        if (report.hasDifferences()) {
            throw new EntitySchemaSyncException(
                    "entity schema validation found database differences", report);
        }
        return report;
    }

    static void rejectSkipped(EntitySchemaSyncMode mode,
                              List<SchemaMigrationPlan> plans,
                              List<ReviewedSchemaMigrationPlan> reviews) {
        EntitySchemaSyncReport report = new EntitySchemaSyncReport(mode, plans, reviews, List.of());
        if (report.requiresManualReview()) {
            throw new EntitySchemaSyncException(
                    "entity schema synchronization contains changes the current migration engine cannot execute",
                    report);
        }
    }

    static Map<String, SchemaMigrationApproval> normalizedApprovals(
            Map<String, SchemaMigrationApproval> approvals) {
        Map<String, SchemaMigrationApproval> safe = Objects.requireNonNull(
                approvals, "schema migration approvals must not be null");
        LinkedHashMap<String, SchemaMigrationApproval> normalized = new LinkedHashMap<>();
        safe.forEach((table, approval) -> {
            String key = normalizeTable(table);
            if (normalized.putIfAbsent(key, Objects.requireNonNull(
                    approval, "schema migration approval must not be null")) != null) {
                throw new IllegalArgumentException("duplicate schema approval table");
            }
        });
        return Map.copyOf(normalized);
    }

    static void verifyApprovals(List<ReviewedSchemaMigrationPlan> reviews,
                                Map<String, SchemaMigrationApproval> approvals) {
        // 先把所有批准核对完再执行第一条 DDL，避免第二张表批准错误时留下半完成的启动迁移。
        reviews.forEach(review -> review.requestsForExecution(approvalFor(review, approvals)));
    }

    static SchemaMigrationApproval approvalFor(ReviewedSchemaMigrationPlan review,
                                                Map<String, SchemaMigrationApproval> approvals) {
        return approvals.get(normalizeTable(review.migration().target().table()));
    }

    private static String normalizeTable(String table) {
        String text = Objects.requireNonNull(table, "schema approval table must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException("schema approval table must not be blank");
        }
        return text.toLowerCase(Locale.ROOT);
    }
}

record EntitySchemaTarget(EntityMetadata<?> metadata) {

    EntitySchemaTarget {
        metadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
    }
}
