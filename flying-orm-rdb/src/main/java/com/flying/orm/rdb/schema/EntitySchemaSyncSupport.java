package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
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
            // 先完成全部实体的严格关系编译，再让任意元数据 reader 或 DDL 执行器接触数据库。
            EntitySchemaDescriptor<?> descriptor = models.schemaDescriptor(safeType);
            String tableKey = normalizeTable(descriptor.metadata().table());
            Class<?> previous = byTable.putIfAbsent(tableKey, safeType);
            if (previous != null) {
                throw new IllegalArgumentException("multiple entity types map to the same table '"
                                                           + descriptor.metadata().table() + "': " + previous.getName()
                                                           + " and " + safeType.getName());
            }
            byType.put(safeType, new EntitySchemaTarget(descriptor));
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
        // 迁移 DDL 会按传入大小写引用物理表名。大小写不同的 quoted identifier 可能是两个真实表，
        // 去重和审批键必须保留同一物理身份，不能把一个表的批准复用到另一个表。
        return text;
    }
}

record EntitySchemaTarget(EntitySchemaDescriptor<?> descriptor) {

    EntitySchemaTarget {
        descriptor = Objects.requireNonNull(descriptor, "entity schema descriptor must not be null");
    }

    /** 保留启动同步内部既有的轻量访问写法，真实所有权仍由完整 descriptor 承担。 */
    EntityMetadata<?> metadata() {
        return descriptor.metadata();
    }
}
