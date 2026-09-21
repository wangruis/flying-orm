package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.List;
import java.util.Objects;

/**
 * createOrAlter 生成的迁移计划。上层可以先看它，再决定要不要执行。
 *
 * @param target         目标动态表单
 * @param targetIndexes  目标索引
 * @param targetForeignKeys 目标外键
 * @param tableExists    读库时表是否已经存在
 * @param requests       默认安全策略允许执行的 SQL
 * @param skippedChanges 默认安全策略拦下来的高风险变更
 * @param additionalCreatedTables 与主目标一起新建的辅助表；已审核的复合计划还会携带本次执行 DDL 的既有辅助表，
 *                                供执行器精确失效元数据，是否删除仍以审核生成的回滚计划为准
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public record SchemaMigrationPlan(DynamicForm target,
                                  List<IndexMetadata> targetIndexes,
                                  List<ForeignKeyMetadata> targetForeignKeys,
                                  boolean tableExists,
                                  List<SqlRequest> requests,
                                  List<SkippedSchemaChange> skippedChanges,
                                  List<String> additionalCreatedTables) {

    public SchemaMigrationPlan(DynamicForm target,
                               List<IndexMetadata> targetIndexes,
                               List<ForeignKeyMetadata> targetForeignKeys,
                               boolean tableExists,
                               List<SqlRequest> requests,
                               List<SkippedSchemaChange> skippedChanges) {
        this(target, targetIndexes, targetForeignKeys, tableExists, requests, skippedChanges, List.of());
    }

    public SchemaMigrationPlan(DynamicForm target,
                               List<IndexMetadata> targetIndexes,
                               boolean tableExists,
                               List<SqlRequest> requests,
                               List<SkippedSchemaChange> skippedChanges) {
        this(target, targetIndexes, List.of(), tableExists, requests, skippedChanges, List.of());
    }

    public SchemaMigrationPlan {
        target = Objects.requireNonNull(target, "target dynamic form must not be null");
        targetIndexes = List.copyOf(Objects.requireNonNull(targetIndexes, "target indexes must not be null"));
        targetForeignKeys = List.copyOf(Objects.requireNonNull(targetForeignKeys,
                                                               "target foreign keys must not be null"));
        requests = List.copyOf(Objects.requireNonNull(requests, "migration sql requests must not be null"));
        skippedChanges = List.copyOf(Objects.requireNonNull(skippedChanges, "skipped changes must not be null"));
        additionalCreatedTables = List.copyOf(Objects.requireNonNull(
                additionalCreatedTables, "additional created tables must not be null"));
        for (String table : additionalCreatedTables) {
            if (table.isBlank()) {
                throw new IllegalArgumentException("additional created table must not be blank");
            }
        }
    }

    public boolean requiresManualReview() {
        return !skippedChanges.isEmpty();
    }

    public boolean hasExecutableSql() {
        return !requests.isEmpty();
    }

    public int executableSqlCount() {
        return requests.size();
    }

    public int skippedCount() {
        return skippedChanges.size();
    }

    public List<String> sqlTexts() {
        return requests.stream().map(SqlRequest::sql).toList();
    }

    public List<String> skippedSummaries() {
        return skippedChanges.stream().map(SkippedSchemaChange::summary).toList();
    }
}
