package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 把受保护字段的辅助表作为独立迁移段接入主表计划。
 *
 * <p>辅助表必须单独读取元数据，才能区分首次启用与重复迁移；主表与辅助表的真实变化一起进入执行和失效。本类型只在 Schema 包内协作，不扩展公开迁移 API。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class ProtectedSchemaMigrationPlanner {

    private final SchemaMigrationPlanner planner;
    ProtectedSchemaMigrationPlanner(SchemaMigrationPlanner planner) {
        this.planner = Objects.requireNonNull(planner, "schema migration planner must not be null");
    }

    SchemaMigrationPlan appendCreatePlan(DynamicForm logical, SchemaMigrationPlan primary,
                                         SchemaMigrationOptions options) {
        return ProtectedContainsLayout.resolve(logical)
                                      .map(layout -> combine(primary,
                                              planner.createTablePlan(layout.table(), layout.indexes(),
                                                      layout.foreignKeys(), options)))
                                      .orElse(primary);
    }

    Mono<SchemaMigrationPlan> planExistingReactive(DynamicForm logical,
                                                   SchemaMigrationPlan primary,
                                                   ReactiveFormMetadataReader reader,
                                                   SchemaMigrationOptions options) {
        return ProtectedContainsLayout.resolve(logical)
                                      .map(layout -> planner.plan(layout.table(), layout.indexes(),
                                                      layout.foreignKeys(), reader, options)
                                              .map(side -> combine(primary, side)))
                                      .orElseGet(() -> Mono.just(primary));
    }

    SchemaMigrationPlan planExistingJdbc(DynamicForm logical,
                                         SchemaMigrationPlan primary,
                                         JdbcFormMetadataReader lookup,
                                         SchemaMigrationOptions options) {
        return ProtectedContainsLayout.resolve(logical)
                                      .map(layout -> combine(primary, planner.planJdbc(layout.table(),
                                              layout.indexes(), layout.foreignKeys(), lookup, options)))
                                      .orElse(primary);
    }

    private static SchemaMigrationPlan combine(SchemaMigrationPlan primary, SchemaMigrationPlan side) {
        List<SkippedSchemaChange> skipped = new ArrayList<>(primary.skippedChanges());
        skipped.addAll(side.skippedChanges());
        List<RelationIdentity> additionalTables = new ArrayList<>(primary.additionalCreatedTables());
        additionalTables.addAll(side.additionalCreatedTables());
        RelationIdentity sideTable = side.target().identity();
        if ((!side.tableExists() || side.hasExecutableSql()) && !additionalTables.contains(sideTable)) {
            additionalTables.add(sideTable);
        }
        if (primary.steps().isEmpty() && primary.hasExecutableSql()
                || side.steps().isEmpty() && side.hasExecutableSql()) {
            // 整计划 SQL 扩展没有逐操作对应关系，不能为替换后的请求发布旧操作明细。
            List<SqlRequest> requests = new ArrayList<>(primary.requests());
            requests.addAll(side.requests());
            return new SchemaMigrationPlan(primary.target(), primary.tableExists(), requests, skipped, additionalTables);
        }
        List<SchemaPlanStep> steps = new ArrayList<>(primary.steps());
        steps.addAll(side.steps());
        return SchemaMigrationPlan.planned(primary.target(), primary.tableExists(), steps, skipped, additionalTables);
    }

}
