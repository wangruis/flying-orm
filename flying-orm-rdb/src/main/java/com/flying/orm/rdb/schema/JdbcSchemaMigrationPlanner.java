package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * JDBC Schema 的薄规划入口。
 *
 * <p>它只负责调用同步 metadata reader，并把“表不存在”转成建表计划；真正的结构比较、
 * 风险判断和 SQL 生成仍由 {@link SchemaMigrationPlanner} 完成，避免两条执行轨道出现分叉。</p>
 */
final class JdbcSchemaMigrationPlanner {

    private final SchemaMigrationPlanner planner;
    private final ProtectedSchemaMigrationPlanner protectedSchemas;
    private final FormSchemaSqlRenderer renderer;

    JdbcSchemaMigrationPlanner(FormSchemaSqlRenderer renderer) {
        this.renderer = Objects.requireNonNull(renderer, "form schema SQL renderer must not be null");
        this.planner = new SchemaMigrationPlanner(this.renderer);
        this.protectedSchemas = new ProtectedSchemaMigrationPlanner(this.planner, this.renderer);
    }

    SchemaMigrationPlan plan(DynamicForm form,
                             List<IndexMetadata> indexes,
                             List<ForeignKeyMetadata> foreignKeys,
                             JdbcFormMetadataReader metadataReader,
                             SchemaMigrationOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "target dynamic form must not be null");
        JdbcFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "jdbc form metadata reader must not be null");
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(options,
                                                                     "schema migration options must not be null");
        return plan(safeForm, indexes, foreignKeys, safeReader::readTable, safeOptions);
    }

    SchemaMigrationPlan plan(DynamicForm form,
                             List<IndexMetadata> indexes,
                             List<ForeignKeyMetadata> foreignKeys,
                             Function<String, TableMetadata> metadataLookup,
                             SchemaMigrationOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "target dynamic form must not be null");
        Function<String, TableMetadata> safeLookup = Objects.requireNonNull(
                metadataLookup, "schema metadata lookup must not be null");
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(options,
                                                                     "schema migration options must not be null");
        try {
            TableMetadata current = safeLookup.apply(safeForm.table());
            SchemaMigrationPlan primary = planner.migrateSafelyPlan(
                    current, safeForm, indexes, foreignKeys, safeOptions);
            return protectedSchemas.planExistingJdbc(safeForm, primary, safeLookup, safeOptions);
        } catch (IllegalArgumentException failure) {
            if (!isTableNotFound(failure)) {
                throw failure;
            }
            return planner.createTablePlan(safeForm, indexes, foreignKeys);
        }
    }

    ReviewedSchemaMigrationPlan review(DynamicForm form,
                                       List<IndexMetadata> indexes,
                                       List<ForeignKeyMetadata> foreignKeys,
                                       JdbcFormMetadataReader metadataReader,
                                       SchemaMigrationOptions migrationOptions,
                                       SchemaMigrationReviewPolicy reviewPolicy) {
        DynamicForm safeForm = Objects.requireNonNull(form, "target dynamic form must not be null");
        JdbcFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "jdbc form metadata reader must not be null");
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(migrationOptions,
                                                                     "schema migration options must not be null");
        SchemaMigrationReviewPolicy safePolicy = Objects.requireNonNull(reviewPolicy,
                                                                         "migration review policy must not be null");
        try {
            TableMetadata current = safeReader.readTable(safeForm.table());
            SchemaMigrationPlan primary = planner.migrateSafelyPlan(
                    current, safeForm, indexes, foreignKeys, safeOptions);
            return protectedSchemas.reviewExistingJdbc(
                    safeForm, current, primary, safeReader::readTable, safeOptions, safePolicy);
        } catch (IllegalArgumentException failure) {
            if (!isTableNotFound(failure)) {
                throw failure;
            }
            return protectedSchemas.reviewCreated(
                    safeForm,
                    planner.createPrimaryTablePlan(safeForm, indexes, foreignKeys),
                    safePolicy);
        }
    }

    private static boolean isTableNotFound(Throwable error) {
        if (!(error instanceof IllegalArgumentException) || error.getMessage() == null) {
            return false;
        }
        String message = error.getMessage();
        return message.equals("table metadata not found") || message.startsWith("table metadata not found:");
    }
}
