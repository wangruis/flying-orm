package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 规划一个已有列的主键、可空性和存储形态变化。 */
final class SchemaColumnShapeChange {

    private final List<SqlRequest> requests;

    private final List<SkippedSchemaChange> skipped;

    private final Input input;

    private final SchemaDialect dialect;

    private final SchemaTableSqlRenderer tables;

    SchemaColumnShapeChange(List<SqlRequest> requests,
                            List<SkippedSchemaChange> skipped,
                            Input input,
                            SchemaDialect dialect,
                            SchemaTableSqlRenderer tables) {
        this.requests = Objects.requireNonNull(requests, "schema migration requests must not be null");
        this.skipped = Objects.requireNonNull(skipped, "skipped schema changes must not be null");
        this.input = Objects.requireNonNull(input, "schema column change input must not be null");
        this.dialect = Objects.requireNonNull(dialect, "schema dialect must not be null");
        this.tables = Objects.requireNonNull(tables, "schema table renderer must not be null");
    }

    boolean apply() {
        if (input.current().primaryKey() != input.target().primaryKey()) {
            return skipPrimaryKeyChange();
        }
        boolean temporalSemanticsChanged = SchemaMigrationSupport.logicalTemporalTypeChanged(
                input.current().databaseType(), input.target().databaseType());
        if (input.current().nullable() != input.target().nullable()) {
            return applyNullabilityChange(temporalSemanticsChanged);
        }
        return applyStorageChange(temporalSemanticsChanged);
    }

    private boolean skipPrimaryKeyChange() {
        if (!input.primaryKeyChanged()) {
            skipped.add(new SkippedSchemaChange(
                    SkippedSchemaChange.Kind.CHANGE_PRIMARY_KEY,
                    input.target().name(),
                    input.options().primaryKeyChangeAllowed()
                            ? "primary key change is not executable yet"
                            : "SAFE mode does not change primary key columns"));
        }
        return false;
    }

    private boolean applyNullabilityChange(boolean temporalSemanticsChanged) {
        if (!SchemaMigrationSupport.sameStorageShape(input.current(), input.target(), tables)
                || temporalSemanticsChanged) {
            skipped.add(new SkippedSchemaChange(
                    SkippedSchemaChange.Kind.CHANGE_COLUMN,
                    input.target().name(),
                    "nullable and other column attributes changed together; split and review the migration"));
            return false;
        }
        if (!input.target().nullable() && !input.options().columnChangeAllowed()) {
            skipped.add(new SkippedSchemaChange(
                    SkippedSchemaChange.Kind.CHANGE_COLUMN,
                    input.target().name(),
                    "SAFE mode does not make a nullable column NOT NULL",
                    Map.of("column", input.target().name(),
                           "currentNullable", true,
                           "targetNullable", false),
                    List.of("check how many existing rows contain null",
                            "backfill or reject those rows before changing the constraint",
                            "review and approve the exact migration plan")));
            return false;
        }
        requests.add(new SqlRequest(dialect.alterColumnNullabilitySql(
                input.table(),
                input.target().name(),
                tables.dataType(input.target()),
                tables.columnDefinition(input.target()),
                input.target().nullable()), List.of()));
        return true;
    }

    private boolean applyStorageChange(boolean temporalSemanticsChanged) {
        if (ProtectedSchemaTarget.sameProtectedStorage(input.current(), input.target())) {
            return true;
        }
        boolean storageShapeChanged = !SchemaMigrationSupport.sameColumnShape(
                input.current(), input.target(), tables);
        if (!storageShapeChanged && !temporalSemanticsChanged) {
            return true;
        }
        if (storageShapeChanged && !temporalSemanticsChanged
                && SchemaMigrationSupport.safeWidening(input.current(), input.target(), tables)) {
            addTypeChange();
            return true;
        }
        if (!input.options().columnChangeAllowed()) {
            skipped.add(new SkippedSchemaChange(
                    SkippedSchemaChange.Kind.CHANGE_COLUMN,
                    input.target().name(),
                    temporalSemanticsChanged
                            ? "SAFE mode does not reinterpret existing values as a different time type"
                            : "SAFE mode does not change column type, length, precision, or scale"));
            return false;
        }
        if (!SchemaGeneratedValueComparison.same(input.current(), input.target())) {
            skipped.add(new SkippedSchemaChange(
                    SkippedSchemaChange.Kind.CHANGE_COLUMN,
                    input.target().name(),
                    "generated value changes require a reviewed migration"));
            return false;
        }
        if (storageShapeChanged) {
            addTypeChange();
        }
        return true;
    }

    private void addTypeChange() {
        requests.add(new SqlRequest(dialect.alterColumnTypeSql(
                input.table(),
                input.target().name(),
                tables.dataType(input.target()),
                tables.columnDefinition(input.target())), List.of()));
    }

    /** 一个已有列形态变化的稳定输入。 */
    record Input(String table,
                 ColumnMetadata current,
                 DynamicField target,
                 SchemaMigrationOptions options,
                 boolean primaryKeyChanged) {

        Input {
            table = Objects.requireNonNull(table, "schema migration table must not be null");
            current = Objects.requireNonNull(current, "current column metadata must not be null");
            target = Objects.requireNonNull(target, "target dynamic field must not be null");
            options = Objects.requireNonNull(options, "schema migration options must not be null");
        }
    }
}
