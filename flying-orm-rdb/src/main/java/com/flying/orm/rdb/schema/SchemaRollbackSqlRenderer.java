package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** 生成审核器需要的反向 SQL；反向计划同样复用主渲染器的标识符和类型安全规则。 */
final class SchemaRollbackSqlRenderer {

    private final SchemaDialect dialect;
    private final SchemaTableSqlRenderer tables;

    SchemaRollbackSqlRenderer(SchemaDialect dialect, SchemaTableSqlRenderer tables) {
        this.dialect = Objects.requireNonNull(dialect, "schema dialect must not be null");
        this.tables = Objects.requireNonNull(tables, "table renderer must not be null");
    }

    SqlRequest rollbackDropTable(String table) {
        return new SqlRequest("drop table " + tables.identifier(table), List.of());
    }

    SqlRequest rollbackAddColumn(String table, ColumnMetadata column) {
        return new SqlRequest(dialect.addColumnSql(
                table, tables.columnDefinition(SchemaMigrationSupport.toDynamicField(column))), List.of());
    }

    SqlRequest rollbackDropColumn(String table, String column) {
        return new SqlRequest("alter table " + tables.identifier(table)
                                      + " drop column " + tables.identifier(column), List.of());
    }

    SqlRequest rollbackColumnType(String table, String currentColumn, ColumnMetadata column) {
        DynamicField field = toField(column, currentColumn);
        return new SqlRequest(dialect.alterColumnTypeSql(table,
                                                         currentColumn,
                                                         tables.dataType(field),
                                                         tables.columnDefinition(field)),
                              List.of());
    }

    /** 按迁移前的完整字段定义恢复 nullable，MySQL 因此不会在回滚时丢掉注释或生成值属性。 */
    SqlRequest rollbackColumnNullability(String table, String currentColumn, ColumnMetadata column) {
        DynamicField field = toField(column, currentColumn);
        return new SqlRequest(dialect.alterColumnNullabilitySql(table,
                                                                 currentColumn,
                                                                 tables.dataType(field),
                                                                 tables.columnDefinition(field),
                                                                 column.nullable()),
                              List.of());
    }

    Optional<SqlRequest> rollbackColumnComment(String table,
                                               String currentColumn,
                                               ColumnMetadata source,
                                               DynamicField target) {
        return dialect.columnCommentChangeSql(table,
                                              currentColumn,
                                              tables.storageComment(target),
                                              tables.storageComment(source))
                      .map(sql -> new SqlRequest(sql, List.of()));
    }

    SqlRequest rollbackRenameColumn(String table, String currentName, String previousName) {
        return new SqlRequest(dialect.renameColumnSql(table, currentName, previousName), List.of());
    }

    SqlRequest rollbackDropIndex(String table, IndexMetadata index) {
        return tables.dropIndex(table, index);
    }

    SqlRequest rollbackCreateIndex(String table, IndexMetadata index) {
        return tables.createIndex(table, index);
    }

    private static DynamicField toField(ColumnMetadata column, String name) {
        return new DynamicField(name,
                                column.dataType(),
                                column.primaryKey(),
                                column.nullable(),
                                false,
                                column.length(),
                                column.precision(),
                                column.scale(),
                                column.comment(),
                                column.generation());
    }
}
