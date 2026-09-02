package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.sql.render.SqlRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 负责把表、字段和索引变成 DDL 片段。
 *
 * <p>这里不比较新旧表结构，也不决定哪些迁移可以执行。这样表结构拼装和迁移判断各自只做一件事，
 * 以后增加字段类型或索引写法时，不会把迁移计划代码一起搅乱。</p>
 */
final class SchemaTableSqlRenderer {

    private static final String TIME_MARKER = "[[flying-orm:v1:TIME]]";
    private static final String OFFSET_TIME_MARKER = "[[flying-orm:v1:OFFSET_TIME]]";
    private static final String COMMENT_ESCAPE = "[[flying-orm:v1:COMMENT]]";

    private final SchemaDialect dialect;
    private final SchemaSequenceSqlRenderer sequences;

    SchemaTableSqlRenderer(SchemaDialect dialect) {
        this.dialect = Objects.requireNonNull(dialect, "schema dialect must not be null");
        this.sequences = new SchemaSequenceSqlRenderer(dialect, this::dataType);
    }

    List<SqlRequest> createTable(DynamicForm form) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        validateGeneratedColumns(safeForm);
        List<DynamicField> primaryKeys = safeForm.fields().stream()
                                                 .filter(DynamicField::primaryKey)
                                                 .toList();
        boolean compositePrimaryKey = primaryKeys.size() > 1;
        String columns = safeForm.fields().stream()
                                 .map(field -> columnDefinition(field, !compositePrimaryKey))
                                 .collect(Collectors.joining(", "));
        if (compositePrimaryKey) {
            String primaryKeyColumns = primaryKeys.stream()
                                                  .map(DynamicField::name)
                                                  .map(this::identifier)
                                                  .collect(Collectors.joining(", "));
            columns += ", primary key (" + primaryKeyColumns + ")";
        }
        List<SqlRequest> requests = new ArrayList<>();
        addSequenceCreates(requests, safeForm.fields(), List.of(), false);
        ValueGeneration tableGeneration = safeForm.fields().stream()
                                                  .map(DynamicField::generation)
                                                  .filter(generation -> generation.strategy()
                                                          == ValueGeneration.Strategy.IDENTITY)
                                                  .findFirst()
                                                  .orElse(ValueGeneration.none());
        requests.add(new SqlRequest("create table " + identifier(safeForm.table()) + " (" + columns + ")"
                                            + dialect.generatedTableClause(tableGeneration),
                                    List.of()));
        for (DynamicField field : safeForm.fields()) {
            addColumnComment(requests, safeForm.table(), field);
        }
        return List.copyOf(requests);
    }

    List<SqlRequest> createIndexes(String table, List<IndexMetadata> indexes) {
        String rawTable = Objects.requireNonNull(table, "table must not be null").trim();
        String safeTable = identifier(rawTable);
        return Objects.requireNonNull(indexes, "indexes must not be null")
                      .stream()
                      .map(index -> createIndex(rawTable, safeTable, index))
                      .toList();
    }

    /** 为 ORM 自有侧表创建经过结构化元数据约束的级联删除外键。 */
    SqlRequest createCascadeForeignKey(String table, ForeignKeyMetadata foreignKey) {
        ForeignKeyMetadata safe = Objects.requireNonNull(foreignKey, "foreign key metadata must not be null");
        String columns = safe.columns().stream().map(this::identifier).collect(Collectors.joining(", "));
        String references = safe.referenceColumns().stream()
                                .map(this::identifier)
                                .collect(Collectors.joining(", "));
        return new SqlRequest("alter table " + identifier(table)
                                      + " add constraint " + identifier(safe.name())
                                      + " foreign key (" + columns + ") references "
                                      + identifier(safe.referenceTable()) + " (" + references + ")"
                                      + " on delete cascade",
                              List.of());
    }

    String columnDefinition(DynamicField field) {
        return columnDefinition(field, true);
    }

    private String columnDefinition(DynamicField field, boolean inlinePrimaryKey) {
        DynamicField safeField = Objects.requireNonNull(field, "dynamic field must not be null");
        String type = dataType(safeField);
        String sql = identifier(safeField.name()) + " " + type;
        sql += dialect.generatedValueClause(safeField.generation(), type);
        if (safeField.primaryKey() && inlinePrimaryKey) {
            if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
                sql += " not null";
            }
            sql += " primary key";
        } else if (!safeField.nullable() || safeField.primaryKey()) {
            sql += " not null";
        } else if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            // SQL Server 省略可空性会受会话和数据库 ANSI_NULL_DFLT 设置影响，必须显式固定物理契约。
            sql += " null";
        }
        String storageComment = storageComment(safeField);
        if (dialect.inlineColumnComment() && storageComment != null) {
            sql += " comment " + dialect.quoteLiteral(storageComment);
        }
        return sql;
    }

    private void validateGeneratedColumns(DynamicForm form) {
        List<DynamicField> identityFields = form.fields().stream()
                                                .filter(field -> field.generation().strategy()
                                                        == ValueGeneration.Strategy.IDENTITY)
                                                .toList();
        switch (dialect.generatedValueStyle()) {
            case MYSQL -> validateMysqlIdentity(form, identityFields);
            case ORACLE -> requireSingleIdentity(identityFields, "oracle");
            case SQL_SERVER -> requireSingleIdentity(identityFields, "sql server");
            default -> {
                // H2/PostgreSQL 的合法数量由数据库语法和字段级类型校验决定。
            }
        }
    }

    private static void validateMysqlIdentity(DynamicForm form, List<DynamicField> identityFields) {
        requireSingleIdentity(identityFields, "mysql");
        if (identityFields.isEmpty()) {
            return;
        }
        DynamicField identity = identityFields.getFirst();
        if (!identity.primaryKey()) {
            throw new IllegalArgumentException("mysql identity column must be the table primary key");
        }
        DynamicField firstPrimaryKey = form.fields().stream()
                                           .filter(DynamicField::primaryKey)
                                           .findFirst()
                                           .orElseThrow();
        if (!firstPrimaryKey.name().equals(identity.name())) {
            throw new IllegalArgumentException("mysql identity column must be the first column of an index");
        }
    }

    private static void requireSingleIdentity(List<DynamicField> identityFields, String database) {
        if (identityFields.size() > 1) {
            throw new IllegalArgumentException(database + " table must not declare more than one identity column");
        }
    }

    String identifier(String value) {
        return dialect.identifier(value);
    }

    String dataType(DynamicField field) {
        DynamicField safeField = Objects.requireNonNull(field, "dynamic field must not be null");
        return dialect.dataType(safeField.dataType(),
                                safeField.length(),
                                safeField.precision(),
                                safeField.scale());
    }

    /** 元数据把类型参数拆成独立属性；比较迁移结构时仍须用与目标字段相同的方言渲染规则还原。 */
    String dataType(ColumnMetadata column) {
        ColumnMetadata safeColumn = Objects.requireNonNull(column, "column metadata must not be null");
        return dialect.dataType(safeColumn.dataType(),
                                safeColumn.length(),
                                safeColumn.precision(),
                                safeColumn.scale());
    }

    boolean sameDataType(String left, String right) {
        return dialect.sameDataType(left, right);
    }

    void addSequenceCreates(List<SqlRequest> requests,
                            List<DynamicField> fields,
                            List<DynamicField> existingFields,
                            boolean verifyExistingDefinitions) {
        sequences.addCreates(requests, fields, existingFields, verifyExistingDefinitions);
    }

    void addSequenceCreate(List<SqlRequest> requests, DynamicField field) {
        sequences.addCreate(requests, field);
    }

    List<SqlRequest> dropSequences(List<DynamicField> fields, List<DynamicField> retainedFields) {
        return sequences.drops(fields, retainedFields);
    }

    SqlRequest createIndex(String table, IndexMetadata index) {
        String rawTable = Objects.requireNonNull(table, "table must not be null").trim();
        return createIndex(rawTable, identifier(rawTable), index);
    }

    private SqlRequest createIndex(String rawTable, String table, IndexMetadata index) {
        IndexMetadata safeIndex = Objects.requireNonNull(index, "index metadata must not be null");
        if (safeIndex.columns().isEmpty()) {
            throw new IllegalArgumentException("index columns must not be empty: " + safeIndex.name());
        }
        String indexName = requireUnqualifiedIdentifier(safeIndex.name(), "index name");
        String unique = safeIndex.unique() ? "unique " : "";
        String columns = safeIndex.columns().stream()
                                  .map(column -> requireUnqualifiedIdentifier(column, "index column"))
                                  .map(this::identifier)
                                  .collect(Collectors.joining(", "));
        return new SqlRequest("create " + unique + "index " + createIndexIdentifier(rawTable, indexName)
                                      + " on " + table + " (" + columns + ")",
                              List.of());
    }

    /** Oracle 的索引属于 schema 而不是表；显式 schema 必须同时限定索引名，和 DROP INDEX 保持对称。 */
    private String createIndexIdentifier(String table, String index) {
        if (dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.ORACLE) {
            return identifier(index);
        }
        int separator = table.lastIndexOf('.');
        return separator < 0 ? identifier(index) : identifier(table.substring(0, separator + 1) + index);
    }

    private static String requireUnqualifiedIdentifier(String value, String category) {
        String identifier = SqlIdentifiers.requireIdentifier(value, category);
        if (identifier.indexOf('.') >= 0) {
            throw new IllegalArgumentException(category + " must not be qualified");
        }
        return identifier;
    }

    SqlRequest dropIndex(String table, IndexMetadata index) {
        IndexMetadata safeIndex = Objects.requireNonNull(index, "index metadata must not be null");
        return new SqlRequest(dialect.dropIndexSql(table, safeIndex.name()), List.of());
    }

    void addMissingComment(List<SqlRequest> requests,
                           String table,
                           com.flying.orm.core.metadata.ColumnMetadata column,
                           DynamicField target) {
        validateCommentChange(table, column, target);
        String previousComment = storageComment(column.databaseType(), column.comment());
        String targetComment = storageComment(target);
        if (Objects.equals(previousComment, targetComment)) {
            return;
        }
        var separateChange = dialect.columnCommentChangeSql(table,
                                                            target.name(),
                                                            previousComment,
                                                            targetComment);
        if (separateChange.isPresent()) {
            requests.add(new SqlRequest(separateChange.orElseThrow(), List.of()));
            return;
        }
        if (!dialect.inlineColumnComment() || !dialect.rewritesFullColumnDefinition()) {
            throw new IllegalArgumentException("the configured dialect cannot alter the column comment safely");
        }
        if (SchemaMigrationSupport.sameColumnShape(column, target, this)) {
            requests.add(new SqlRequest(dialect.alterColumnTypeSql(table,
                                                                    target.name(),
                                                                    dataType(target),
                                                                    columnDefinition(target)),
                                        List.of()));
        }
    }

    void validateCommentChange(String table,
                               com.flying.orm.core.metadata.ColumnMetadata column,
                               DynamicField target) {
        String previousComment = storageComment(column.databaseType(), column.comment());
        String targetComment = storageComment(target);
        if (Objects.equals(previousComment, targetComment)) {
            return;
        }
        boolean separateChangeSupported = dialect.columnCommentChangeSql(table,
                                                                         target.name(),
                                                                         previousComment,
                                                                         targetComment)
                                                 .isPresent();
        if (!separateChangeSupported
                && (!dialect.inlineColumnComment() || !dialect.rewritesFullColumnDefinition())) {
            throw new IllegalArgumentException("the configured dialect cannot alter the column comment safely");
        }
    }

    void addColumnComment(List<SqlRequest> requests, String table, DynamicField field) {
        validateNewColumnComment(table, field);
        dialect.columnCommentSql(table, field.name(), storageComment(field))
               .ifPresent(sql -> requests.add(new SqlRequest(sql, List.of())));
    }

    void validateNewColumnComment(String table, DynamicField field) {
        String comment = storageComment(field);
        if (comment == null || dialect.inlineColumnComment()) {
            return;
        }
        if (dialect.columnCommentSql(table, field.name(), comment).isEmpty()) {
            throw new IllegalArgumentException("the configured dialect cannot create the column comment safely");
        }
    }

    String storageComment(DynamicField field) {
        DynamicField safeField = Objects.requireNonNull(field, "dynamic field must not be null");
        return storageComment(safeField.databaseType(), safeField.comment());
    }

    String storageComment(com.flying.orm.core.metadata.ColumnMetadata column) {
        com.flying.orm.core.metadata.ColumnMetadata safeColumn = Objects.requireNonNull(
                column, "column metadata must not be null");
        return storageComment(safeColumn.databaseType(), safeColumn.comment());
    }

    private String storageComment(DatabaseType dataType, String comment) {
        SchemaDialect.GeneratedValueStyle style = dialect.generatedValueStyle();
        boolean offsetTime = dataType.logicalType() == LogicalType.OFFSET_TIME;
        if (style != SchemaDialect.GeneratedValueStyle.ORACLE
                && style != SchemaDialect.GeneratedValueStyle.MYSQL
                && style != SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            return comment;
        }
        String marker;
        if (offsetTime) {
            marker = OFFSET_TIME_MARKER;
        } else if (style == SchemaDialect.GeneratedValueStyle.ORACLE
                && dataType.logicalType() == LogicalType.TIME) {
            marker = TIME_MARKER;
        } else {
            return escapeReservedComment(comment);
        }
        if (comment == null || comment.isEmpty()) {
            return marker;
        }
        return marker + comment;
    }

    /** 普通列可使用任意用户注释；与内部协议同前缀时写入转义标记，读取后仍恢复原文。 */
    private static String escapeReservedComment(String comment) {
        if (comment == null || comment.isEmpty()) {
            return comment;
        }
        return comment.startsWith(TIME_MARKER)
                || comment.startsWith(OFFSET_TIME_MARKER)
                || comment.startsWith(COMMENT_ESCAPE)
                ? COMMENT_ESCAPE + comment
                : comment;
    }

    void addColumnCommentChange(List<SqlRequest> requests,
                                String table,
                                String column,
                                String previousComment,
                                String targetComment) {
        dialect.columnCommentChangeSql(table, column, previousComment, targetComment)
               .ifPresent(sql -> requests.add(new SqlRequest(sql, List.of())));
    }
}
