package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.internal.template.SqlLexicalScanner;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * 负责字段注释、索引、字段改名和字段类型变更等 DDL 语法差异。
 *
 * <p>数据库版本边界仍由上层配置的样式枚举表达；这里不会根据数据库名字猜语法，也不会把不确定的
 * 在线 DDL 当成通用能力。</p>
 */
final class SchemaDialectDdlSupport {

    private final SchemaDialectTypeSupport types;
    private final SchemaDialect.ColumnCommentStyle columnCommentStyle;
    private final SchemaDialect.TableCommentStyle tableCommentStyle;
    private final SchemaDialect.DropIndexStyle dropIndexStyle;
    private final SchemaDialect.RenameColumnStyle renameColumnStyle;
    private final SchemaDialect.GeneratedValueStyle databaseStyle;
    private final SchemaDialect.ColumnChangeStyle columnChangeStyle;
    private final boolean concurrentIndexesSupported;
    private final SqlLexicalScanner.Rules lexicalRules;

    SchemaDialectDdlSupport(SchemaDialectTypeSupport types,
                            SchemaDialect.ColumnCommentStyle columnCommentStyle,
                            SchemaDialect.TableCommentStyle tableCommentStyle,
                            SchemaDialect.DropIndexStyle dropIndexStyle,
                            SchemaDialect.RenameColumnStyle renameColumnStyle,
                            SchemaDialect.GeneratedValueStyle databaseStyle,
                            SchemaDialect.ColumnChangeStyle columnChangeStyle,
                            boolean concurrentIndexesSupported) {
        this.types = Objects.requireNonNull(types, "type support must not be null");
        this.columnCommentStyle = Objects.requireNonNull(columnCommentStyle, "column comment style must not be null");
        this.tableCommentStyle = Objects.requireNonNull(tableCommentStyle, "table comment style must not be null");
        this.dropIndexStyle = Objects.requireNonNull(dropIndexStyle, "drop index style must not be null");
        this.renameColumnStyle = Objects.requireNonNull(renameColumnStyle, "rename column style must not be null");
        this.databaseStyle = Objects.requireNonNull(databaseStyle, "database DDL style must not be null");
        this.lexicalRules = SqlLexicalScanner.rulesFor(
                databaseStyle == SchemaDialect.GeneratedValueStyle.SQL_SERVER ? "sqlserver" : databaseStyle.name());
        this.columnChangeStyle = Objects.requireNonNull(columnChangeStyle, "column change style must not be null");
        this.concurrentIndexesSupported = concurrentIndexesSupported;
    }

    boolean inlineColumnComment() {
        return columnCommentStyle == SchemaDialect.ColumnCommentStyle.INLINE;
    }

    String commentLiteral(String comment) {
        return MySqlSchemaCommentSupport.literal(
                types, comment, tableCommentStyle == SchemaDialect.TableCommentStyle.MYSQL_OPTION);
    }

    String inlineTableCommentClause(String comment) {
        if (comment == null || tableCommentStyle != SchemaDialect.TableCommentStyle.MYSQL_OPTION) {
            return "";
        }
        return " comment = " + commentLiteral(comment);
    }

    Optional<String> tableCommentSql(RelationIdentity table, String comment) {
        if (comment == null
                || tableCommentStyle == SchemaDialect.TableCommentStyle.NONE
                || tableCommentStyle == SchemaDialect.TableCommentStyle.MYSQL_OPTION) {
            return Optional.empty();
        }
        if (tableCommentStyle == SchemaDialect.TableCommentStyle.SQL_SERVER_EXTENDED_PROPERTY) {
            return Optional.of(sqlServerComment(table, null, comment, "sp_addextendedproperty"));
        }
        return Optional.of("comment on table " + types.identifier(table)
                                   + " is " + types.quoteLiteral(comment));
    }


    Optional<String> columnCommentSql(RelationIdentity table, String column, String comment) {
        if (comment == null || columnCommentStyle == SchemaDialect.ColumnCommentStyle.NONE
                || columnCommentStyle == SchemaDialect.ColumnCommentStyle.INLINE) {
            return Optional.empty();
        }
        if (columnCommentStyle == SchemaDialect.ColumnCommentStyle.SQL_SERVER_EXTENDED_PROPERTY) {
            return Optional.of(sqlServerComment(table, Objects.requireNonNull(column, "column must not be null"),
                    comment, "sp_addextendedproperty"));
        }
        return Optional.of("comment on column " + types.identifier(table) + "." + types.identifier(column)
                                   + " is " + types.quoteLiteral(comment));
    }


    Optional<String> columnCommentChangeSql(RelationIdentity table,
                                            String column,
                                            String previousComment,
                                            String targetComment) {
        if (Objects.equals(previousComment, targetComment)
                || columnCommentStyle == SchemaDialect.ColumnCommentStyle.NONE
                || columnCommentStyle == SchemaDialect.ColumnCommentStyle.INLINE
                && databaseStyle != SchemaDialect.GeneratedValueStyle.H2) {
            return Optional.empty();
        }
        if (columnCommentStyle == SchemaDialect.ColumnCommentStyle.SQL_SERVER_EXTENDED_PROPERTY) {
            String procedure = previousComment == null ? "sp_addextendedproperty"
                    : targetComment == null ? "sp_dropextendedproperty" : "sp_updateextendedproperty";
            return Optional.of(sqlServerComment(table, Objects.requireNonNull(column, "column must not be null"),
                    targetComment, procedure));
        }
        String value = targetComment == null
                ? databaseStyle == SchemaDialect.GeneratedValueStyle.ORACLE ? "''" : "null"
                : types.quoteLiteral(targetComment);
        return Optional.of("comment on column " + types.identifier(table) + "." + types.identifier(column)
                                   + " is " + value);
    }


    String dropIndexSql(RelationIdentity table, String index) {
        String indexName = requireUnqualifiedIdentifier(index, "drop index name");
        String renderedIndex = dropIndexStyle == SchemaDialect.DropIndexStyle.NAME_ONLY
                ? types.namespaceObjectIdentifier(table, indexName)
                : types.identifier(indexName);
        String sql = "drop index " + renderedIndex;
        return dropIndexStyle == SchemaDialect.DropIndexStyle.ON_TABLE
                ? sql + " on " + types.identifier(table)
                : sql;
    }

    String dropConstraintSql(RelationIdentity table, String constraint) {
        return "alter table " + types.identifier(table)
                + " drop constraint " + types.identifier(constraint);
    }

    String dropConstraintSql(RelationIdentity table, String constraint, SchemaOperation.Kind kind) {
        if (databaseStyle != SchemaDialect.GeneratedValueStyle.MYSQL) {
            return dropConstraintSql(table, constraint);
        }
        String action = switch (kind) {
            case DROP_PRIMARY_KEY -> "drop primary key";
            case DROP_UNIQUE -> "drop index " + types.identifier(constraint);
            case DROP_CHECK -> "drop check " + types.identifier(constraint);
            case DROP_FOREIGN_KEY -> "drop foreign key " + types.identifier(constraint);
            default -> throw new IllegalArgumentException("operation does not drop a constraint");
        };
        return "alter table " + types.identifier(table) + ' ' + action;
    }

    String changeConstraintSql(RelationIdentity table,
                               String actualConstraint,
                               String desiredDefinition,
                               SchemaOperation.Kind kind) {
        SchemaOperation.Kind drop = switch (kind) {
            case CHANGE_PRIMARY_KEY -> SchemaOperation.Kind.DROP_PRIMARY_KEY;
            case CHANGE_UNIQUE -> SchemaOperation.Kind.DROP_UNIQUE;
            case CHANGE_CHECK -> SchemaOperation.Kind.DROP_CHECK;
            case CHANGE_FOREIGN_KEY -> SchemaOperation.Kind.DROP_FOREIGN_KEY;
            default -> throw new IllegalArgumentException("operation does not change a constraint");
        };
        return dropConstraintSql(table, actualConstraint, drop) + ", add "
                + SchemaDialectTypeSupport.requireText(
                        desiredDefinition, "desired constraint definition");
    }

    String dropColumnSql(RelationIdentity table, String column) {
        return "alter table " + types.identifier(table)
                + " drop column " + types.identifier(column);
    }

    String renameConstraintSql(RelationIdentity table, String actual, String desired) {
        if (databaseStyle != SchemaDialect.GeneratedValueStyle.H2
                && databaseStyle != SchemaDialect.GeneratedValueStyle.ORACLE) {
            throw new UnsupportedOperationException("current dialect cannot rename a relational constraint");
        }
        return "alter table " + types.identifier(table) + " rename constraint " + types.identifier(actual)
                + " to " + types.identifier(desired);
    }

    String renameIndexSql(RelationIdentity table, String actual, String desired) {
        if (databaseStyle != SchemaDialect.GeneratedValueStyle.H2
                && databaseStyle != SchemaDialect.GeneratedValueStyle.POSTGRESQL
                && databaseStyle != SchemaDialect.GeneratedValueStyle.ORACLE) {
            throw new UnsupportedOperationException("current dialect cannot rename a relational index");
        }
        return "alter index " + types.namespaceObjectIdentifier(table, actual)
                + " rename to " + types.identifier(desired);
    }

    String dropTableSql(RelationIdentity table) {
        return "drop table " + types.identifier(table);
    }

    String alterColumnDefaultSql(RelationIdentity table,
                                 String column,
                                 String defaultExpression) {
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.ORACLE) {
            return "alter table " + types.identifier(table) + " modify (" + types.identifier(column)
                    + " default " + (defaultExpression == null ? "null" : defaultExpression) + ')';
        }
        String action = defaultExpression == null
                ? "drop default"
                : "set default " + defaultExpression;
        return "alter table " + types.identifier(table)
                + " alter column " + types.identifier(column) + ' ' + action;
    }

    SqlRequest preferConcurrentIndex(SqlRequest request) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "schema SQL request must not be null");
        if (!concurrentIndexesSupported) {
            return safeRequest;
        }
        String sql = safeRequest.sql();
        String lower = sql.toLowerCase(Locale.ROOT);
        if (lower.startsWith("create unique index ")) {
            return new SqlRequest("create unique index concurrently "
                                          + sql.substring("create unique index ".length()),
                                  safeRequest.parameters(), safeRequest.bindMarkerStyle());
        }
        if (lower.startsWith("create index ")) {
            return new SqlRequest("create index concurrently "
                                          + sql.substring("create index ".length()),
                                  safeRequest.parameters(), safeRequest.bindMarkerStyle());
        }
        return safeRequest;
    }

    private static String requireUnqualifiedIdentifier(String value, String category) {
        String identifier = com.flying.orm.core.sql.render.SqlIdentifiers.requireIdentifier(value);
        if (identifier.indexOf('.') >= 0) {
            throw new IllegalArgumentException(category + " must not be qualified");
        }
        return identifier;
    }


    String renameColumnSql(RelationIdentity table, String oldName, String newName) {
        String source = requireUnqualifiedIdentifier(oldName, "old column name");
        String target = requireUnqualifiedIdentifier(newName, "new column name");
        String relation = types.identifier(table);
        if (renameColumnStyle == SchemaDialect.RenameColumnStyle.SQL_SERVER_SP_RENAME) {
            return "exec sp_rename N" + types.quoteLiteral(relation + '.' + types.identifier(source))
                    + ", N" + types.quoteLiteral(target) + ", N'COLUMN'";
        }
        return "alter table " + relation + " rename column " + types.identifier(source)
                + " to " + types.identifier(target);
    }


    String addColumnSql(RelationIdentity table, String columnDefinition) {
        String tableName = types.identifier(table);
        String safeDefinition = requireColumnDefinition(columnDefinition);
        return switch (columnChangeStyle) {
            case ORACLE -> "alter table " + tableName + " add (" + safeDefinition + ")";
            case SQL_SERVER -> "alter table " + tableName + " add " + safeDefinition;
            case STANDARD -> "alter table " + tableName + " add column " + safeDefinition;
        };
    }

    String alterColumnTypeSql(RelationIdentity table, String column, String databaseType) {
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.MYSQL) {
            throw incompleteColumnDefinition("mysql");
        }
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            throw incompleteColumnDefinition("sql server");
        }
        return alterColumnTypeSql(table, column, databaseType, null, true, null);
    }

    String alterColumnTypeSql(RelationIdentity table, String column, String databaseType,
                              String columnDefinition, boolean nullable, String collation) {
        String tableName = types.identifier(table);
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.MYSQL) {
            return "alter table " + tableName + " modify column "
                    + requireColumnDefinition(columnDefinition);
        }
        String safeType = SchemaDialectTypeSupport.requireDataType(databaseType, "alter column data type");
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            return "alter table " + tableName + " alter column " + types.identifier(column)
                    + ' ' + safeType + (collation == null ? "" : " collate "
                        + SchemaDialectTypeSupport.sqlServerCollation(collation))
                    + (nullable ? " null" : " not null");
        }
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.H2) {
            return "alter table " + tableName + " alter column " + types.identifier(column)
                    + " set data type " + safeType;
        }
        return alterColumnTypeForRenderedTable(tableName, column, safeType);
    }

    private String alterColumnTypeForRenderedTable(String table, String column, String databaseType) {
        return switch (columnChangeStyle) {
            case ORACLE -> "alter table " + table + " modify ("
                    + types.identifier(column) + " " + databaseType + ")";
            case SQL_SERVER -> "alter table " + table + " alter column "
                    + types.identifier(column) + " " + databaseType;
            case STANDARD -> "alter table " + table + " alter column "
                    + types.identifier(column) + " type " + databaseType;
        };
    }


    private static IllegalArgumentException incompleteColumnDefinition(String database) {
        return new IllegalArgumentException(
                "alter column type requires a complete column definition for " + database);
    }

    boolean rewritesFullColumnDefinition() {
        return databaseStyle == SchemaDialect.GeneratedValueStyle.MYSQL;
    }

    /**
     * 生成字段可空性变更 SQL。
     *
     * <p>PostgreSQL 和 H2 可以直接增删 NOT NULL；Oracle 使用 MODIFY 括号语法；SQL Server 必须重写类型；
     * MySQL 的 MODIFY COLUMN 会重写整列定义，因此必须使用渲染器给出的完整目标定义，避免悄悄丢掉注释或自增属性。
     * 自定义标准方言没有明确数据库族时，采用 PostgreSQL/H2 都支持的 ALTER COLUMN 写法。</p>
     */

    String alterColumnNullabilitySql(RelationIdentity table, String column,
                                     String databaseType, String columnDefinition,
                                     boolean nullable, String collation) {
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            return alterColumnTypeSql(table, column, databaseType, columnDefinition, nullable, collation);
        }
        String safeType = SchemaDialectTypeSupport.requireDataType(databaseType, "alter column data type");
        String tableName = types.identifier(table);
        String columnName = types.identifier(column);
        String nullClause = nullable ? " null" : " not null";
        return switch (databaseStyle) {
            case MYSQL -> "alter table " + tableName + " modify column "
                    + requireColumnDefinition(columnDefinition);
            case ORACLE -> "alter table " + tableName + " modify (" + columnName + nullClause + ")";
            case SQL_SERVER -> "alter table " + tableName + " alter column "
                    + columnName + " " + safeType + nullClause;
            case H2, POSTGRESQL, NONE -> "alter table " + tableName + " alter column " + columnName
                    + (nullable ? " drop not null" : " set not null");
        };
    }


    private String sqlServerComment(RelationIdentity table, String column, String comment, String procedure) {
        RelationIdentity relation = requireSqlServerCommentRelation(table);
        String sql = "exec " + procedure + " @name = N'MS_Description'";
        if (comment != null) {
            sql += ", @value = " + unicode(comment);
        }
        sql += ", @level0type = N'SCHEMA', @level0name = " + sqlServerCommentSchema(relation)
                + ", @level1type = N'TABLE', @level1name = " + unicode(relation.table());
        if (column != null) {
            String safeColumn = com.flying.orm.core.sql.render.SqlIdentifiers.requireIdentifier(column);
            sql += ", @level2type = N'COLUMN', @level2name = " + unicode(safeColumn);
        }
        return resolveSqlServerCommentSchema(relation, sql);
    }

    private static RelationIdentity requireSqlServerCommentRelation(RelationIdentity table) {
        RelationIdentity relation = Objects.requireNonNull(table, "comment table must not be null");
        if (relation.catalog().isPresent()) {
            throw new UnsupportedOperationException(
                    "SQL Server comments do not support a catalog-qualified table");
        }
        return relation;
    }

    private String sqlServerCommentSchema(RelationIdentity relation) {
        return relation.schema().map(this::unicode).orElse("@schema");
    }

    private String resolveSqlServerCommentSchema(RelationIdentity relation, String sql) {
        if (relation.schema().isPresent()) {
            return sql;
        }
        String resolved = "declare @schema sysname = object_schema_name(object_id(quotename("
                + unicode(relation.table()) + "))); " + sql;
        return "exec sp_executesql " + unicode(resolved);
    }

    private String unicode(String value) {
        return "N" + types.quoteLiteral(value);
    }

    private String requireColumnDefinition(String value) {
        String text = SchemaDialectTypeSupport.requireText(value, "column definition");
        for (int index = 0; index < text.length();) {
            if (text.startsWith("/*", index)) {
                int literalEnd = tableCommentStyle == SchemaDialect.TableCommentStyle.MYSQL_OPTION
                        ? MySqlSchemaCommentSupport.markedLiteralEnd(text, index) : index;
                if (literalEnd > index) {
                    index = literalEnd;
                    continue;
                }
                throw new IllegalArgumentException("column definition contains unsupported SQL syntax");
            }
            if (text.charAt(index) == ';' || text.startsWith("--", index)) {
                throw new IllegalArgumentException("column definition contains unsupported SQL syntax");
            }
            long segment = SqlLexicalScanner.protectedSegmentAt(text, index, lexicalRules, false);
            if (segment >= 0L && SqlLexicalScanner.segmentKind(segment) == SqlLexicalScanner.SegmentKind.LINE_COMMENT) {
                throw new IllegalArgumentException("column definition contains unsupported SQL syntax");
            }
            index = segment < 0L ? index + 1 : SqlLexicalScanner.segmentEnd(segment);
        }
        return text;
    }


}
