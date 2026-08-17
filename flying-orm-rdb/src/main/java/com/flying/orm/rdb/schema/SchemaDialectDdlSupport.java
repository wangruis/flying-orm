package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * 负责字段注释、索引、字段改名、字段类型变更和会话锁超时等 DDL 语法差异。
 *
 * <p>数据库版本边界仍由上层配置的样式枚举表达；这里不会根据数据库名字猜语法，也不会把不确定的
 * 在线 DDL 当成通用能力。</p>
 */
final class SchemaDialectDdlSupport {

    private final SchemaDialectTypeSupport types;
    private final SchemaDialect.ColumnCommentStyle columnCommentStyle;
    private final SchemaDialect.DropIndexStyle dropIndexStyle;
    private final SchemaDialect.RenameColumnStyle renameColumnStyle;
    private final SchemaDialect.GeneratedValueStyle databaseStyle;
    private final SchemaDialect.ColumnChangeStyle columnChangeStyle;
    private final SchemaOnlineDdlSupport onlineDdlSupport;
    private final SchemaLockTimeoutStyle lockTimeoutStyle;

    SchemaDialectDdlSupport(SchemaDialectTypeSupport types,
                            SchemaDialect.ColumnCommentStyle columnCommentStyle,
                            SchemaDialect.DropIndexStyle dropIndexStyle,
                            SchemaDialect.RenameColumnStyle renameColumnStyle,
                            SchemaDialect.GeneratedValueStyle databaseStyle,
                            SchemaDialect.ColumnChangeStyle columnChangeStyle,
                            SchemaOnlineDdlSupport onlineDdlSupport,
                            SchemaLockTimeoutStyle lockTimeoutStyle) {
        this.types = Objects.requireNonNull(types, "type support must not be null");
        this.columnCommentStyle = Objects.requireNonNull(columnCommentStyle, "column comment style must not be null");
        this.dropIndexStyle = Objects.requireNonNull(dropIndexStyle, "drop index style must not be null");
        this.renameColumnStyle = Objects.requireNonNull(renameColumnStyle, "rename column style must not be null");
        this.databaseStyle = Objects.requireNonNull(databaseStyle, "database DDL style must not be null");
        this.columnChangeStyle = Objects.requireNonNull(columnChangeStyle, "column change style must not be null");
        this.onlineDdlSupport = Objects.requireNonNull(onlineDdlSupport, "online DDL support must not be null");
        this.lockTimeoutStyle = Objects.requireNonNull(lockTimeoutStyle, "lock timeout style must not be null");
    }

    boolean inlineColumnComment() {
        return columnCommentStyle == SchemaDialect.ColumnCommentStyle.INLINE;
    }

    Optional<String> columnCommentSql(String table, String column, String comment) {
        if (comment == null || columnCommentStyle == SchemaDialect.ColumnCommentStyle.NONE
                || columnCommentStyle == SchemaDialect.ColumnCommentStyle.INLINE) {
            return Optional.empty();
        }
        if (columnCommentStyle == SchemaDialect.ColumnCommentStyle.SQL_SERVER_EXTENDED_PROPERTY) {
            return Optional.of(sqlServerColumnComment(table, column, comment));
        }
        return Optional.of("comment on column " + types.identifier(table) + "." + types.identifier(column)
                                   + " is " + types.quoteLiteral(comment));
    }

    Optional<String> columnCommentChangeSql(String table,
                                            String column,
                                            String previousComment,
                                            String targetComment) {
        if (Objects.equals(previousComment, targetComment)
                || columnCommentStyle == SchemaDialect.ColumnCommentStyle.NONE
                || columnCommentStyle == SchemaDialect.ColumnCommentStyle.INLINE) {
            return Optional.empty();
        }
        if (columnCommentStyle == SchemaDialect.ColumnCommentStyle.SQL_SERVER_EXTENDED_PROPERTY) {
            if (previousComment == null) {
                return Optional.of(sqlServerColumnComment(table, column, targetComment));
            }
            return Optional.of(targetComment == null
                    ? sqlServerColumnComment(table, column, null, "sp_dropextendedproperty")
                    : sqlServerColumnComment(table, column, targetComment, "sp_updateextendedproperty"));
        }
        String value = targetComment == null
                ? databaseStyle == SchemaDialect.GeneratedValueStyle.ORACLE ? "''" : "null"
                : types.quoteLiteral(targetComment);
        return Optional.of("comment on column " + types.identifier(table) + "." + types.identifier(column)
                                   + " is " + value);
    }

    String dropIndexSql(String table, String index) {
        String indexName = requireUnqualifiedIdentifier(index, "drop index name");
        if (dropIndexStyle == SchemaDialect.DropIndexStyle.NAME_ONLY) {
            String safeTable = com.flying.orm.core.sql.render.SqlIdentifiers.requireIdentifier(
                    table, "drop index table");
            int separator = safeTable.lastIndexOf('.');
            if (separator >= 0) {
                indexName = safeTable.substring(0, separator + 1) + indexName;
            }
        }
        String sql = "drop index " + types.identifier(indexName);
        return dropIndexStyle == SchemaDialect.DropIndexStyle.ON_TABLE
                ? sql + " on " + types.identifier(table)
                : sql;
    }

    SqlRequest preferOnline(SqlRequest request) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "schema SQL request must not be null");
        if (onlineDdlSupport != SchemaOnlineDdlSupport.CONCURRENT_INDEX) {
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

    SchemaDdlSessionGuard lockTimeoutGuard(Duration timeout) {
        Duration safeTimeout = Objects.requireNonNull(timeout, "DDL lock timeout must not be null");
        if (safeTimeout.isZero() || safeTimeout.isNegative()) {
            throw new IllegalArgumentException("DDL lock timeout must be positive");
        }
        long millis = Math.max(1, saturatingMillis(safeTimeout));
        long seconds = Math.max(1, ceilMillisToSeconds(millis));
        requireLockTimeoutRange(millis, seconds);
        return switch (lockTimeoutStyle) {
            case MYSQL -> guard("set session lock_wait_timeout = " + seconds,
                                "set session lock_wait_timeout = default");
            case POSTGRESQL -> guard("set lock_timeout = '" + millis + "ms'", "reset lock_timeout");
            case ORACLE -> guard("alter session set ddl_lock_timeout = " + seconds,
                                 "alter session set ddl_lock_timeout = 0");
            case SQL_SERVER -> guard("set lock_timeout " + millis, "set lock_timeout -1");
            case NONE -> throw new UnsupportedOperationException(
                    "current schema dialect does not support session lock timeout");
        };
    }

    /** 会话变量的数值上限由数据库定义，越界配置必须在发送 SQL 前稳定失败。 */
    private void requireLockTimeoutRange(long millis, long seconds) {
        boolean withinRange = switch (lockTimeoutStyle) {
            case MYSQL -> seconds <= 31_536_000L;
            case POSTGRESQL, SQL_SERVER -> millis <= Integer.MAX_VALUE;
            case ORACLE -> seconds <= 1_000_000L;
            case NONE -> true;
        };
        if (!withinRange) {
            throw new IllegalArgumentException("DDL lock timeout exceeds current database range");
        }
    }

    private static String requireUnqualifiedIdentifier(String value, String category) {
        String identifier = com.flying.orm.core.sql.render.SqlIdentifiers.requireIdentifier(value, category);
        if (identifier.indexOf('.') >= 0) {
            throw new IllegalArgumentException(category + " must not be qualified");
        }
        return identifier;
    }

    /** 数据库超时使用 long 毫秒表示；超大 Duration 保持为可传递的最大值，而不是在 ORM 中溢出。 */
    private static long saturatingMillis(Duration duration) {
        try {
            return duration.toMillis();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    /** 正数毫秒向上换算秒，避免 {@code millis + 999} 在最大值时溢出。 */
    private static long ceilMillisToSeconds(long millis) {
        return millis / 1000L + (millis % 1000L == 0L ? 0L : 1L);
    }

    String renameColumnSql(String table, String oldName, String newName) {
        if (renameColumnStyle == SchemaDialect.RenameColumnStyle.SQL_SERVER_SP_RENAME) {
            String objectName = com.flying.orm.core.sql.render.SqlIdentifiers.requireIdentifier(table, "rename table")
                    + "." + com.flying.orm.core.sql.render.SqlIdentifiers.requireIdentifier(oldName, "old column name");
            String targetName = com.flying.orm.core.sql.render.SqlIdentifiers.requireIdentifier(newName, "new column name");
            return "exec sp_rename N" + types.quoteLiteral(objectName)
                    + ", N" + types.quoteLiteral(targetName) + ", N'COLUMN'";
        }
        return "alter table " + types.identifier(table) + " rename column " + types.identifier(oldName)
                + " to " + types.identifier(newName);
    }

    String addColumnSql(String table, String columnDefinition) {
        String safeDefinition = requireColumnDefinition(columnDefinition);
        return switch (columnChangeStyle) {
            case ORACLE -> "alter table " + types.identifier(table) + " add (" + safeDefinition + ")";
            case SQL_SERVER -> "alter table " + types.identifier(table) + " add " + safeDefinition;
            case STANDARD -> "alter table " + types.identifier(table) + " add column " + safeDefinition;
        };
    }

    String alterColumnTypeSql(String table, String column, String databaseType) {
        String safeType = SchemaDialectTypeSupport.requireDataType(databaseType, "alter column data type");
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.MYSQL) {
            throw incompleteColumnDefinition("mysql");
        }
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            throw incompleteColumnDefinition("sql server");
        }
        return switch (columnChangeStyle) {
            case ORACLE -> "alter table " + types.identifier(table) + " modify ("
                    + types.identifier(column) + " " + safeType + ")";
            case SQL_SERVER -> "alter table " + types.identifier(table) + " alter column "
                    + types.identifier(column) + " " + safeType;
            case STANDARD -> "alter table " + types.identifier(table) + " alter column "
                    + types.identifier(column) + " type " + safeType;
        };
    }

    String alterColumnTypeSql(String table,
                              String column,
                              String databaseType,
                              String columnDefinition) {
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.MYSQL) {
            return "alter table " + types.identifier(table) + " modify column "
                    + requireColumnDefinition(columnDefinition);
        }
        if (databaseStyle == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            String safeType = SchemaDialectTypeSupport.requireDataType(databaseType, "alter column data type");
            return "alter table " + types.identifier(table) + " alter column "
                    + types.identifier(column) + " " + safeType
                    + sqlServerNullability(columnDefinition);
        }
        return alterColumnTypeSql(table, column, databaseType);
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
    String alterColumnNullabilitySql(String table,
                                     String column,
                                     String databaseType,
                                     String columnDefinition,
                                     boolean nullable) {
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

    private String sqlServerColumnComment(String table, String column, String comment) {
        return sqlServerColumnComment(table, column, comment, "sp_addextendedproperty");
    }

    private String sqlServerColumnComment(String table, String column, String comment, String procedure) {
        String safeTable = com.flying.orm.core.sql.render.SqlIdentifiers.requireIdentifier(table, "comment table");
        String[] parts = safeTable.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException("SQL Server column comments require a schema-qualified table");
        }
        String schema = parts[0];
        String tableName = parts[1];
        String safeColumn = com.flying.orm.core.sql.render.SqlIdentifiers.requireIdentifier(column, "comment column");
        String sql = "exec " + procedure + " @name = N'MS_Description'";
        if (comment != null) {
            sql += ", @value = " + unicode(comment);
        }
        return sql
                + ", @level0type = N'SCHEMA', @level0name = " + unicode(schema)
                + ", @level1type = N'TABLE', @level1name = " + unicode(tableName)
                + ", @level2type = N'COLUMN', @level2name = " + unicode(safeColumn);
    }

    private String unicode(String value) {
        return "N" + types.quoteLiteral(value);
    }

    private static SchemaDdlSessionGuard guard(String setup, String cleanup) {
        return new SchemaDdlSessionGuard(List.of(new SqlRequest(setup, List.of())),
                                         List.of(new SqlRequest(cleanup, List.of())));
    }

    private static String requireColumnDefinition(String value) {
        String text = SchemaDialectTypeSupport.requireText(value, "column definition");
        if (text.indexOf(';') >= 0 || text.contains("--") || text.contains("/*")) {
            throw new IllegalArgumentException("column definition contains unsupported SQL syntax");
        }
        return text;
    }

    private static String sqlServerNullability(String columnDefinition) {
        String normalized = requireColumnDefinition(columnDefinition).toLowerCase(Locale.ROOT);
        if (normalized.endsWith(" not null") || normalized.endsWith(" not null primary key")) {
            return " not null";
        }
        if (normalized.endsWith(" null")) {
            return " null";
        }
        throw new IllegalArgumentException("SQL Server column definition must declare nullability");
    }
}
