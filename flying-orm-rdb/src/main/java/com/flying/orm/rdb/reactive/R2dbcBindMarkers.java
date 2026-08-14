package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.internal.template.SqlStatements;
import io.r2dbc.spi.ConnectionFactory;

import java.util.Locale;
import java.util.Objects;

/**
 * 把渲染层统一使用的问号占位符换成具体 R2DBC 驱动认识的写法。
 * SQL AST 和参数顺序不需要知道驱动差异，执行层只在真正创建 Statement 前转换一次。
 *
 * <p>AUTO 风格的 SQL 必须来自 flying-orm 渲染器，问号都代表参数；调用方传入数据库原生 SQL 时应声明
 * NATIVE，避免把字符串常量里的问号误认为占位符。实例只保存枚举格式，可并发共享。</p>
 */
final class R2dbcBindMarkers {

    private final MarkerFormat format;

    private R2dbcBindMarkers(MarkerFormat format) {
        this.format = format;
    }

    static R2dbcBindMarkers from(ConnectionFactory connectionFactory) {
        // ConnectionFactory metadata 是 R2DBC SPI 在不取连接时识别驱动的标准入口。
        String databaseName = Objects.requireNonNull(connectionFactory.getMetadata(),
                                                     "connection factory metadata must not be null").getName();
        String normalized = databaseName == null ? "" : databaseName.toLowerCase(Locale.ROOT);
        if (normalized.contains("postgres")) {
            return new R2dbcBindMarkers(MarkerFormat.POSTGRESQL);
        }
        if (normalized.contains("sql server") || normalized.contains("mssql")) {
            return new R2dbcBindMarkers(MarkerFormat.SQL_SERVER);
        }
        if (normalized.contains("oracle")) {
            return new R2dbcBindMarkers(MarkerFormat.ORACLE);
        }
        return new R2dbcBindMarkers(MarkerFormat.QUESTION_MARK);
    }

    String adapt(String sql, int parameterCount, SqlBindMarkerStyle markerStyle) {
        Objects.requireNonNull(sql, "sql must not be null");
        Objects.requireNonNull(markerStyle, "sql bind marker style must not be null");
        if (markerStyle == SqlBindMarkerStyle.NATIVE) {
            return sql;
        }
        if (sql.indexOf('?') < 0) {
            if (parameterCount != 0) {
                throw new IllegalArgumentException("sql parameter marker count does not match parameter count");
            }
            return sql;
        }

        // 预留少量增长空间，PostgreSQL 的 $n 和 SQL Server 的 @Pn 通常比问号长。
        StringBuilder adapted = new StringBuilder(sql.length() + parameterCount * 2);
        int markerIndex = 0;
        int blockCommentDepth = 0;
        String dollarQuoteDelimiter = null;
        QuoteState state = QuoteState.PLAIN;
        for (int i = 0; i < sql.length(); i++) {
            char current = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';
            if (state == QuoteState.DOLLAR_QUOTE) {
                if (sql.startsWith(dollarQuoteDelimiter, i)) {
                    adapted.append(dollarQuoteDelimiter);
                    i += dollarQuoteDelimiter.length() - 1;
                    dollarQuoteDelimiter = null;
                    state = QuoteState.PLAIN;
                } else {
                    adapted.append(current);
                }
                continue;
            }
            if (state == QuoteState.BLOCK_COMMENT && current == '/' && next == '*') {
                blockCommentDepth++;
                adapted.append(current).append(next);
                i++;
                continue;
            }
            if (state == QuoteState.BLOCK_COMMENT && current == '*' && next == '/') {
                blockCommentDepth--;
                adapted.append(current).append(next);
                i++;
                if (blockCommentDepth == 0) {
                    state = QuoteState.PLAIN;
                }
                continue;
            }
            if (state == QuoteState.PLAIN && format == MarkerFormat.ORACLE) {
                int oracleQuoteEnd = SqlStatements.oracleAlternativeQuoteEnd(sql, i);
                if (oracleQuoteEnd >= 0) {
                    adapted.append(sql, i, oracleQuoteEnd);
                    i = oracleQuoteEnd - 1;
                    continue;
                }
            }
            if (state == QuoteState.PLAIN && current == '-' && next == '-') {
                state = QuoteState.LINE_COMMENT;
            } else if (state == QuoteState.PLAIN && current == '/' && next == '*') {
                state = QuoteState.BLOCK_COMMENT;
                blockCommentDepth = 1;
            } else if (state == QuoteState.LINE_COMMENT && (current == '\n' || current == '\r')) {
                state = QuoteState.PLAIN;
            } else if (state == QuoteState.PLAIN && current == '\'') {
                state = QuoteState.SINGLE_QUOTE;
            } else if (state == QuoteState.SINGLE_QUOTE && current == '\'' && next == '\'') {
                adapted.append(current).append(next);
                i++;
                continue;
            } else if (state == QuoteState.SINGLE_QUOTE && current == '\'') {
                state = QuoteState.PLAIN;
            } else if (state == QuoteState.PLAIN && current == '"') {
                state = QuoteState.DOUBLE_QUOTE;
            } else if (state == QuoteState.DOUBLE_QUOTE && current == '"' && next == '"') {
                adapted.append(current).append(next);
                i++;
                continue;
            } else if (state == QuoteState.DOUBLE_QUOTE && current == '"') {
                state = QuoteState.PLAIN;
            } else if (state == QuoteState.PLAIN && format == MarkerFormat.QUESTION_MARK && current == '`') {
                state = QuoteState.BACKTICK_IDENTIFIER;
            } else if (state == QuoteState.BACKTICK_IDENTIFIER && current == '`' && next == '`') {
                adapted.append(current).append(next);
                i++;
                continue;
            } else if (state == QuoteState.BACKTICK_IDENTIFIER && current == '`') {
                state = QuoteState.PLAIN;
            } else if (state == QuoteState.PLAIN && format == MarkerFormat.SQL_SERVER && current == '[') {
                state = QuoteState.BRACKET_IDENTIFIER;
            } else if (state == QuoteState.BRACKET_IDENTIFIER && current == ']' && next == ']') {
                adapted.append(current).append(next);
                i++;
                continue;
            } else if (state == QuoteState.BRACKET_IDENTIFIER && current == ']') {
                state = QuoteState.PLAIN;
            } else if (state == QuoteState.PLAIN && format == MarkerFormat.POSTGRESQL && current == '$') {
                String delimiter = dollarQuoteDelimiterAt(sql, i);
                if (delimiter != null) {
                    adapted.append(delimiter);
                    i += delimiter.length() - 1;
                    dollarQuoteDelimiter = delimiter;
                    state = QuoteState.DOLLAR_QUOTE;
                    continue;
                }
            }
            if (state == QuoteState.PLAIN && current == '?') {
                appendMarker(adapted, markerIndex++);
            } else {
                adapted.append(current);
            }
        }
        // CANONICAL SQL 来自渲染器。引号或注释没闭合说明 SQL 已损坏，不能把半截语句交给驱动猜。
        if (state != QuoteState.PLAIN && state != QuoteState.LINE_COMMENT) {
            throw new IllegalArgumentException("sql contains an unclosed quoted value, identifier or block comment");
        }
        // 数量不一致说明渲染结果和参数列表已经脱节，必须在到达驱动前失败。
        if (markerIndex != parameterCount) {
            throw new IllegalArgumentException("sql parameter marker count does not match parameter count");
        }
        return adapted.toString();
    }

    /**
     * 识别 PostgreSQL 的 {@code $$} 或 {@code $tag$} 文本边界。
     * 数字不能作为标签首字符，因此 {@code $1} 这类原生参数不会被误当成文本开始。
     */
    private static String dollarQuoteDelimiterAt(String sql, int offset) {
        int end = sql.indexOf('$', offset + 1);
        if (end < 0) {
            return null;
        }
        if (end == offset + 1) {
            return "$$";
        }
        char first = sql.charAt(offset + 1);
        if (!(Character.isLetter(first) || first == '_')) {
            return null;
        }
        for (int index = offset + 2; index < end; index++) {
            char character = sql.charAt(index);
            if (!(Character.isLetterOrDigit(character) || character == '_')) {
                return null;
            }
        }
        return sql.substring(offset, end + 1);
    }

    private void appendMarker(StringBuilder sql, int markerIndex) {
        if (format == MarkerFormat.QUESTION_MARK || format == MarkerFormat.ORACLE) {
            sql.append('?');
            return;
        }
        if (format == MarkerFormat.POSTGRESQL) {
            sql.append('$').append(markerIndex + 1);
            return;
        }
        sql.append("@P").append(markerIndex);
    }

    private enum MarkerFormat {
        QUESTION_MARK,
        ORACLE,
        POSTGRESQL,
        SQL_SERVER
    }

    /** 只区分渲染器可能生成的字符串、标识符和 SQL 注释；原生数据库语法应使用 NATIVE 模式。 */
    private enum QuoteState {
        PLAIN,
        SINGLE_QUOTE,
        DOUBLE_QUOTE,
        BACKTICK_IDENTIFIER,
        DOLLAR_QUOTE,
        BRACKET_IDENTIFIER,
        LINE_COMMENT,
        BLOCK_COMMENT
    }
}
