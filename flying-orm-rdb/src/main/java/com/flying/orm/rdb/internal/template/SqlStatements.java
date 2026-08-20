package com.flying.orm.rdb.internal.template;

import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.Locale;
import java.util.Objects;

/**
 * 原生 SQL 和注册模板共用的最小语句边界。
 *
 * <p>这里只确认文本非空且词法上只有一条语句，保证一个入口只交给驱动一条语句。查询是否只读属于注册模板自己的
 * 更严格策略，不能放在这里，否则显式的 unsafe 原生写入口也会被误判成查询模板。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class SqlStatements {

    private SqlStatements() {
    }

    /**
     * 返回去掉首尾空白的单条 SQL；模板构造阶段只做不依赖方言的词法检查，执行前仍按真实方言复验。
     */
    public static String requireSingle(String sql) {
        // 模板构造时还不知道方言，不能用某个数据库的关键字规则猜测其他方言语法。
        return requireSingle(sql, true, true, false, false, false);
    }

    /**
     * 在尚不知道数据库方言的低层执行边界采用可移植词法规则；井号按普通运算符处理，不能隐藏后续语句。
     */
    public static String requirePortableSingle(String sql) {
        return requireSingle(sql, false, false, false, false, false);
    }

    /**
     * 按当前方言识别注释词法；只有 SQL Server 额外检查不需要分号的批处理语句边界。
     */
    public static String requireSingle(String sql, RdbDialect dialect) {
        String dialectName = Objects.requireNonNull(dialect, "RDB dialect must not be null").name();
        return requireSingle(sql, dialectName);
    }

    /**
     * 低层 JDBC/R2DBC 原生请求按驱动报告的数据库名复用同一词法边界，避免上层校验后被错误的通用规则二次拒绝。
     */
    @InternalApi
    public static String requireSingleForDatabaseProduct(String sql, String databaseProductName) {
        return requireSingle(sql, Objects.requireNonNullElse(databaseProductName, ""));
    }

    private static String requireSingle(String sql, String dialectName) {
        String normalized = dialectName.trim().toLowerCase(Locale.ROOT);
        boolean mysql = normalized.contains("mysql") || normalized.contains("mariadb");
        boolean nestedBlockComments = normalized.contains("postgres")
                || normalized.contains("sql server")
                || normalized.contains("sqlserver")
                || normalized.contains("mssql");
        boolean sqlServer = normalized.contains("sql server")
                || normalized.contains("sqlserver")
                || normalized.contains("mssql");
        boolean oracle = normalized.contains("oracle");
        return requireSingle(sql, mysql, nestedBlockComments, mysql, sqlServer, oracle);
    }

    private static String requireSingle(String sql,
                                         boolean hashLineComments,
                                         boolean nestedBlockComments,
                                         boolean mysqlDialect,
                                         boolean sqlServerDialect,
                                         boolean oracleDialect) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("SQL statement must not be blank");
        }
        String statement = sql.trim();
        SqlStatementBoundary.validate(
                statement, hashLineComments, nestedBlockComments, mysqlDialect, sqlServerDialect, oracleDialect);
        return statement;
    }

    /**
     * 返回 Oracle {@code q'[...]'} 或 {@code nq'[...]'} 替代引号结束后的偏移；当前位置不是合法起点时
     * 返回 {@code -1}。
     * 只识别词法边界，不解释正文，调用方可以据此跳过正文里的引号、参数符号和关键字。
     *
     * @param sql 已确认非空的 SQL 文本
     * @param offset 待检查字符的偏移
     * @return 结束引号后一位的偏移，或 {@code -1}
     * @throws IllegalArgumentException 识别到合法起点但找不到结束引号时抛出
     */
    public static int oracleAlternativeQuoteEnd(String sql, int offset) {
        if (offset < 0 || offset >= sql.length()) {
            return -1;
        }
        int quotePrefixOffset = offset;
        char prefix = sql.charAt(offset);
        if (prefix == 'n' || prefix == 'N') {
            if (offset + 3 >= sql.length()) {
                return -1;
            }
            char quotePrefix = sql.charAt(offset + 1);
            if (quotePrefix != 'q' && quotePrefix != 'Q') {
                return -1;
            }
            quotePrefixOffset++;
        } else if (prefix != 'q' && prefix != 'Q') {
            return -1;
        }
        if (offset > 0 && isOracleIdentifierPart(sql.charAt(offset - 1))) {
            return -1;
        }
        if (quotePrefixOffset + 2 >= sql.length() || sql.charAt(quotePrefixOffset + 1) != '\'') {
            return -1;
        }
        char opening = sql.charAt(quotePrefixOffset + 2);
        if (Character.isWhitespace(opening)) {
            return -1;
        }
        char closing = switch (opening) {
            case '[' -> ']';
            case '{' -> '}';
            case '(' -> ')';
            case '<' -> '>';
            default -> opening;
        };
        for (int index = quotePrefixOffset + 3; index + 1 < sql.length(); index++) {
            if (sql.charAt(index) == closing && sql.charAt(index + 1) == '\'') {
                return index + 2;
            }
        }
        throw new IllegalArgumentException("SQL contains an unclosed Oracle alternative-quoted value");
    }

    private static boolean isOracleIdentifierPart(char character) {
        return Character.isLetterOrDigit(character)
                || character == '_'
                || character == '$'
                || character == '#';
    }
}
