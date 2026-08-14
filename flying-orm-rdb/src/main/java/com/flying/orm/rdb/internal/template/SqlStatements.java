package com.flying.orm.rdb.internal.template;

/**
 * 原生 SQL 和注册模板共用的最小语句边界。
 *
 * <p>这里只确认文本非空且没有分号，保证一个入口只交给驱动一条语句。查询是否只读属于注册模板自己的
 * 更严格策略，不能放在这里，否则显式的 unsafe 原生写入口也会被误判成查询模板。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class SqlStatements {

    private SqlStatements() {
    }

    /** 返回去掉首尾空白的单条 SQL；空文本或含分号时立即拒绝。 */
    public static String requireSingle(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("SQL statement must not be blank");
        }
        String statement = sql.trim();
        if (statement.indexOf(';') >= 0) {
            throw new IllegalArgumentException("SQL must contain exactly one statement and no semicolon");
        }
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
