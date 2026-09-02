package com.flying.orm.rdb.internal.template;

import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.dialect.DatabaseProduct;

import java.util.Objects;

/**
 * 受支持 SQL 方言共享的词法区域扫描器。
 *
 * <p>扫描器只返回原字符串中的起止偏移，不创建 token、子串或语法树。模板编译、单语句/只读校验、参数标记
 * 适配和日志脱敏都消费相同区域边界，各自保留自己的业务规则。</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.0
 */
@InternalApi
public final class SqlLexicalScanner {

    private static final Rules GENERIC = new Rules(false, true, true, true, true, true, true, true, true, true);
    private static final Rules PORTABLE = new Rules(false, false, false, true, true, true, true, true, true, true);
    private static final Rules MYSQL = new Rules(true, true, false, true, false, false, false, false, true, true);
    private static final Rules POSTGRESQL = new Rules(false, false, true, false, false, true, false, true, false, false);
    private static final Rules SQL_SERVER = new Rules(false, false, true, false, true, false, false, false, false, false);
    private static final Rules ORACLE = new Rules(false, false, false, false, false, false, true, false, false, false);
    private static final Rules OTHER = new Rules(false, false, false, true, false, false, false, false, false, false);
    private static final SegmentKind[] KINDS = SegmentKind.values();

    private SqlLexicalScanner() {
    }

    /** 尚未确定方言时识别全部受支持文本边界，并拒绝可执行注释。 */
    public static Rules genericRules() {
        return GENERIC;
    }

    /** 驱动产品未知时使用可移植注释规则，但仍识别全部受支持引用边界。 */
    public static Rules portableRules() {
        return PORTABLE;
    }

    /** 根据方言名或驱动产品名创建不可变扫描规则。 */
    public static Rules rulesFor(String dialectName) {
        return switch (DatabaseProduct.detect(dialectName)) {
            case MYSQL -> MYSQL;
            case POSTGRESQL -> POSTGRESQL;
            case SQL_SERVER -> SQL_SERVER;
            case ORACLE -> ORACLE;
            case H2, UNKNOWN -> OTHER;
        };
    }

    /**
     * 按原文本顺序报告最大连续 CODE 区域和受保护区域。回调只在区域边界执行，不按字符分配对象。
     */
    public static void scan(String sql, Rules rules, boolean templateSlots, SegmentConsumer consumer) {
        String source = Objects.requireNonNull(sql, "SQL must not be null");
        Rules activeRules = Objects.requireNonNull(rules, "SQL lexical rules must not be null");
        SegmentConsumer sink = Objects.requireNonNull(consumer, "SQL segment consumer must not be null");
        int codeStart = 0;
        int index = 0;
        while (index < source.length()) {
            long encodedSegment = protectedSegmentAt(source, index, activeRules, templateSlots);
            if (encodedSegment < 0L) {
                index++;
                continue;
            }
            SegmentKind kind = segmentKind(encodedSegment);
            int end = segmentEnd(encodedSegment);
            if (codeStart < index) {
                sink.accept(SegmentKind.CODE, codeStart, index);
            }
            sink.accept(kind, index, end);
            index = end;
            codeStart = index;
        }
        if (codeStart < source.length()) {
            sink.accept(SegmentKind.CODE, codeStart, source.length());
        }
    }

    /**
     * 返回当前位置开始的受保护区域编码；普通 SQL 代码返回 {@code -1}。热路径可逐字符调用而不分配 token。
     */
    public static long protectedSegmentAt(String sql, int offset, Rules rules, boolean templateSlots) {
        char current = sql.charAt(offset);
        char next = offset + 1 < sql.length() ? sql.charAt(offset + 1) : '\0';
        if (rules.postgresqlEscapeStrings && isPostgresqlEscapeStringStart(sql, offset, current, next)) {
            return segment(SegmentKind.SINGLE_QUOTED, backslashEscapedQuotedEnd(
                    sql, offset + 1, '\'', "SQL contains an unclosed PostgreSQL escape string", false));
        }
        if (current == '\'') {
            int end = rules.backslashEscapedStrings
                    ? backslashEscapedQuotedEnd(sql, offset, '\'', "SQL contains an unclosed quoted value",
                                                rules.mysqlDashComments)
                    : quotedEnd(sql, offset, '\'', "SQL contains an unclosed quoted value");
            return segment(SegmentKind.SINGLE_QUOTED, end);
        }
        if (current == '"') {
            int end = rules.backslashEscapedStrings
                    ? backslashEscapedQuotedEnd(
                            sql, offset, '"', "SQL contains an unclosed quoted identifier or value",
                            rules.mysqlDashComments)
                    : quotedEnd(sql, offset, '"', "SQL contains an unclosed quoted identifier or value");
            return segment(SegmentKind.DOUBLE_QUOTED, end);
        }
        if (rules.backticks && current == '`') {
            return segment(SegmentKind.BACKTICK_IDENTIFIER, quotedEnd(sql, offset, '`',
                    "SQL contains an unclosed backtick-quoted identifier"));
        }
        if (rules.brackets && current == '[') {
            return segment(SegmentKind.BRACKET_IDENTIFIER, quotedEnd(sql, offset, ']',
                    "SQL contains an unclosed bracket-quoted identifier"));
        }
        if (rules.oracleQuotes) {
            int end = oracleQuoteEnd(sql, offset);
            if (end >= 0) {
                return segment(SegmentKind.ORACLE_QUOTED, end);
            }
        }
        if (rules.dollarQuotes && current == '$') {
            int end = dollarQuoteEnd(sql, offset);
            if (end >= 0) {
                return segment(SegmentKind.DOLLAR_QUOTED, end);
            }
        }
        if (templateSlots && current == '$' && next == '{') {
            int end = sql.indexOf('}', offset + 2);
            if (end < 0) {
                throw new IllegalArgumentException("SQL contains an unclosed template slot");
            }
            return segment(SegmentKind.TEMPLATE_SLOT, end + 1);
        }
        if (current == '-' && next == '-' && isDashCommentStart(sql, offset, rules.mysqlDashComments)) {
            return segment(SegmentKind.LINE_COMMENT, lineCommentEnd(sql, offset + 2));
        }
        if (rules.hashComments && current == '#') {
            return segment(SegmentKind.LINE_COMMENT, lineCommentEnd(sql, offset + 1));
        }
        if (current == '/' && next == '*') {
            if (rules.rejectExecutableComments && isExecutableComment(sql, offset)) {
                throw new IllegalArgumentException("MySQL executable comments are not allowed");
            }
            return segment(SegmentKind.BLOCK_COMMENT,
                    blockCommentEnd(sql, offset + 2, rules.nestedBlockComments));
        }
        return -1L;
    }

    private static long segment(SegmentKind kind, int end) {
        return (long) kind.ordinal() << 32 | end & 0xffffffffL;
    }

    /** 从 {@link #protectedSegmentAt} 的非负结果读取区域种类。 */
    public static SegmentKind segmentKind(long encodedSegment) {
        return KINDS[(int) (encodedSegment >>> 32)];
    }

    /** 从 {@link #protectedSegmentAt} 的非负结果读取结束偏移。 */
    public static int segmentEnd(long encodedSegment) {
        return (int) encodedSegment;
    }

    private static int quotedEnd(String sql, int offset, char closing, String error) {
        for (int index = offset + 1; index < sql.length(); index++) {
            if (sql.charAt(index) != closing) {
                continue;
            }
            if (index + 1 < sql.length() && sql.charAt(index + 1) == closing) {
                index++;
            } else {
                return index + 1;
            }
        }
        throw new IllegalArgumentException(error);
    }

    private static boolean isPostgresqlEscapeStringStart(String sql, int offset, char current, char next) {
        return (current == 'e' || current == 'E')
                && next == '\''
                && (offset == 0 || !isPostgresqlIdentifierPart(sql.charAt(offset - 1)));
    }

    private static int backslashEscapedQuotedEnd(String sql,
                                                  int quoteOffset,
                                                  char quote,
                                                  String error,
                                                  boolean rejectModeDependentQuote) {
        for (int index = quoteOffset + 1; index < sql.length(); index++) {
            char current = sql.charAt(index);
            if (current == '\\') {
                if (rejectModeDependentQuote
                        && index + 1 < sql.length()
                        && sql.charAt(index + 1) == quote) {
                    throw new IllegalArgumentException(
                            "MySQL SQL quote boundary must not depend on NO_BACKSLASH_ESCAPES");
                }
                index++;
                continue;
            }
            if (current != quote) {
                continue;
            }
            if (index + 1 < sql.length() && sql.charAt(index + 1) == quote) {
                index++;
            } else {
                return index + 1;
            }
        }
        throw new IllegalArgumentException(error);
    }

    private static int oracleQuoteEnd(String sql, int offset) {
        int qOffset = offset;
        char prefix = sql.charAt(offset);
        if (prefix == 'n' || prefix == 'N') {
            if (offset + 3 >= sql.length() || (sql.charAt(offset + 1) != 'q' && sql.charAt(offset + 1) != 'Q')) {
                return -1;
            }
            qOffset++;
        } else if (prefix != 'q' && prefix != 'Q') {
            return -1;
        }
        if (offset > 0 && isOracleIdentifierPart(sql.charAt(offset - 1))) {
            return -1;
        }
        if (qOffset + 2 >= sql.length() || sql.charAt(qOffset + 1) != '\'') {
            return -1;
        }
        char opening = sql.charAt(qOffset + 2);
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
        for (int index = qOffset + 3; index + 1 < sql.length(); index++) {
            if (sql.charAt(index) == closing && sql.charAt(index + 1) == '\'') {
                return index + 2;
            }
        }
        throw new IllegalArgumentException("SQL contains an unclosed Oracle alternative-quoted value");
    }

    private static int dollarQuoteEnd(String sql, int offset) {
        if (offset > 0 && isPostgresqlIdentifierPart(sql.charAt(offset - 1))) {
            return -1;
        }
        int delimiterEnd = offset + 1;
        if (delimiterEnd < sql.length() && sql.charAt(delimiterEnd) != '$') {
            if (!isWordStart(sql.charAt(delimiterEnd))) {
                return -1;
            }
            delimiterEnd++;
            while (delimiterEnd < sql.length() && isWordPart(sql.charAt(delimiterEnd))) {
                delimiterEnd++;
            }
        }
        if (delimiterEnd >= sql.length() || sql.charAt(delimiterEnd) != '$') {
            return -1;
        }
        int delimiterLength = delimiterEnd - offset + 1;
        for (int index = delimiterEnd + 1; index + delimiterLength <= sql.length(); index++) {
            if (sql.regionMatches(index, sql, offset, delimiterLength)) {
                return index + delimiterLength;
            }
        }
        throw new IllegalArgumentException("SQL contains an unclosed dollar-quoted value");
    }

    private static int lineCommentEnd(String sql, int offset) {
        for (int index = offset; index < sql.length(); index++) {
            char current = sql.charAt(index);
            if (current == '\n' || current == '\r') {
                return index + 1;
            }
        }
        return sql.length();
    }

    private static int blockCommentEnd(String sql, int offset, boolean nested) {
        int depth = 1;
        for (int index = offset; index + 1 < sql.length(); index++) {
            char current = sql.charAt(index);
            char next = sql.charAt(index + 1);
            if (nested && current == '/' && next == '*') {
                depth++;
                index++;
            } else if (current == '*' && next == '/') {
                depth--;
                index++;
                if (depth == 0) {
                    return index + 1;
                }
            }
        }
        throw new IllegalArgumentException("SQL contains an unclosed block comment");
    }

    private static boolean isDashCommentStart(String sql, int offset, boolean mysql) {
        if (!mysql || offset + 2 >= sql.length()) {
            return true;
        }
        char following = sql.charAt(offset + 2);
        return Character.isWhitespace(following) || Character.isISOControl(following);
    }

    private static boolean isExecutableComment(String sql, int offset) {
        int marker = offset + 2;
        return marker < sql.length() && (sql.charAt(marker) == '!'
                || marker + 1 < sql.length()
                && (sql.charAt(marker) == 'm' || sql.charAt(marker) == 'M')
                && sql.charAt(marker + 1) == '!');
    }

    private static boolean isWordStart(char value) {
        return Character.isLetter(value) || value == '_';
    }

    private static boolean isWordPart(char value) {
        return Character.isLetterOrDigit(value) || value == '_';
    }

    private static boolean isPostgresqlIdentifierPart(char value) {
        return isWordPart(value) || value == '$';
    }

    private static boolean isOracleIdentifierPart(char value) {
        return isWordPart(value) || value == '$' || value == '#';
    }

    /** 供跨包消费者持有的不可变、不透明方言规则。 */
    public static final class Rules {
        private final boolean mysqlDashComments;
        private final boolean hashComments;
        private final boolean nestedBlockComments;
        private final boolean backticks;
        private final boolean brackets;
        private final boolean dollarQuotes;
        private final boolean oracleQuotes;
        private final boolean postgresqlEscapeStrings;
        private final boolean backslashEscapedStrings;
        private final boolean rejectExecutableComments;

        private Rules(boolean mysqlDashComments, boolean hashComments, boolean nestedBlockComments,
                      boolean backticks, boolean brackets, boolean dollarQuotes, boolean oracleQuotes,
                      boolean postgresqlEscapeStrings, boolean backslashEscapedStrings,
                      boolean rejectExecutableComments) {
            this.mysqlDashComments = mysqlDashComments;
            this.hashComments = hashComments;
            this.nestedBlockComments = nestedBlockComments;
            this.backticks = backticks;
            this.brackets = brackets;
            this.dollarQuotes = dollarQuotes;
            this.oracleQuotes = oracleQuotes;
            this.postgresqlEscapeStrings = postgresqlEscapeStrings;
            this.backslashEscapedStrings = backslashEscapedStrings;
            this.rejectExecutableComments = rejectExecutableComments;
        }
    }

    /** 扫描区域种类。 */
    public enum SegmentKind {
        CODE,
        SINGLE_QUOTED,
        DOUBLE_QUOTED,
        BACKTICK_IDENTIFIER,
        BRACKET_IDENTIFIER,
        DOLLAR_QUOTED,
        ORACLE_QUOTED,
        LINE_COMMENT,
        BLOCK_COMMENT,
        TEMPLATE_SLOT
    }

    /** 接收原 SQL 中的区域起止偏移。 */
    @FunctionalInterface
    public interface SegmentConsumer {
        void accept(SegmentKind kind, int start, int end);
    }

}
