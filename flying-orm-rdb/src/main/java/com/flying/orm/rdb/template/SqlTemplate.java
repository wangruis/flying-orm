package com.flying.orm.rdb.template;

import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.template.SqlStatements;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * 启动阶段注册的只读 SQL 查询模板。
 *
 * <p>模板正文只能来自后端代码或可信配置，不能由前端请求直接传入。业务值写成 {@code :name}，
 * 动态表名或列名写成 {@code ${name}}，并且必须在 {@link #query(String, String, Set)} 中提前声明。
 * 这个类型没有公开构造器，所有模板都会经过同一套单语句和只读检查。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class SqlTemplate {

    /**
     * 这些关键字出现在引号和注释之外时，语句就可能改数据、改结构或持有额外锁。
     * 查询模板宁可保守拒绝，也不把一个看起来只读的入口变成隐蔽写入口。
     */
    private static final Set<String> WRITE_KEYWORDS = Set.of(
            "ALTER", "BACKUP", "BULK", "CALL", "CHECKPOINT", "COPY", "CREATE", "DBCC", "DELETE", "DENY",
            "DROP", "EXEC", "EXECUTE", "COMMIT", "DECLARE", "GRANT", "INSERT", "INTO", "KILL", "LOAD",
            "LOCK", "MERGE", "PRINT", "RAISERROR", "RECONFIGURE", "REPLACE", "RESTORE", "RETURN", "REVOKE",
            "ROLLBACK", "SAVE", "SHUTDOWN", "THROW", "TRUNCATE", "UPDATE", "UPSERT", "USE", "WAITFOR");

    private static final Set<String> SQL_SERVER_BATCH_KEYWORDS = Set.of(
            "ADD", "BREAK", "CONTINUE", "DUMP", "GOTO", "IF", "LOAD", "READTEXT", "RECEIVE", "REVERT",
            "SEND", "SETUSER", "UPDATETEXT", "WHILE", "WRITETEXT");

    private final String id;

    private final String sql;

    private final Set<String> identifierSlots;

    private SqlTemplate(String id, String sql, Set<String> identifierSlots, boolean readOnly) {
        this.id = requireText(id, "SQL template id");
        this.sql = SqlStatements.requireSingle(sql);
        this.identifierSlots = normalizeSlots(identifierSlots);
        if (readOnly) {
            requireReadOnlyQuery(this.sql);
        }
    }

    /**
     * 注册一条只读查询模板。
     *
     * @param id 稳定模板 ID，执行时只按 ID 选择模板
     * @param sql 单条 SELECT 或只读 WITH 查询
     * @param identifierSlots 允许替换的动态表名或列名槽位
     * @return 完成校验后的不可变查询模板
     */
    public static SqlTemplate query(String id, String sql, Set<String> identifierSlots) {
        return new SqlTemplate(id, sql, identifierSlots, true);
    }

    /** 原生 SQL 只复用命名参数扫描模型，不套用注册查询模板的只读策略。 */
    static SqlTemplate nativeStatement(String sql) {
        return new SqlTemplate("direct-native-sql", sql, Set.of(), false);
    }

    public String id() {
        return id;
    }

    public String sql() {
        return sql;
    }

    public Set<String> identifierSlots() {
        return identifierSlots;
    }

    static String requireName(String value, String fieldName) {
        String text = requireText(value, fieldName);
        if (!Character.isJavaIdentifierStart(text.charAt(0))) {
            throw new IllegalArgumentException(fieldName + " must start with a letter or underscore");
        }
        for (int index = 1; index < text.length(); index++) {
            if (!Character.isJavaIdentifierPart(text.charAt(index))) {
                throw new IllegalArgumentException(fieldName + " contains an unsafe character");
            }
        }
        return text;
    }

    private static Set<String> normalizeSlots(Set<String> identifierSlots) {
        Set<String> normalizedSlots = new LinkedHashSet<>();
        for (String slot : Objects.requireNonNull(identifierSlots, "identifier slots must not be null")) {
            String normalized = requireName(slot, "identifier slot");
            if (!normalizedSlots.add(normalized)) {
                throw new IllegalArgumentException("duplicate identifier slot after normalization");
            }
        }
        return Collections.unmodifiableSet(normalizedSlots);
    }

    private static void requireReadOnlyQuery(String sql) {
        requireReadOnlyQuery(sql, null);
    }

    /**
     * 按实际方言复验只读模板。注册阶段尚不知道方言，因此只跳过全部受支持方言都可能使用的文本边界；
     * 引擎装配后再按唯一方言收紧注释和引号规则，避免跨方言词法差异隐藏写操作。
     */
    static void requireReadOnlyQuery(String sql, RdbDialect dialect) {
        String dialectName = dialect == null ? "" : dialect.name();
        boolean generic = dialect == null;
        boolean mysql = "mysql".equalsIgnoreCase(dialectName);
        boolean oracle = generic || "oracle".equalsIgnoreCase(dialectName);
        boolean postgresql = generic || "postgresql".equalsIgnoreCase(dialectName);
        boolean sqlServer = "sqlserver".equalsIgnoreCase(dialectName)
                || "sql-server".equalsIgnoreCase(dialectName);
        String firstKeyword = null;
        String previousKeyword = null;
        int caseDepth = 0;
        boolean previousEndClosedCase = false;
        for (int index = 0; index < sql.length();) {
            char current = sql.charAt(index);
            char next = index + 1 < sql.length() ? sql.charAt(index + 1) : '\0';

            if (Character.isWhitespace(current)) {
                index++;
                continue;
            }
            if (isLineCommentStart(sql, index, mysql)) {
                index = skipLineComment(sql, index + 2);
                continue;
            }
            if ((generic || mysql) && current == '#') {
                index = skipLineComment(sql, index + 1);
                continue;
            }
            if (current == '/' && next == '*') {
                if (isExecutableBlockComment(sql, index)) {
                    throw new IllegalArgumentException(
                            "SQL query template must not contain executable comments");
                }
                index = skipBlockComment(sql, index + 2, generic || postgresql || sqlServer);
                continue;
            }
            if (oracle) {
                int oracleQuoteEnd = SqlStatements.oracleAlternativeQuoteEnd(sql, index);
                if (oracleQuoteEnd >= 0) {
                    index = oracleQuoteEnd;
                    continue;
                }
            }
            if (current == '\'' || current == '\"' || current == '`' || current == '[') {
                index = skipQuoted(sql, index, current == '[' ? ']' : current);
                continue;
            }
            if (current == '$' && next == '{') {
                index = skipIdentifierSlot(sql, index + 2);
                continue;
            }
            if (postgresql && current == '$') {
                String delimiter = dollarQuoteDelimiterAt(sql, index);
                if (delimiter != null) {
                    index = skipDollarQuoted(sql, index, delimiter);
                    continue;
                }
            }
            if (current == ':' && Character.isJavaIdentifierStart(next)) {
                index = skipIdentifier(sql, index + 1);
                continue;
            }
            if (!Character.isLetter(current)) {
                index++;
                continue;
            }

            int end = index + 1;
            while (end < sql.length() && (Character.isLetterOrDigit(sql.charAt(end)) || sql.charAt(end) == '_')) {
                end++;
            }
            String keyword = sql.substring(index, end).toUpperCase(Locale.ROOT);
            if (firstKeyword == null) {
                firstKeyword = keyword;
            }
            boolean endClosedCase = "END".equals(keyword) && caseDepth > 0;
            if (sqlServer && "CONVERSATION".equals(keyword) && "END".equals(previousKeyword)
                    && !previousEndClosedCase) {
                throw new IllegalArgumentException(
                        "SQL query template contains a write or DDL statement: END CONVERSATION");
            }
            if (WRITE_KEYWORDS.contains(keyword) || sqlServer && SQL_SERVER_BATCH_KEYWORDS.contains(keyword)) {
                throw new IllegalArgumentException(
                        "SQL query template contains a write or DDL keyword: " + keyword);
            }
            if ("CASE".equals(keyword)) {
                caseDepth++;
            } else if (endClosedCase) {
                caseDepth--;
            }
            previousEndClosedCase = endClosedCase;
            previousKeyword = keyword;
            index = end;
        }

        if (!"SELECT".equals(firstKeyword) && !"WITH".equals(firstKeyword)) {
            throw new IllegalArgumentException("SQL query template must start with SELECT or WITH");
        }
    }

    /** MySQL 与 MariaDB 会执行这两类特殊块注释，不能把它们当作普通注释跳过。 */
    private static boolean isExecutableBlockComment(String sql, int index) {
        int marker = index + 2;
        if (marker < sql.length() && sql.charAt(marker) == '!') {
            return true;
        }
        return marker + 1 < sql.length()
                && (sql.charAt(marker) == 'M' || sql.charAt(marker) == 'm')
                && sql.charAt(marker + 1) == '!';
    }

    private static int skipLineComment(String sql, int index) {
        while (index < sql.length() && sql.charAt(index) != '\n' && sql.charAt(index) != '\r') {
            index++;
        }
        return index;
    }

    private static int skipBlockComment(String sql, int index, boolean nested) {
        int depth = 1;
        while (index < sql.length()) {
            char current = sql.charAt(index);
            char next = index + 1 < sql.length() ? sql.charAt(index + 1) : '\0';
            if (nested && current == '/' && next == '*') {
                depth++;
                index += 2;
            } else if (current == '*' && next == '/') {
                depth--;
                index += 2;
                if (depth == 0) {
                    return index;
                }
            } else {
                index++;
            }
        }
        throw new IllegalArgumentException("SQL query template contains an unclosed block comment");
    }

    private static int skipQuoted(String sql, int index, char closing) {
        index++;
        while (index < sql.length()) {
            if (closing != ']' && sql.charAt(index) == '\\'
                    && index + 1 < sql.length() && sql.charAt(index + 1) == closing) {
                throw new IllegalArgumentException(
                        "SQL query template must use doubled delimiters instead of backslash escapes");
            }
            if (sql.charAt(index) != closing) {
                index++;
                continue;
            }
            if (index + 1 < sql.length() && sql.charAt(index + 1) == closing) {
                index += 2;
                continue;
            }
            return index + 1;
        }
        throw new IllegalArgumentException("SQL query template contains an unclosed quoted value or identifier");
    }

    /**
     * MySQL 只有在双减号后跟 ASCII 空格、控制字符或文本结束时才把它视为注释；其他受支持方言按
     * 普通双减号注释处理。实际方言会在模板引擎创建时确定。
     */
    static boolean isLineCommentStart(String sql, int index, boolean mysql) {
        if (index + 1 >= sql.length() || sql.charAt(index) != '-' || sql.charAt(index + 1) != '-') {
            return false;
        }
        if (!mysql || index + 2 >= sql.length()) {
            return true;
        }
        char following = sql.charAt(index + 2);
        return following == ' ' || Character.isISOControl(following);
    }

    private static int skipIdentifierSlot(String sql, int index) {
        int end = sql.indexOf('}', index);
        if (end < 0) {
            throw new IllegalArgumentException("SQL template identifier slot is not closed");
        }
        requireName(sql.substring(index, end), "identifier slot");
        return end + 1;
    }

    private static int skipIdentifier(String sql, int index) {
        int end = index + 1;
        while (end < sql.length() && Character.isJavaIdentifierPart(sql.charAt(end))) {
            end++;
        }
        return end;
    }

    /** 返回当前位置的 PostgreSQL dollar quote 分隔符，不是合法起点时返回 {@code null}。 */
    static String dollarQuoteDelimiterAt(String sql, int index) {
        if (index > 0 && isPostgresqlIdentifierPart(sql.charAt(index - 1))) {
            return null;
        }
        int end = sql.indexOf('$', index + 1);
        if (end < 0) {
            return null;
        }
        String tag = sql.substring(index + 1, end);
        if (!tag.isEmpty() && !Character.isJavaIdentifierStart(tag.charAt(0))) {
            return null;
        }
        for (int tagIndex = 1; tagIndex < tag.length(); tagIndex++) {
            if (!Character.isJavaIdentifierPart(tag.charAt(tagIndex))) {
                return null;
            }
        }
        return sql.substring(index, end + 1);
    }

    private static boolean isPostgresqlIdentifierPart(char character) {
        return Character.isLetterOrDigit(character) || character == '_' || character == '$';
    }

    private static int skipDollarQuoted(String sql, int index, String delimiter) {
        int end = sql.indexOf(delimiter, index + delimiter.length());
        if (end < 0) {
            throw new IllegalArgumentException("SQL query template contains an unclosed dollar-quoted value");
        }
        return end + delimiter.length();
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }
}
