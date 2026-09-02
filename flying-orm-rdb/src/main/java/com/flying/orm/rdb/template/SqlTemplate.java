package com.flying.orm.rdb.template;

import com.flying.orm.rdb.internal.template.SqlLexicalScanner;
import com.flying.orm.rdb.internal.template.SqlStatements;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * 启动阶段注册的受控 SQL 查询模板。
 *
 * <p>模板正文只能来自后端代码或可信配置，不能由前端请求直接传入。业务值写成 {@code :name}，
 * 动态表名或列名写成 {@code ${name}}，并且必须在 {@link #query(String, String, Set)} 中提前声明。
 * 这个类型没有公开构造器，所有模板都会经过同一套单语句和顶层查询结构检查。数据库函数和具体方言
 * 语义仍由可信来源负责，模板注册不宣称能够证明语句绝对只读。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class SqlTemplate {

    private final String id;

    private final String sql;

    private final Set<String> identifierSlots;

    private SqlTemplate(String id,
                        String sql,
                        Set<String> identifierSlots,
                        boolean readOnly,
                        boolean singleStatementValidated) {
        this.id = requireText(id, "SQL template id");
        this.sql = singleStatementValidated
                ? requireText(sql, "SQL statement") : SqlStatements.requireSingle(sql);
        this.identifierSlots = normalizeSlots(identifierSlots);
        if (readOnly) {
            requireQueryStart(this.sql);
        }
    }

    /**
     * 注册一条受控查询模板；它保证单条查询和顶层 SELECT/WITH 结构，不承诺数据库函数在语义上绝对无副作用。
     *
     * @param id 稳定模板 ID，执行时只按 ID 选择模板
     * @param sql 单条顶层 SELECT 或 WITH 查询
     * @param identifierSlots 允许替换的动态表名或列名槽位
     * @return 完成校验后的不可变查询模板
     */
    public static SqlTemplate query(String id, String sql, Set<String> identifierSlots) {
        return new SqlTemplate(id, sql, identifierSlots, true, false);
    }

    /** 原生 SQL 已按真实方言校验，只复用命名参数扫描模型，不再套用无方言单语句边界。 */
    static SqlTemplate nativeStatement(String sql) {
        return new SqlTemplate("direct-native-sql", sql, Set.of(), false, true);
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

    private static void requireQueryStart(String sql) {
        for (int index = 0; index < sql.length();) {
            long protectedSegment = SqlLexicalScanner.protectedSegmentAt(
                    sql, index, SqlLexicalScanner.genericRules(), false);
            if (protectedSegment >= 0L) {
                index = SqlLexicalScanner.segmentEnd(protectedSegment);
                continue;
            }
            char current = sql.charAt(index);
            if (Character.isWhitespace(current)) {
                index++;
                continue;
            }
            if (!Character.isLetter(current)) {
                break;
            }
            int end = index + 1;
            while (end < sql.length() && (Character.isLetterOrDigit(sql.charAt(end)) || sql.charAt(end) == '_')) {
                end++;
            }
            String keyword = sql.substring(index, end).toUpperCase(Locale.ROOT);
            if ("SELECT".equals(keyword) || "WITH".equals(keyword)) {
                return;
            }
            break;
        }
        throw new IllegalArgumentException("SQL query template must start with SELECT or WITH");
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }
}
