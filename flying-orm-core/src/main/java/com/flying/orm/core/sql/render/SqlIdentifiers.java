package com.flying.orm.core.sql.render;

import java.util.regex.Pattern;

/**
 * SQL 里的表名、列名只接受普通标识符。值可以使用占位符绑定，标识符却不能，因此这里是防止
 * 前端字段名、动态表名和排序字段混入 SQL 表达式的最后一道基础校验。
 *
 * <p>允许 {@code schema.table}、{@code alias.column} 这种逐段校验的限定名，但不允许引号、空格、
 * 函数、注释符或其他 SQL 片段。不同数据库的引号规则由方言层负责，本类只接收跨方言都安全的名称。</p>
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
public final class SqlIdentifiers {

    private static final Pattern PART = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private SqlIdentifiers() {
    }

    /**
     * 校验普通或点分限定标识符。每一段都必须以字母或下划线开头。
     *
     * @param value 待校验的表名、列名或别名
     * @param fieldName 兼容保留的诊断标签；公开异常始终使用固定分类，不回显该输入
     * @return 原始标识符文本
     */
    public static String requireIdentifier(String value, String fieldName) {
        return requirePlainIdentifier(value, "sql identifier");
    }

    /**
     * 校验 select 投影。这里只比普通标识符额外允许 {@code *} 和 {@code alias.*}，不接受任意表达式。
     *
     * @param value 投影文本
     * @param fieldName 兼容保留的诊断标签；公开异常始终使用固定分类，不回显该输入
     * @return 校验后的投影文本
     */
    public static String requireProjection(String value, String fieldName) {
        String text = RenderNames.requireText(value, "sql projection");
        if ("*".equals(text)) {
            return text;
        }
        if (text.endsWith(".*")) {
            return requirePlainIdentifier(text.substring(0, text.length() - 2), "sql projection") + ".*";
        }
        return requirePlainIdentifier(text, "sql projection");
    }

    private static String requirePlainIdentifier(String value, String category) {
        String text = RenderNames.requireText(value, category);
        String[] parts = text.split("\\.", -1);
        for (String part : parts) {
            if (!PART.matcher(part).matches()) {
                throw new IllegalArgumentException(category + " must be a plain identifier");
            }
        }
        return text;
    }
}
