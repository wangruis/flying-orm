package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * SQL 结构计划的不可变缓存键，只描述会改变 SQL 文本或参数槽布局的形状。
 *
 * <p>缓存键不接受完整 SQL、参数值、租户值、DataScope 值、实体实例或条件树。字段、条件、分组、排序和
 * 分页均使用调用方预先生成的稳定结构标识；自定义 term 无法保证稳定时应绕过结构缓存。</p>
 *
 * @param dialect 数据库方言名称
 * @param bindMarkerStyle 参数标记风格
 * @param formFingerprint DynamicForm 结构指纹
 * @param table schema.table 或 table 物理身份；保留大小写以匹配 quoted identifier 语义
 * @param operation select、insert、update 等操作
 * @param fields 写入字段或投影字段的稳定顺序
 * @param conditionShape 条件 AST 形状，不包含条件值
 * @param groupShape 分组形状
 * @param sortShape 排序形状
 * @param pageShape 分页或游标形状
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public record SqlPlanSpec(String dialect,
                          SqlBindMarkerStyle bindMarkerStyle,
                          String formFingerprint,
                          String table,
                          String operation,
                          List<String> fields,
                          String conditionShape,
                          String groupShape,
                          String sortShape,
                          String pageShape) {

    /** 复制结构列表并规范化文本，保证作为并发缓存键后不会变化。 */
    public SqlPlanSpec {
        dialect = requireText(dialect, "sql plan dialect").toLowerCase(Locale.ROOT);
        bindMarkerStyle = Objects.requireNonNull(bindMarkerStyle, "sql plan bind marker style must not be null");
        formFingerprint = requireText(formFingerprint, "sql plan form fingerprint");
        table = normalizeTable(table);
        operation = requireText(operation, "sql plan operation").toLowerCase(Locale.ROOT);
        fields = List.copyOf(Objects.requireNonNull(fields, "sql plan fields must not be null"));
        if (fields.stream().anyMatch(field -> field == null || field.isBlank())) {
            throw new IllegalArgumentException("sql plan fields must not contain blank values");
        }
        conditionShape = requireNonNullShape(conditionShape, "condition");
        groupShape = requireNonNullShape(groupShape, "group");
        sortShape = requireNonNullShape(sortShape, "sort");
        pageShape = requireNonNullShape(pageShape, "page");
    }

    /** @return 规范化 schema；无 schema 时返回空字符串。 */
    public String schema() {
        int separator = table.indexOf('.');
        return separator < 0 ? "" : table.substring(0, separator);
    }

    /** @return 不带 schema 的规范化物理表名。 */
    public String physicalTable() {
        int separator = table.indexOf('.');
        return separator < 0 ? table : table.substring(separator + 1);
    }

    static String normalizeTable(String table) {
        String normalized = requireText(table, "sql plan table");
        int separator = normalized.indexOf('.');
        if (separator == 0 || separator == normalized.length() - 1
                || separator >= 0 && normalized.indexOf('.', separator + 1) >= 0) {
            throw new IllegalArgumentException("sql plan table must be table or schema.table");
        }
        return normalized;
    }

    private static String requireNonNullShape(String shape, String name) {
        return Objects.requireNonNull(shape, "sql plan " + name + " shape must not be null");
    }

    private static String requireText(String text, String name) {
        String safeText = Objects.requireNonNull(text, name + " must not be null").trim();
        if (safeText.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return safeText;
    }
}
