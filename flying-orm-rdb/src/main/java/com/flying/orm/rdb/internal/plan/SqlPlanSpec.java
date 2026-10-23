package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.metadata.RelationIdentity;

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
 * @param table 分段物理关系身份，不把展示名重新解析成关系
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
                          RelationIdentity table,
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
        table = Objects.requireNonNull(table, "sql plan relation must not be null");
        operation = requireText(operation, "sql plan operation").toLowerCase(Locale.ROOT);
        fields = List.copyOf(Objects.requireNonNull(fields, "sql plan fields must not be null"));
        for (int index = 0; index < fields.size(); index++) {
            if (fields.get(index).isBlank()) {
                throw new IllegalArgumentException("sql plan fields must not contain blank values");
            }
        }
        conditionShape = Objects.requireNonNull(conditionShape, "sql plan condition shape must not be null");
        groupShape = Objects.requireNonNull(groupShape, "sql plan group shape must not be null");
        sortShape = Objects.requireNonNull(sortShape, "sql plan sort shape must not be null");
        pageShape = Objects.requireNonNull(pageShape, "sql plan page shape must not be null");
    }

    private static String requireText(String text, String name) {
        if (text == null) {
            throw new NullPointerException(name + " must not be null");
        }
        String safeText = text.trim();
        if (safeText.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return safeText;
    }
}
