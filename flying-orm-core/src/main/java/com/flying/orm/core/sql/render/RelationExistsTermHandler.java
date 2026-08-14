package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermCondition;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * 关系存在型 SQL term handler，用一张关系表表达“外层字段属于某个业务集合”的条件。
 *
 * <p>典型用法是将 `where("userId", "user-in-org", orgId)` 渲染为
 * `exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id = ?)`。
 * 业务参数始终进入参数列表，不直接拼接到 SQL 文本。</p>
 *
 * @param id                  term id
 * @param relationTable       关系表名
 * @param relationAlias       关系表别名
 * @param relationKeyColumn   关系表中指向外层字段的列名
 * @param relationValueColumn 关系表中接收 term 值的列名
 * @param negated             是否渲染为 not exists
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
record RelationExistsTermHandler(String id,
                                        String relationTable,
                                        String relationAlias,
                                        String relationKeyColumn,
                                        String relationValueColumn,
                                        boolean negated) implements SqlTermHandler {

    private static final int MAX_COLLECTION_VALUES = 1_000;

    @Override
    public ConditionValueShape shape() {
        return ConditionValueShape.SCALAR_OR_COLLECTION;
    }

    /**
     * 创建关系存在型 SQL term handler，并完成基础名称校验。
     */
    public RelationExistsTermHandler {
        id = RenderNames.normalize(id, "sql term id");
        relationTable = SqlIdentifiers.requireIdentifier(relationTable, "relation table");
        relationAlias = requireIdentifierPart(relationAlias);
        relationKeyColumn = requireIdentifierPart(relationKeyColumn);
        relationValueColumn = requireIdentifierPart(relationValueColumn);
    }

    /**
     * 创建关系存在型 SQL term handler。
     *
     * @param id                  term id
     * @param relationTable       关系表名
     * @param relationAlias       关系表别名
     * @param relationKeyColumn   关系表中指向外层字段的列名
     * @param relationValueColumn 关系表中接收 term 值的列名
     * @return 关系存在型 SQL term handler
     */
    public static RelationExistsTermHandler of(String id,
                                               String relationTable,
                                               String relationAlias,
                                               String relationKeyColumn,
                                             String relationValueColumn) {
        return new RelationExistsTermHandler(id,
                                             relationTable,
                                             relationAlias,
                                             relationKeyColumn,
                                             relationValueColumn,
                                             false);
    }

    /**
     * 创建关系不存在型 SQL term handler。
     *
     * @param id                  term id
     * @param relationTable       关系表名
     * @param relationAlias       关系表别名
     * @param relationKeyColumn   关系表中指向外层字段的列名
     * @param relationValueColumn 关系表中接收 term 值的列名
     * @return 关系不存在型 SQL term handler
     */
    public static RelationExistsTermHandler notExists(String id,
                                                      String relationTable,
                                                      String relationAlias,
                                                      String relationKeyColumn,
                                                      String relationValueColumn) {
        return new RelationExistsTermHandler(id,
                                             relationTable,
                                             relationAlias,
                                             relationKeyColumn,
                                             relationValueColumn,
                                             true);
    }

    /**
     * 渲染为 exists 或 not exists 子查询，外层字段由 term.field 提供，业务值始终作为 SQL 参数绑定。
     *
     * @param term    term 条件
     * @param context SQL 渲染上下文
     * @return SQL 片段
     */
    @Override
    public SqlFragment render(TermCondition term, SqlRenderContext context) {
        TermCondition safeTerm = Objects.requireNonNull(term, "term condition must not be null");
        SqlRenderContext safeContext = Objects.requireNonNull(context, "sql render context must not be null");
        String alias = safeContext.identifier(relationAlias);
        String relationKey = alias + "." + safeContext.identifier(relationKeyColumn);
        String relationValue = alias + "." + safeContext.identifier(relationValueColumn);
        List<Object> values = values(safeTerm.value()).stream().map(safeContext::parameter).toList();
        String valueExpression = valueExpression(relationValue, values.size());
        String sql = (negated ? "not exists" : "exists")
                + " (select 1 from "
                + safeContext.identifier(relationTable)
                + " "
                + alias
                + " where "
                + relationKey
                + " = "
                + safeContext.identifier(safeTerm.field())
                + " and "
                + valueExpression
                + ")";
        return new SqlFragment(sql, values);
    }

    private static String valueExpression(String relationValue, int valueSize) {
        // 单值使用等号让 SQL 更短，多值才展开 IN；两种情况的值都只进入占位符。
        if (valueSize == 1) {
            return relationValue + " = ?";
        }
        StringJoiner placeholders = new StringJoiner(", ");
        for (int i = 0; i < valueSize; i++) {
            placeholders.add("?");
        }
        return relationValue + " in (" + placeholders + ")";
    }

    private static String requireIdentifierPart(String value) {
        String identifier = SqlIdentifiers.requireIdentifier(value, "relation identifier");
        if (identifier.indexOf('.') >= 0) {
            throw new IllegalArgumentException("relation alias and columns must be single identifiers");
        }
        return identifier;
    }

    private static List<Object> values(Object value) {
        // 同时兼容集合、对象数组和基本类型数组。保持遍历顺序，确保参数与占位符严格对应。
        if (value == null) {
            return Collections.singletonList(null);
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> values = new ArrayList<>();
            for (Object item : iterable) {
                if (values.size() == MAX_COLLECTION_VALUES) {
                    throw new IllegalArgumentException(
                            "relation exists term must not contain more than "
                                    + MAX_COLLECTION_VALUES + " values");
                }
                values.add(item);
            }
            requireNotEmpty(values);
            return values;
        }
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            if (length > MAX_COLLECTION_VALUES) {
                throw new IllegalArgumentException(
                        "relation exists term must not contain more than "
                                + MAX_COLLECTION_VALUES + " values");
            }
            List<Object> values = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                values.add(Array.get(value, i));
            }
            requireNotEmpty(values);
            return values;
        }
        return Collections.singletonList(value);
    }

    private static void requireNotEmpty(List<Object> values) {
        // 空 IN 集合在不同数据库中语法或语义不一致，因此在渲染前明确拒绝。
        if (values.isEmpty()) {
            throw new IllegalArgumentException("relation exists term values must not be empty");
        }
    }
}
