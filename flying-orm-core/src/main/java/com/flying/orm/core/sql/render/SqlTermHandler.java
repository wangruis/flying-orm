package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermCondition;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * SQL term handler 负责把一个 term id 对应的结构化条件转换为参数化 SQL 片段。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public interface SqlTermHandler {

    /**
     * 返回 term id。
     *
     * @return term id
     */
    String id();

    /**
     * 返回这个 term 接受的值形状。条件构建和 SQL 渲染共用它，避免一边允许集合、另一边却按单值校验。
     */
    default ConditionValueShape shape() {
        return ConditionValueShape.SCALAR;
    }

    /**
     * 渲染 term 条件。
     *
     * @param term    term 条件
     * @param context SQL 渲染上下文
     * @return SQL 片段
     */
    SqlFragment render(TermCondition term, SqlRenderContext context);

    /**
     * 创建等值 term handler。
     *
     * @return 等值 term handler
     */
    static SqlTermHandler equalsTo() {
        return scalar("=", "=");
    }

    /**
     * 创建 SQL 的 != 条件。
     */
    static SqlTermHandler notEquals() {
        return scalar("!=", "!=");
    }

    /**
     * 创建 SQL 标准的 {@code <>} 条件。
     */
    static SqlTermHandler notEqualsStandard() {
        return scalar("<>", "<>");
    }

    /**
     * 创建大于 term handler。
     *
     * @return 大于 term handler
     */
    static SqlTermHandler greaterThan() {
        return scalar(">", ">");
    }

    /**
     * 创建大于等于 term handler，时间范围和数字范围都可以直接复用。
     *
     * @return 大于等于 term handler
     */
    static SqlTermHandler greaterThanOrEqual() {
        return scalar(">=", ">=");
    }

    /**
     * 创建小于 term handler。
     *
     * @return 小于 term handler
     */
    static SqlTermHandler lessThan() {
        return scalar("<", "<");
    }

    /**
     * 创建小于等于 term handler，适合显式包含结束边界的范围条件。
     *
     * @return 小于等于 term handler
     */
    static SqlTermHandler lessThanOrEqual() {
        return scalar("<=", "<=");
    }

    /**
     * 创建 like term handler。
     *
     * @return like term handler
     */
    static SqlTermHandler like() {
        return scalar("like", "like");
    }

    /**
     * 创建 NOT LIKE 条件。
     */
    static SqlTermHandler notLike() {
        return scalar("not-like", "not like");
    }

    /**
     * 创建 in term handler。
     *
     * @return in term handler
     */
    static SqlTermHandler in() {
        return collection("in", "in");
    }

    /**
     * 创建 NOT IN 条件。
     */
    static SqlTermHandler notIn() {
        return collection("not-in", "not in");
    }

    /**
     * 创建闭区间 BETWEEN 条件。
     */
    static SqlTermHandler between() {
        return range("between", "between");
    }

    /**
     * 创建 NOT BETWEEN 条件。
     */
    static SqlTermHandler notBetween() {
        return range("not-between", "not between");
    }

    /**
     * 创建 IS NULL 条件，不生成参数。
     */
    static SqlTermHandler isNull() {
        return of("is-null",
                  ConditionValueShape.NONE,
                  (term, context) -> SqlFragment.of(context.identifier(term.field()) + " is null"));
    }

    /**
     * 创建 IS NOT NULL 条件，不生成参数。
     */
    static SqlTermHandler isNotNull() {
        return of("is-not-null",
                  ConditionValueShape.NONE,
                  (term, context) -> SqlFragment.of(context.identifier(term.field()) + " is not null"));
    }

    /**
     * 创建关系存在型业务 term handler，例如 `where("userId", "user-in-org", orgId)`。
     *
     * @param id                  term id
     * @param relationTable       关系表名
     * @param relationAlias       关系表别名
     * @param relationKeyColumn   关系表中指向外层字段的列名
     * @param relationValueColumn 关系表中接收 term 值的列名
     * @return 关系存在型 term handler
     */
    static SqlTermHandler relationExists(String id,
                                         String relationTable,
                                         String relationAlias,
                                         String relationKeyColumn,
                                         String relationValueColumn) {
        return RelationExistsTermHandler.of(id, relationTable, relationAlias, relationKeyColumn, relationValueColumn);
    }

    /**
     * 创建关系不存在型业务 term handler，例如排除属于指定组织、角色或租户的数据。
     *
     * @param id                  term id
     * @param relationTable       关系表名
     * @param relationAlias       关系表别名
     * @param relationKeyColumn   关系表中指向外层字段的列名
     * @param relationValueColumn 关系表中接收 term 值的列名
     * @return 关系不存在型 term handler
     */
    static SqlTermHandler relationNotExists(String id,
                                            String relationTable,
                                            String relationAlias,
                                            String relationKeyColumn,
                                            String relationValueColumn) {
        return RelationExistsTermHandler.notExists(id,
                                                   relationTable,
                                                   relationAlias,
                                                   relationKeyColumn,
                                                   relationValueColumn);
    }

    /**
     * 创建默认 SQL term handler 集合。
     *
     * @return 默认 SQL term handler 集合
     */
    static List<SqlTermHandler> defaults() {
        return List.of(equalsTo(),
                       notEquals(),
                       notEqualsStandard(),
                       greaterThan(),
                       greaterThanOrEqual(),
                       lessThan(),
                       lessThanOrEqual(),
                       like(),
                       notLike(),
                       caseInsensitiveLike("like-ignore-case", "like"),
                       caseInsensitiveLike("not-like-ignore-case", "not like"),
                       in(),
                       notIn(),
                       between(),
                       notBetween(),
                       isNull(),
                       isNotNull());
    }

    /**
     * 创建自定义 term handler。
     *
     * @param id       term id
     * @param renderer term 渲染函数
     * @return SQL term handler
     */
    static SqlTermHandler of(String id, SqlTermRenderer renderer) {
        return of(id, ConditionValueShape.SCALAR, renderer);
    }

    /**
     * 创建自定义 term handler，并把可接受的值形状和渲染逻辑放在同一个定义里。
     */
    static SqlTermHandler of(String id, ConditionValueShape shape, SqlTermRenderer renderer) {
        return new SimpleSqlTermHandler(id, shape, renderer);
    }

    private static SqlTermHandler scalar(String id, String sqlOperator) {
        return of(id, (term, context) -> new SqlFragment(
                context.identifier(term.field()) + " " + sqlOperator + " ?",
                List.of(context.parameter(term.value()))));
    }

    /** 大小写折叠交给数据库执行，避免 Java Locale 造成参数与列比较规则不一致。 */
    private static SqlTermHandler caseInsensitiveLike(String id, String sqlOperator) {
        return of(id, (term, context) -> new SqlFragment(
                "lower(" + context.identifier(term.field()) + ") " + sqlOperator + " lower(?)",
                List.of(context.parameter(term.value()))));
    }

    private static List<Object> values(Object value) {
        // 只接受明确的多值容器，避免把字符串误当成字符集合；基本类型数组通过反射逐项读取。
        if (value == null) {
            return List.of();
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> values = new ArrayList<>();
            for (Object item : iterable) {
                if (values.size() == 1_000) {
                    throw new IllegalArgumentException(
                            "multi-value term must not contain more than 1000 values");
                }
                values.add(item);
            }
            return values;
        }
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            if (length > 1_000) {
                throw new IllegalArgumentException(
                        "multi-value term must not contain more than 1000 values");
            }
            List<Object> values = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                values.add(Array.get(value, i));
            }
            return values;
        }
        throw new IllegalArgumentException("in term value must be iterable or array");
    }

    private static SqlTermHandler collection(String id, String sqlOperator) {
        return of(id, ConditionValueShape.COLLECTION, (term, context) -> {
            List<Object> values = values(term.value()).stream().map(context::parameter).toList();
            if (values.isEmpty()) {
                // 条件构建器会提前拒绝空集合；这里补住直接拼 AST 的低层入口，避免产出 in ()。
                throw new IllegalArgumentException("collection term value must not be empty");
            }
            StringJoiner placeholders = new StringJoiner(", ");
            // 占位符数量完全由值数量决定，值本身绝不会进入 SQL 文本。
            values.forEach(ignored -> placeholders.add("?"));
            return new SqlFragment(context.identifier(term.field())
                                           + " " + sqlOperator + " (" + placeholders + ")",
                                   values);
        });
    }

    private static SqlTermHandler range(String id, String sqlOperator) {
        return of(id, ConditionValueShape.RANGE, (term, context) -> {
            // BETWEEN 的“两项”约束已经由条件值策略校验，这里只负责保持参数顺序。
            List<Object> values = values(term.value()).stream().map(context::parameter).toList();
            if (values.size() != 2) {
                // 直接构造 AST 也必须保持参数数目与两个占位符一致，不能把错误拖到驱动绑定阶段。
                throw new IllegalArgumentException("range term value must contain exactly two values");
            }
            return new SqlFragment(context.identifier(term.field()) + " " + sqlOperator + " ? and ?", values);
        });
    }
}

record SimpleSqlTermHandler(String id,
                            ConditionValueShape shape,
                            SqlTermRenderer renderer) implements SqlTermHandler {

    SimpleSqlTermHandler {
        id = RenderNames.normalize(id, "sql term id");
        shape = Objects.requireNonNull(shape, "sql term value shape must not be null");
        renderer = Objects.requireNonNull(renderer, "sql term renderer must not be null");
    }

    @Override
    public SqlFragment render(TermCondition term, SqlRenderContext context) {
        return renderer.render(term, context);
    }
}
