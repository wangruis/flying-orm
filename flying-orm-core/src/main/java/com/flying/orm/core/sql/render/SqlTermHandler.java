package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.internal.Names;

import java.util.List;
import java.util.Objects;

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
        return SqlTermHandlerSupport.scalar("=", "=");
    }

    /**
     * 创建 SQL 的 != 条件。
     */
    static SqlTermHandler notEquals() {
        return SqlTermHandlerSupport.scalar("!=", "!=");
    }

    /**
     * 创建 SQL 标准的 {@code <>} 条件。
     */
    static SqlTermHandler notEqualsStandard() {
        return SqlTermHandlerSupport.scalar("<>", "<>");
    }

    /**
     * 创建大于 term handler。
     *
     * @return 大于 term handler
     */
    static SqlTermHandler greaterThan() {
        return SqlTermHandlerSupport.scalar(">", ">");
    }

    /**
     * 创建大于等于 term handler，时间范围和数字范围都可以直接复用。
     *
     * @return 大于等于 term handler
     */
    static SqlTermHandler greaterThanOrEqual() {
        return SqlTermHandlerSupport.scalar(">=", ">=");
    }

    /**
     * 创建小于 term handler。
     *
     * @return 小于 term handler
     */
    static SqlTermHandler lessThan() {
        return SqlTermHandlerSupport.scalar("<", "<");
    }

    /**
     * 创建小于等于 term handler，适合显式包含结束边界的范围条件。
     *
     * @return 小于等于 term handler
     */
    static SqlTermHandler lessThanOrEqual() {
        return SqlTermHandlerSupport.scalar("<=", "<=");
    }

    /**
     * 创建 like term handler。
     *
     * @return like term handler
     */
    static SqlTermHandler like() {
        return SqlTermHandlerSupport.scalar("like", "like");
    }

    /**
     * 创建 NOT LIKE 条件。
     */
    static SqlTermHandler notLike() {
        return SqlTermHandlerSupport.scalar("not-like", "not like");
    }

    /**
     * 创建 in term handler。
     *
     * @return in term handler
     */
    static SqlTermHandler in() {
        return SqlTermHandlerSupport.collection("in", "in");
    }

    /**
     * 创建 NOT IN 条件。
     */
    static SqlTermHandler notIn() {
        return SqlTermHandlerSupport.collection("not-in", "not in");
    }

    /**
     * 创建闭区间 BETWEEN 条件。
     */
    static SqlTermHandler between() {
        return SqlTermHandlerSupport.range("between", "between");
    }

    /**
     * 创建 NOT BETWEEN 条件。
     */
    static SqlTermHandler notBetween() {
        return SqlTermHandlerSupport.range("not-between", "not between");
    }

    /**
     * 创建 IS NULL 条件，不生成参数。
     */
    static SqlTermHandler isNull() {
        return SqlTermHandlerSupport.structural(
                "is-null",
                ConditionValueShape.NONE,
                (term, context, output) -> output.appendSql(
                        context.identifier(term.field()) + " is null"));
    }

    /**
     * 创建 IS NOT NULL 条件，不生成参数。
     */
    static SqlTermHandler isNotNull() {
        return SqlTermHandlerSupport.structural(
                "is-not-null",
                ConditionValueShape.NONE,
                (term, context, output) -> output.appendSql(
                        context.identifier(term.field()) + " is not null"));
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
                       SqlTermHandlerSupport.caseInsensitiveLike("like-ignore-case", "like"),
                       SqlTermHandlerSupport.caseInsensitiveLike("not-like-ignore-case", "not like"),
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
        return new SimpleSqlTermHandler(id, shape, renderer, false);
    }

}

@FunctionalInterface
interface InternalSqlTermRenderer {

    void render(TermCondition term, SqlRenderContext context, SqlTermOutput output);
}

interface SqlTermOutput {

    void appendSql(String value);

    void addParameter(Object value);
}

record SimpleSqlTermHandler(String id,
                            ConditionValueShape shape,
                            SqlTermRenderer renderer,
                            boolean structuralCacheSafe) implements SqlTermHandler {

    SimpleSqlTermHandler {
        id = Names.key(id, "sql term id");
        shape = Objects.requireNonNull(shape, "sql term value shape must not be null");
        renderer = Objects.requireNonNull(renderer, "sql term renderer must not be null");
    }

    static SimpleSqlTermHandler internal(String id,
                                         ConditionValueShape shape,
                                         InternalSqlTermRenderer renderer) {
        return new SimpleSqlTermHandler(
                id,
                shape,
                new AccumulatingSqlTermRenderer(renderer),
                true);
    }

    @Override
    public SqlFragment render(TermCondition term, SqlRenderContext context) {
        return renderer.render(term, context);
    }

    boolean appendTo(TermCondition term, SqlRenderContext context, SqlTermOutput output) {
        if (!(renderer instanceof AccumulatingSqlTermRenderer accumulating)) {
            return false;
        }
        accumulating.render(term, context, output);
        return true;
    }
}

final class AccumulatingSqlTermRenderer implements SqlTermRenderer {

    private final InternalSqlTermRenderer renderer;

    AccumulatingSqlTermRenderer(InternalSqlTermRenderer renderer) {
        this.renderer = Objects.requireNonNull(renderer, "internal SQL term renderer must not be null");
    }

    @Override
    public SqlFragment render(TermCondition term, SqlRenderContext context) {
        FragmentOutput output = new FragmentOutput();
        renderer.render(term, context, output);
        return output.publish();
    }

    void render(TermCondition term, SqlRenderContext context, SqlTermOutput output) {
        renderer.render(term, context, output);
    }

    private static final class FragmentOutput implements SqlTermOutput {

        private final StringBuilder sql = new StringBuilder();

        private final OwnedBindableValues.Buffer parameters = OwnedBindableValues.buffer();

        @Override
        public void appendSql(String value) {
            sql.append(value);
        }

        @Override
        public void addParameter(Object value) {
            parameters.add(SqlFragment.bindableParameter(value));
        }

        private SqlFragment publish() {
            return new SqlFragment(sql.toString(), parameters.publish());
        }
    }
}
