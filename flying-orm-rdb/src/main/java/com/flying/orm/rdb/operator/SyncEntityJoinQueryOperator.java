package com.flying.orm.rdb.operator;

import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.List;
import java.util.Objects;

/**
 * 实体 Lambda 轻量 JOIN 的原生 JDBC 同步入口。
 *
 * @param <R> 根实体类型
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class SyncEntityJoinQueryOperator<R> {

    private final SyncFormClient client;
    private final EntityJoinQueryCommand<R> command;

    SyncEntityJoinQueryOperator(SyncFormClient client, SqlRenderer renderer, Class<R> rootType) {
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
        this.command = new EntityJoinQueryCommand<>(client.entityModels(), renderer, rootType);
    }

    /** 追加 INNER JOIN。 */
    public <J> SyncEntityJoinQueryOperator<R> join(Class<J> type,
                                                   EntityProperty<R, ?> left,
                                                   EntityProperty<J, ?> right) {
        command.join(JoinType.INNER, type, left, right); return this;
    }
    /** 追加 LEFT OUTER JOIN。 */
    public <J> SyncEntityJoinQueryOperator<R> leftJoin(Class<J> type,
                                                       EntityProperty<R, ?> left,
                                                       EntityProperty<J, ?> right) {
        command.join(JoinType.LEFT, type, left, right); return this;
    }
    /** 追加 RIGHT OUTER JOIN。 */
    public <J> SyncEntityJoinQueryOperator<R> rightJoin(Class<J> type,
                                                        EntityProperty<R, ?> left,
                                                        EntityProperty<J, ?> right) {
        command.join(JoinType.RIGHT, type, left, right); return this;
    }
    /** 为最近加入的实体追加根实体到该实体的复合等值 ON。 */
    public <J> SyncEntityJoinQueryOperator<R> andOn(EntityProperty<R, ?> left, EntityProperty<J, ?> right) {
        command.andOn(left, right); return this;
    }
    /** 使用稳定默认别名选择实体字段。 */
    public <S> SyncEntityJoinQueryOperator<R> select(Class<S> type, EntityProperty<S, ?> property) {
        command.select(type, property, null); return this;
    }
    /** 使用显式结果别名选择实体字段。 */
    public <S> SyncEntityJoinQueryOperator<R> selectAs(Class<S> type,
                                                       EntityProperty<S, ?> property,
                                                       String alias) {
        command.select(type, property, alias); return this;
    }
    /** 为指定实体源追加参数化 AND 条件。 */
    public <S> SyncEntityJoinQueryOperator<R> where(Class<S> type,
                                                    EntityProperty<S, ?> property,
                                                    String operator,
                                                    Object value) {
        command.where(type, property, operator, value); return this;
    }
    /** 为指定实体源追加本次可信数据范围。 */
    public <S> SyncEntityJoinQueryOperator<R> scope(Class<S> type, DataScope scope) {
        command.scope(type, scope); return this;
    }
    /** 追加升序字段。 */
    public <S> SyncEntityJoinQueryOperator<R> orderByAsc(Class<S> type, EntityProperty<S, ?> property) {
        command.orderBy(type, property, PageSort.Direction.ASC); return this;
    }
    /** 追加降序字段。 */
    public <S> SyncEntityJoinQueryOperator<R> orderByDesc(Class<S> type, EntityProperty<S, ?> property) {
        command.orderBy(type, property, PageSort.Direction.DESC); return this;
    }
    /** 使用字段声明的默认展示策略。 */
    public SyncEntityJoinQueryOperator<R> declaredDisplay() {
        command.declaredDisplay(); return this;
    }
    /** 强制对已声明 masked 的投影字段执行脱敏。 */
    public SyncEntityJoinQueryOperator<R> masked() {
        command.masked(); return this;
    }
    /** 显式请求完整敏感值；上层仍必须自行完成权限判断。 */
    public SyncEntityJoinQueryOperator<R> showSensitive() {
        command.showSensitive(); return this;
    }
    /** 使用同步客户端默认保护执行查询。 */
    public List<DynamicRow> executeRows() { return client.selectJoin(command.spec()); }
    /** 使用实体 Lambda 已声明的 JOIN 排序执行原生 JDBC 页码分页。 */
    public PageResult<DynamicRow> page(PageQuery page) {
        return client.pageJoin(command.spec(), Objects.requireNonNull(page, "join page query must not be null"));
    }
    /** 使用本次显式保护执行查询。 */
    public List<DynamicRow> executeRows(SqlExecutionOptions options) {
        return client.selectJoin(command.spec(), options);
    }
    /** 使用现有平面 RowMapper 映射投影结果。 */
    public <T> List<T> execute(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "join row mapper must not be null");
        return client.selectJoinMapped(command.spec(), safeMapper);
    }
}
