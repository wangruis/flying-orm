package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinType;
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
 * DynamicForm 轻量 JOIN 的原生 JDBC 同步链式入口。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class SyncJoinQueryOperator {

    private final SyncFormClient client;
    private final JoinQueryCommand command;

    SyncJoinQueryOperator(SyncFormClient client, SqlRenderer renderer, DynamicForm rootForm) {
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
        this.command = new JoinQueryCommand(rootForm, renderer);
    }

    /** 追加 INNER JOIN。 */
    public SyncJoinQueryOperator join(DynamicForm form, String leftField, String rightField) {
        command.join(JoinType.INNER, form, leftField, rightField); return this;
    }
    /** 追加 LEFT OUTER JOIN。 */
    public SyncJoinQueryOperator leftJoin(DynamicForm form, String leftField, String rightField) {
        command.join(JoinType.LEFT, form, leftField, rightField); return this;
    }
    /** 追加 RIGHT OUTER JOIN。 */
    public SyncJoinQueryOperator rightJoin(DynamicForm form, String leftField, String rightField) {
        command.join(JoinType.RIGHT, form, leftField, rightField); return this;
    }
    /** 为最近加入的源追加根源到该源的复合等值 ON。 */
    public SyncJoinQueryOperator andOn(String leftField, String rightField) {
        command.andOn(leftField, rightField); return this;
    }
    /** 使用稳定默认结果别名选择字段。 */
    public SyncJoinQueryOperator select(DynamicForm form, String field) {
        command.select(form, field); return this;
    }
    /** 使用显式结果别名选择字段。 */
    public SyncJoinQueryOperator selectAs(DynamicForm form, String field, String alias) {
        command.selectAs(form, field, alias); return this;
    }
    /** 为指定源追加参数化 AND 条件。 */
    public SyncJoinQueryOperator where(DynamicForm form, String field, String operator, Object value) {
        command.where(form, field, operator, value); return this;
    }
    /** 为指定源追加本次可信数据范围。 */
    public SyncJoinQueryOperator scope(DynamicForm form, DataScope scope) {
        command.scope(form, scope); return this;
    }
    /** 追加升序字段。 */
    public SyncJoinQueryOperator orderByAsc(DynamicForm form, String field) {
        command.orderBy(form, field, PageSort.Direction.ASC); return this;
    }
    /** 追加降序字段。 */
    public SyncJoinQueryOperator orderByDesc(DynamicForm form, String field) {
        command.orderBy(form, field, PageSort.Direction.DESC); return this;
    }
    /** 使用字段声明的默认展示策略。 */
    public SyncJoinQueryOperator declaredDisplay() {
        command.declaredDisplay(); return this;
    }
    /** 强制对已声明 masked 的投影字段执行脱敏。 */
    public SyncJoinQueryOperator masked() {
        command.masked(); return this;
    }
    /** 显式请求完整敏感值；上层仍必须自行完成权限判断。 */
    public SyncJoinQueryOperator showSensitive() {
        command.showSensitive(); return this;
    }
    /** 使用同步客户端默认保护执行查询。 */
    public List<DynamicRow> executeRows() {
        return client.selectJoin(command.spec());
    }
    /** 使用本次显式保护执行查询。 */
    public List<DynamicRow> executeRows(SqlExecutionOptions options) {
        return client.selectJoin(command.spec(), options);
    }
    /** 使用已声明的 source-qualified 排序执行原生 JDBC count + 页数据查询。 */
    public PageResult<DynamicRow> page(PageQuery page) {
        return client.pageJoin(command.spec(), Objects.requireNonNull(page, "join page query must not be null"));
    }
    /** 使用现有平面 RowMapper 映射投影结果。 */
    public <T> List<T> execute(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "join row mapper must not be null");
        return client.selectJoinMapped(command.spec(), safeMapper);
    }
}
