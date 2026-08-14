package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * DynamicForm 轻量 JOIN 的响应式链式入口。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class JoinQueryOperator {

    private final ReactiveFormClient client;
    private final JoinQueryCommand command;

    JoinQueryOperator(ReactiveFormClient client, SqlRenderer renderer, DynamicForm rootForm) {
        this.client = Objects.requireNonNull(client, "form client must not be null");
        this.command = new JoinQueryCommand(rootForm, renderer);
    }

    /** 追加 INNER JOIN。 */
    public JoinQueryOperator join(DynamicForm form, String leftField, String rightField) {
        command.join(JoinType.INNER, form, leftField, rightField); return this;
    }
    /** 追加 LEFT OUTER JOIN。 */
    public JoinQueryOperator leftJoin(DynamicForm form, String leftField, String rightField) {
        command.join(JoinType.LEFT, form, leftField, rightField); return this;
    }
    /** 追加 RIGHT OUTER JOIN。 */
    public JoinQueryOperator rightJoin(DynamicForm form, String leftField, String rightField) {
        command.join(JoinType.RIGHT, form, leftField, rightField); return this;
    }
    /** 为最近加入的源追加根源到该源的复合等值 ON。 */
    public JoinQueryOperator andOn(String leftField, String rightField) {
        command.andOn(leftField, rightField); return this;
    }
    /** 使用稳定默认结果别名选择字段。 */
    public JoinQueryOperator select(DynamicForm form, String field) {
        command.select(form, field); return this;
    }
    /** 使用显式结果别名选择字段。 */
    public JoinQueryOperator selectAs(DynamicForm form, String field, String alias) {
        command.selectAs(form, field, alias); return this;
    }
    /** 为指定源追加参数化 AND 条件。 */
    public JoinQueryOperator where(DynamicForm form, String field, String operator, Object value) {
        command.where(form, field, operator, value); return this;
    }
    /** 为指定源追加本次可信数据范围。 */
    public JoinQueryOperator scope(DynamicForm form, DataScope scope) {
        command.scope(form, scope); return this;
    }
    /** 追加升序字段。 */
    public JoinQueryOperator orderByAsc(DynamicForm form, String field) {
        command.orderBy(form, field, PageSort.Direction.ASC); return this;
    }
    /** 追加降序字段。 */
    public JoinQueryOperator orderByDesc(DynamicForm form, String field) {
        command.orderBy(form, field, PageSort.Direction.DESC); return this;
    }
    /** 使用字段声明的默认展示策略。 */
    public JoinQueryOperator declaredDisplay() {
        command.declaredDisplay(); return this;
    }
    /** 强制对已声明 masked 的投影字段执行脱敏。 */
    public JoinQueryOperator masked() {
        command.masked(); return this;
    }
    /** 显式请求完整敏感值；上层仍必须自行完成权限判断。 */
    public JoinQueryOperator showSensitive() {
        command.showSensitive(); return this;
    }
    /** 使用客户端默认保护惰性执行查询。 */
    public Flux<DynamicRow> executeRows() {
        return client.selectJoin(command.spec());
    }
    /** 使用本次显式保护惰性执行查询。 */
    public Flux<DynamicRow> executeRows(SqlExecutionOptions options) {
        return client.selectJoin(command.spec(), options);
    }
    /** 使用已声明的 source-qualified 排序执行 count + 页数据查询。 */
    public Mono<PageResult<DynamicRow>> page(PageQuery page) {
        return client.pageJoin(command.spec(), Objects.requireNonNull(page, "join page query must not be null"));
    }
    /** 使用现有平面 RowMapper 映射投影结果。 */
    public <T> Flux<T> execute(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "join row mapper must not be null");
        return executeRows().map(safeMapper::map);
    }
}
