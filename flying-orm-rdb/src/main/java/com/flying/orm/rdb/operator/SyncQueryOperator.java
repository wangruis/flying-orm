package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 动态查询的同步门面。
 *
 * <p>它和响应式 {@link QueryOperator} 共用 {@link DmlQueryCommand}，但最终只调用
 * {@link SyncSqlExecutor}，不会创建 Publisher 或等待 R2DBC。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class SyncQueryOperator {

    private final SyncSqlExecutor executor;
    private final SyncFormClient formClient;
    private final DmlQueryCommand command;

    /** 原生 JDBC 构造器，只保存同步执行能力和共享命令状态。 */
    SyncQueryOperator(SyncSqlExecutor executor, SqlRenderer renderer, DataScope defaultDataScope) {
        this(null, executor, renderer, defaultDataScope);
    }

    SyncQueryOperator(SyncFormClient formClient,
                      SyncSqlExecutor executor,
                      SqlRenderer renderer,
                      DataScope defaultDataScope) {
        this.formClient = formClient;
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        this.command = new DmlQueryCommand(renderer, defaultDataScope);
    }

    /** 追加查询字段。 */
    public SyncQueryOperator select(String... columns) {
        command.select(columns);
        return this;
    }

    /** 设置目标物理表。 */
    public SyncQueryOperator from(String table) {
        command.from(table);
        return this;
    }

    /** 使用表单元数据查询，继承客户端的 Scope、字段治理和保护配置。 */
    public SyncQueryOperator from(DynamicForm form) {
        command.from(form);
        return this;
    }

    /** 追加升序字段。 */
    public SyncQueryOperator orderByAsc(String field) { command.orderBy(field, PageSort.Direction.ASC); return this; }
    /** 追加降序字段。 */
    public SyncQueryOperator orderByDesc(String field) { command.orderBy(field, PageSort.Direction.DESC); return this; }
    /** 追加分组字段；必须显式 select 投影。 */
    public SyncQueryOperator groupBy(String... fields) { command.groupBy(fields); return this; }
    /** 使用默认安全策略处理前端结构化条件，与服务端 where 和 Scope 取交集。 */
    public SyncQueryOperator filter(StructuredConditionInput input) { return filter(input, StructuredConditionPolicy.defaults()); }
    /** 指定前端条件的字段、操作符和复杂度策略。 */
    public SyncQueryOperator filter(StructuredConditionInput input, StructuredConditionPolicy policy) {
        command.filter(input, policy); return this;
    }
    /** 按字段声明展示结果。 */
    public SyncQueryOperator declaredDisplay() { command.display(SensitiveDisplayMode.DECLARED); return this; }
    /** 对已声明脱敏字段使用脱敏展示；显式 FieldUsePolicy 优先，应在策略中设置 MASKED。 */
    public SyncQueryOperator masked() { command.display(SensitiveDisplayMode.MASKED); return this; }
    /** 向已获授权的后端展示完整敏感值；不会放宽日志脱敏。 */
    public SyncQueryOperator showSensitive() { command.display(SensitiveDisplayMode.FULL); return this; }

    /** 按既有实体/record 映射规则读取投影结果。 */
    public <T> List<T> fetch(Class<T> type) {
        if (!command.governed()) {
            return fetch(formClient.entityModels().rawRowMapper(type, formClient.entityRenderer().valueCodecs()));
        }
        var query = command.governedQuery(null);
        return configuredClient(query).select(query.spec(), type);
    }

    /** 用应用提供的映射器转换动态行。 */
    public <T> List<T> fetch(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        return mapped(safeMapper, 0);
    }

    /** 查询零或一行；多行会报错，不静默截断。 */
    public DynamicRow one() {
        List<DynamicRow> rows = mapped(row -> row, 2);
        if (rows.size() > 1) { throw new IllegalStateException("query expected zero or one row"); }
        return rows.isEmpty() ? null : rows.getFirst();
    }

    private <T> List<T> mapped(RowMapper<T> mapper, int rowLimit) {
        if (!command.governed()) {
            return executor.queryMapped(command.toRequest(), null, mapper, rowLimit);
        }
        var query = command.governedQuery(null);
        return configuredClient(query).selectMapped(query.spec(), mapper, rowLimit);
    }

    /** 一基页码分页；复用表单分页内核及当前排序。 */
    public PageResult<DynamicRow> page(int page, int size) {
        var query = command.governedQuery(null);
        return configuredClient(query).page(query.spec(), new PageQuery(page, size, query.spec().sorts()));
    }

    /** 稳定游标分页，不额外查询总数。 */
    public CursorPageResult<DynamicRow> cursorPage(CursorPageQuery page) {
        var query = command.governedQuery(null);
        return configuredClient(query).cursorPage(query.spec(), page);
    }

    /** 支持复合排序与可空字段的键集分页。 */
    public KeysetPageResult<DynamicRow> keysetPage(KeysetPageQuery page) {
        var query = command.governedQuery(null);
        return configuredClient(query).keysetPage(query.spec(), page);
    }

    /** 在当前条件与 Scope 下声明类型化报表聚合。 */
    public List<AggregateRow> aggregate(Consumer<AggregateSpec.Builder> consumer) {
        var query = command.governedQuery(null);
        var report = AggregateSpec.builder(query.spec());
        Objects.requireNonNull(consumer, "aggregate consumer must not be null").accept(report);
        return configuredClient(query).aggregate(report.build());
    }

    /** 使用 DynamicForm 元数据和显式字段策略启用 governed DML；string 表入口仍为 trusted。 */
    public SyncQueryOperator from(DynamicForm form, FieldUsePolicy policy) {
        return from(form, policy, QueryShapeLimits.defaults());
    }

    /** 使用本次字段策略和查询形状预算启用 governed DML。 */
    public SyncQueryOperator from(DynamicForm form, FieldUsePolicy policy, QueryShapeLimits limits) {
        command.from(form, policy, limits);
        return this;
    }

    /** 替换本次业务条件；Scope 独立保留，回调只接受条件 DSL。 */
    public SyncQueryOperator where(Consumer<WhereDsl> consumer) {
        command.where(consumer);
        return this;
    }

    /** 以 AND 追加等值条件，值始终通过参数绑定。 */
    public SyncQueryOperator where(String field, Object value) {
        return where(field, "=", value);
    }

    /** 以 AND 追加标准或已注册的业务条件，不接受 SQL 片段。 */
    public SyncQueryOperator where(String field, String operator, Object value) {
        command.where(field, operator, value);
        return this;
    }

    /** 使用 0/1 逻辑删除约定。 */
    public SyncQueryOperator logicDelete(String fieldName) {
        return logicDelete(fieldName, 0, 1);
    }

    /** 声明逻辑删除字段和业务值。 */
    public SyncQueryOperator logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
        command.logicDelete(fieldName, notDeletedValue, deletedValue);
        return this;
    }

    /** 追加本次数据范围，不会覆盖默认范围。 */
    public SyncQueryOperator scope(DataScope scope) {
        command.scope(scope);
        return this;
    }

    /** 使用默认执行保护返回完整的有界结果列表。 */
    public List<DynamicRow> fetchMap() {
        if (!command.governed()) {
            return executor.query(command.toRequest());
        }
        return executeGoverned(command.governedQuery(null));
    }

    /** 使用本次显式执行保护返回完整的有界结果列表。 */
    public List<DynamicRow> fetchMap(SqlExecutionOptions options) {
        if (!command.governed()) {
            return executor.query(command.toRequest(), options);
        }
        return executeGoverned(command.governedQuery(
                Objects.requireNonNull(options, "SQL execution options must not be null")));
    }

    private List<DynamicRow> executeGoverned(DmlQueryCommand.GovernedQuery query) {
        SyncFormClient client = Objects.requireNonNull(
                formClient, "governed query requires the form client");
        return query.policy() == null ? client.select(query.spec())
                : client.selectGoverned(query.spec(), query.policy(), query.limits());
    }

    private SyncFormClient configuredClient(DmlQueryCommand.GovernedQuery query) {
        SyncFormClient client = Objects.requireNonNull(formClient, "form query requires the form client");
        return query.policy() == null ? client
                : client.withQueryGovernance(query.policy(), query.limits());
    }
}
