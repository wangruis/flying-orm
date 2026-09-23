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
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * {@code operator.dml().query()} 使用的响应式查询门面。
 *
 * <p>构建状态和安全 SQL 生成由 {@link DmlQueryCommand} 统一处理，本类只选择真正非阻塞的 R2DBC 执行器。
 * 构建器可变，只应在单次调用、单个线程内使用；执行器和渲染器仍可作为单例并发共享。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class QueryOperator {

    private final ReactiveSqlExecutor executor;
    private final ReactiveFormClient formClient;
    private final DmlQueryCommand command;

    QueryOperator(ReactiveSqlExecutor executor, SqlRenderer renderer, DataScope defaultDataScope) {
        this(null, executor, renderer, defaultDataScope);
    }

    QueryOperator(ReactiveFormClient formClient,
                  ReactiveSqlExecutor executor,
                  SqlRenderer renderer,
                  DataScope defaultDataScope) {
        this.formClient = formClient;
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.command = new DmlQueryCommand(renderer, defaultDataScope);
    }

    /** 追加查询字段；没有显式投影时仍由 FieldScope 决定是否允许使用星号。 */
    public QueryOperator select(String... columns) {
        command.select(columns);
        return this;
    }

    /** 设置目标物理表，表名会立即按标识符规则校验。 */
    public QueryOperator from(String table) {
        command.from(table);
        return this;
    }

    /** 使用表单元数据查询，继承客户端的 Scope、字段治理和保护配置。 */
    public QueryOperator from(DynamicForm form) {
        command.from(form);
        return this;
    }

    /** 追加升序字段。 */
    public QueryOperator orderByAsc(String field) { command.orderBy(field, PageSort.Direction.ASC); return this; }
    /** 追加降序字段。 */
    public QueryOperator orderByDesc(String field) { command.orderBy(field, PageSort.Direction.DESC); return this; }
    /** 追加分组字段；必须显式 select 投影。 */
    public QueryOperator groupBy(String... fields) { command.groupBy(fields); return this; }
    /** 使用默认安全策略处理前端结构化条件，与服务端 where 和 Scope 取交集。 */
    public QueryOperator filter(StructuredConditionInput input) { return filter(input, StructuredConditionPolicy.defaults()); }
    /** 指定前端条件的字段、操作符和复杂度策略。 */
    public QueryOperator filter(StructuredConditionInput input, StructuredConditionPolicy policy) {
        command.filter(input, policy); return this;
    }
    /** 按字段声明展示结果。 */
    public QueryOperator declaredDisplay() { command.display(SensitiveDisplayMode.DECLARED); return this; }
    /** 对已声明脱敏字段使用脱敏展示；显式 FieldUsePolicy 优先，应在策略中设置 MASKED。 */
    public QueryOperator masked() { command.display(SensitiveDisplayMode.MASKED); return this; }
    /** 向已获授权的后端展示完整敏感值；不会放宽日志脱敏。 */
    public QueryOperator showSensitive() { command.display(SensitiveDisplayMode.FULL); return this; }

    /** 按既有实体/record 映射规则读取投影结果。 */
    public <T> Flux<T> fetch(Class<T> type) {
        if (!command.governed()) {
            return fetch(formClient.entityModels().rawRowMapper(type, formClient.entityRenderer().valueCodecs()));
        }
        var query = command.governedQuery(null);
        return configuredClient(query).select(query.spec(), type);
    }

    /** 用应用提供的映射器转换动态行。 */
    public <T> Flux<T> fetch(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        return fetchMap().map(safeMapper::map);
    }

    /** 查询零或一行；多行会报错，不静默截断。 */
    public Mono<DynamicRow> one() {
        return fetchMap().singleOrEmpty();
    }

    /** 一基页码分页；复用表单分页内核及当前排序。 */
    public Mono<PageResult<DynamicRow>> page(int page, int size) {
        var query = command.governedQuery(null);
        return configuredClient(query).page(query.spec(), new PageQuery(page, size, query.spec().sorts()));
    }

    /** 稳定游标分页，不额外查询总数。 */
    public Mono<CursorPageResult<DynamicRow>> cursorPage(CursorPageQuery page) {
        var query = command.governedQuery(null);
        return configuredClient(query).cursorPage(query.spec(), page);
    }

    /** 支持复合排序与可空字段的键集分页。 */
    public Mono<KeysetPageResult<DynamicRow>> keysetPage(KeysetPageQuery page) {
        var query = command.governedQuery(null);
        return configuredClient(query).keysetPage(query.spec(), page);
    }

    /** 在当前条件与 Scope 下声明类型化报表聚合。 */
    public Flux<AggregateRow> aggregate(Consumer<AggregateSpec.Builder> consumer) {
        var query = command.governedQuery(null);
        var report = AggregateSpec.builder(query.spec());
        Objects.requireNonNull(consumer, "aggregate consumer must not be null").accept(report);
        return configuredClient(query).aggregate(report.build());
    }

    /** 使用 DynamicForm 元数据和显式字段策略启用 governed DML；string 表入口仍为 trusted。 */
    public QueryOperator from(DynamicForm form, FieldUsePolicy policy) {
        return from(form, policy, QueryShapeLimits.defaults());
    }

    /** 使用本次字段策略和查询形状预算启用 governed DML。 */
    public QueryOperator from(DynamicForm form, FieldUsePolicy policy, QueryShapeLimits limits) {
        command.from(form, policy, limits);
        return this;
    }

    /** 替换本次业务条件；Scope 独立保留，回调只接受条件 DSL。 */
    public QueryOperator where(Consumer<WhereDsl> consumer) {
        command.where(consumer);
        return this;
    }

    /** 以 AND 追加等值条件，值始终通过参数绑定。 */
    public QueryOperator where(String field, Object value) {
        return where(field, "=", value);
    }

    /** 以 AND 追加标准或已注册的业务条件，不接受 SQL 片段。 */
    public QueryOperator where(String field, String operator, Object value) {
        command.where(field, operator, value);
        return this;
    }

    /** 使用常见的 0=未删除、1=已删除约定。 */
    public QueryOperator logicDelete(String fieldName) {
        return logicDelete(fieldName, 0, 1);
    }

    /** 声明逻辑删除字段及未删除、已删除业务值。 */
    public QueryOperator logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
        command.logicDelete(fieldName, notDeletedValue, deletedValue);
        return this;
    }

    /** 追加本次数据范围；它只会继续收紧门面已有的默认范围。 */
    public QueryOperator scope(DataScope scope) {
        command.scope(scope);
        return this;
    }

    /** 使用执行器默认保护发起真正非阻塞的查询。 */
    public Flux<DynamicRow> fetchMap() {
        if (!command.governed()) {
            return executor.query(command.toRequest());
        }
        return executeGoverned(command.governedQuery(null));
    }

    /** 使用本次显式执行保护发起真正非阻塞的查询。 */
    public Flux<DynamicRow> fetchMap(SqlExecutionOptions options) {
        if (!command.governed()) {
            return executor.query(command.toRequest(), options);
        }
        return executeGoverned(command.governedQuery(
                Objects.requireNonNull(options, "SQL execution options must not be null")));
    }

    /** 包内契约测试和同步门面复用同一不可变请求，不对外暴露绕过执行保护的入口。 */
    SqlRequest toRequest() {
        return command.toRequest();
    }

    private Flux<DynamicRow> executeGoverned(DmlQueryCommand.GovernedQuery query) {
        ReactiveFormClient client = Objects.requireNonNull(
                formClient, "governed query requires the form client");
        return query.policy() == null ? client.select(query.spec())
                : client.selectGoverned(query.spec(), query.policy(), query.limits());
    }

    private ReactiveFormClient configuredClient(DmlQueryCommand.GovernedQuery query) {
        ReactiveFormClient client = Objects.requireNonNull(formClient, "form query requires the form client");
        return query.policy() == null ? client
                : client.withQueryGovernance(query.policy(), query.limits());
    }
}
