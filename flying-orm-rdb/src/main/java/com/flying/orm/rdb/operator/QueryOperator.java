package com.flying.orm.rdb.operator;

import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

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
public final class QueryOperator extends DmlQueryCommand<QueryOperator> {

    private final ReactiveSqlExecutor executor;
    private final ReactiveFormClient formClient;
    private final RdbDialect dialect;

    QueryOperator(ReactiveFormClient formClient,
                  ReactiveSqlExecutor executor,
                  SqlRenderer renderer,
                  DataScope defaultDataScope, RdbDialect dialect) {
        super(renderer, defaultDataScope);
        this.formClient = formClient;
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.dialect = Objects.requireNonNull(dialect, "RDB dialect must not be null");
    }

    private QueryOperator(QueryOperator source) {
        super(source);
        this.formClient = source.formClient;
        this.executor = source.executor;
        this.dialect = source.dialect;
    }

    @Override
    protected QueryOperator self() { return this; }

    /** 复制当前查询；后续修改条件、投影、排序或目标互不影响，保留 Scope、过滤及字段策略。 */
    public QueryOperator copy() {
        return new QueryOperator(this);
    }

    /** 使用完整查询输入中的分页窗口，同时返回总数。 */
    public Mono<PageResult<DynamicRow>> page() {
        PageQuery page = inputPage();
        return page(page.page(), page.size());
    }

    /** 使用完整查询输入中的窗口映射分页结果。 */
    public <R> Mono<PageResult<R>> page(Class<R> type) {
        PageQuery page = inputPage();
        return page(page.page(), page.size(), type);
    }

    /** 按既有实体/record 映射规则读取投影结果。 */
    public <T> Flux<T> fetch(Class<T> type) {
        if (!governed()) {
            return fetch(formClient.entityModels().rawRowMapper(type, formClient.entityRenderer().valueCodecs()));
        }
        GovernedQuery query = governedQuery(null);
        return configuredClient(query).select(query.spec(), type);
    }

    /** 用应用提供的映射器转换动态行。 */
    public <T> Flux<T> fetch(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        return fetch().map(safeMapper::map);
    }

    /** 查询零或一行；多行会报错，不静默截断。 */
    public Mono<DynamicRow> one() {
        return fetch().singleOrEmpty();
    }

    /** 按实体/record 映射零或一行；多行会报错，不静默截断。 */
    public <T> Mono<T> one(Class<T> type) {
        return fetch(type).singleOrEmpty();
    }

    /** 只读取首条结果；不查询总数，未排序时由数据库决定首行。 */
    public Mono<DynamicRow> first() { return firstRow(null); }
    /** 使用本次执行保护读取首条结果。 */
    public Mono<DynamicRow> first(SqlExecutionOptions options) {
        return firstRow(Objects.requireNonNull(options, "SQL execution options must not be null"));
    }
    /** 按实体/record 映射首条结果。 */
    public <T> Mono<T> first(Class<T> type) { return firstMapped(type, null); }
    /** 使用本次执行保护映射首条结果。 */
    public <T> Mono<T> first(Class<T> type, SqlExecutionOptions options) {
        return firstMapped(type, Objects.requireNonNull(options, "SQL execution options must not be null"));
    }
    /** 统计行数或 groupBy 后的组数；忽略结果投影与排序。 */
    public Mono<Long> count() { return countRows(null); }
    /** 使用本次执行保护统计行数或组数。 */
    public Mono<Long> count(SqlExecutionOptions options) {
        return countRows(Objects.requireNonNull(options, "SQL execution options must not be null"));
    }

    private Mono<DynamicRow> firstRow(SqlExecutionOptions options) {
        if (governed()) {
            DmlQueryCommand.GovernedQuery query = governedQuery(options);
            return configuredClient(query).first(query.spec());
        }
        SqlRequest request = toFirstRequest(dialect);
        return (options == null ? executor.query(request) : executor.query(request, options))
                .reduce((first, ignored) -> first);
    }

    private <T> Mono<T> firstMapped(Class<T> type, SqlExecutionOptions options) {
        if (governed()) {
            DmlQueryCommand.GovernedQuery query = governedQuery(options);
            return configuredClient(query).first(query.spec(), type);
        }
        RowMapper<T> mapper = formClient.entityModels().rawRowMapper(type, formClient.entityRenderer().valueCodecs());
        return firstRow(options).map(mapper::map);
    }

    private Mono<Long> countRows(SqlExecutionOptions options) {
        if (governed()) {
            DmlQueryCommand.GovernedQuery query = governedCountQuery(options);
            return configuredClient(query).count(query.spec());
        }
        SqlRequest request = toCountRequest(dialect);
        return (options == null ? executor.query(request) : executor.query(request, options))
                .map(RowMapper.count()::map).singleOrEmpty().defaultIfEmpty(0L);
    }

    /** 一基页码分页，沿用当前条件与排序；元数据入口继续复用表单分页内核。 */
    public Mono<PageResult<DynamicRow>> page(int page, int size) {
        if (!governed()) {
            return trustedPage(page, size, executor::query);
        }
        GovernedQuery query = governedQuery(null);
        return configuredClient(query).page(query.spec(), new PageQuery(page, size, query.spec().sorts()));
    }

    /** 按实体/record 映射分页结果，保留当前排序与字段治理。 */
    public <T> Mono<PageResult<T>> page(int page, int size, Class<T> type) {
        if (!governed()) {
            RowMapper<T> mapper = formClient.entityModels().rawRowMapper(type, formClient.entityRenderer().valueCodecs());
            return trustedPage(page, size, request -> executor.query(request).map(mapper::map));
        }
        DmlQueryCommand.GovernedQuery query = governedQuery(null);
        return configuredClient(query).page(query.spec(), new PageQuery(page, size, query.spec().sorts()), type);
    }

    private <T> Mono<PageResult<T>> trustedPage(int page, int size, Function<SqlRequest, Flux<T>> read) {
        PageQuery paging = PageQuery.of(page, size);
        SqlRequest data = toPageRequest(paging, dialect);
        return countRows(null).flatMap(total -> total == 0L
                ? Mono.just(PageResult.of(List.of(), 0L, paging))
                : read.apply(data).collectList().map(rows -> PageResult.of(rows, total, paging)));
    }

    /** 支持复合排序与可空字段的键集分页。 */
    public Mono<KeysetPageResult<DynamicRow>> keysetPage(KeysetPageQuery page) {
        GovernedQuery query = governedQuery(null);
        return configuredClient(query).keysetPage(query.spec(), page);
    }

    /** 按实体/record 映射复合排序与可空字段的键集分页结果。 */
    public <T> Mono<KeysetPageResult<T>> keysetPage(KeysetPageQuery page, Class<T> type) {
        DmlQueryCommand.GovernedQuery query = governedQuery(null);
        return configuredClient(query).keysetPage(query.spec(), page, type);
    }

    /** 在当前条件与 Scope 下声明类型化报表聚合。 */
    public Flux<AggregateRow> aggregate(Consumer<AggregateSpec.Builder> consumer) {
        GovernedQuery query = governedQuery(null);
        var report = AggregateSpec.builder(query.spec());
        Objects.requireNonNull(consumer, "aggregate consumer must not be null").accept(report);
        return configuredClient(query).aggregate(report.build());
    }

    /** 对当前条件下的报表分组分页，total 统计 HAVING 后的分组数。 */
    public Mono<PageResult<AggregateRow>> aggregatePage(Consumer<AggregateSpec.Builder> consumer, int page, int size) {
        DmlQueryCommand.GovernedQuery query = governedQuery(null);
        AggregateSpec.Builder report = AggregateSpec.builder(query.spec());
        Objects.requireNonNull(consumer, "aggregate consumer must not be null").accept(report);
        return configuredClient(query).aggregatePage(report.build(), PageQuery.of(page, size));
    }

    /** 使用执行器默认保护发起真正非阻塞的查询。 */
    public Flux<DynamicRow> fetch() {
        if (!governed()) {
            return executor.query(toRequest(dialect));
        }
        return executeGoverned(governedQuery(null));
    }

    /** 使用本次显式执行保护发起真正非阻塞的查询。 */
    public Flux<DynamicRow> fetch(SqlExecutionOptions options) {
        if (!governed()) {
            return executor.query(toRequest(dialect), options);
        }
        return executeGoverned(governedQuery(
                Objects.requireNonNull(options, "SQL execution options must not be null")));
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
