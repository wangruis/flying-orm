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
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

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
public final class SyncQueryOperator extends DmlQueryCommand<SyncQueryOperator> {

    private final SyncSqlExecutor executor;
    private final SyncFormClient formClient;
    private final RdbDialect dialect;

    SyncQueryOperator(SyncFormClient formClient,
                      SyncSqlExecutor executor,
                      SqlRenderer renderer,
                      DataScope defaultDataScope, RdbDialect dialect) {
        super(renderer, defaultDataScope);
        this.formClient = formClient;
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        this.dialect = Objects.requireNonNull(dialect, "RDB dialect must not be null");
    }

    private SyncQueryOperator(SyncQueryOperator source) {
        super(source);
        this.formClient = source.formClient;
        this.executor = source.executor;
        this.dialect = source.dialect;
    }

    @Override
    protected SyncQueryOperator self() { return this; }

    /** 复制当前查询；后续修改条件、投影、排序或目标互不影响，保留 Scope、过滤及字段策略。 */
    public SyncQueryOperator copy() {
        return new SyncQueryOperator(this);
    }

    /** 使用完整查询输入中的分页窗口，同时返回总数。 */
    public PageResult<DynamicRow> page() {
        PageQuery page = inputPage();
        return page(page.page(), page.size());
    }

    /** 使用完整查询输入中的窗口映射分页结果。 */
    public <R> PageResult<R> page(Class<R> type) {
        PageQuery page = inputPage();
        return page(page.page(), page.size(), type);
    }

    /** 按既有实体/record 映射规则读取投影结果。 */
    public <T> List<T> fetch(Class<T> type) {
        if (!governed()) {
            return fetch(formClient.entityModels().rawRowMapper(type, formClient.entityRenderer().valueCodecs()));
        }
        GovernedQuery query = governedQuery(null);
        return configuredClient(query).select(query.spec(), type);
    }

    /** 用应用提供的映射器转换动态行。 */
    public <T> List<T> fetch(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        return mapped(safeMapper, 0);
    }

    /** 逐行消费动态结果，不收集列表；原生 JDBC 回调期间资源打开，不要重入同一连接。 */
    public long forEach(Consumer<? super DynamicRow> consumer) { return forEach(row -> row, consumer); }

    /** 逐行映射实体或 DTO；自定义执行器是否增量读取由其 queryEach 实现决定。 */
    public <T> long forEach(Class<T> type, Consumer<? super T> consumer) {
        if (!governed()) return forEach(
                formClient.entityModels().rawRowMapper(type, formClient.entityRenderer().valueCodecs()), consumer);
        DmlQueryCommand.GovernedQuery query = governedQuery(null);
        return configuredClient(query).forEach(query.spec(), type, consumer);
    }

    /** 按查询顺序调用映射器及消费者；返回成功消费数，失败立即中止并清理资源。 */
    public <T> long forEach(RowMapper<T> mapper, Consumer<? super T> consumer) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        Consumer<? super T> safeConsumer = Objects.requireNonNull(consumer, "row consumer must not be null");
        if (!governed()) return executor.queryEach(
                toRequest(dialect), null, row -> safeConsumer.accept(safeMapper.map(row)));
        DmlQueryCommand.GovernedQuery query = governedQuery(null);
        return configuredClient(query).forEach(query.spec(), safeMapper, safeConsumer);
    }

    /** 逐行处理，false 正常停止；返回数包含当前行，资源与治理契约同 forEach。 */
    public long forEachWhile(Predicate<? super DynamicRow> consumer) { return forEachWhile(row -> row, consumer); }

    /** 逐行映射实体/DTO 后决定是否继续。 */
    public <T> long forEachWhile(Class<T> type, Predicate<? super T> consumer) {
        if (!governed()) return forEachWhile(
                formClient.entityModels().rawRowMapper(type, formClient.entityRenderer().valueCodecs()), consumer);
        DmlQueryCommand.GovernedQuery query = governedQuery(null);
        return configuredClient(query).forEachWhile(query.spec(), type, consumer);
    }

    /** 自定义行映射后决定是否继续；异常立即中止并按原规则清理资源。 */
    public <T> long forEachWhile(RowMapper<T> mapper, Predicate<? super T> consumer) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        Predicate<? super T> safeConsumer = Objects.requireNonNull(consumer, "row consumer must not be null");
        if (!governed()) return executor.queryWhile(
                toRequest(dialect), null, row -> safeConsumer.test(safeMapper.map(row)));
        DmlQueryCommand.GovernedQuery query = governedQuery(null);
        return configuredClient(query).forEachWhile(query.spec(), safeMapper, safeConsumer);
    }

    /** 查询零或一行；多行会报错，不静默截断。 */
    public DynamicRow one() {
        return single(mapped(row -> row, 2));
    }

    /** 按实体/record 映射零或一行；多行会报错，沿用表单读取限制与保护。 */
    public <T> T one(Class<T> type) {
        if (!governed()) {
            return single(mapped(formClient.entityModels().rawRowMapper(
                    type, formClient.entityRenderer().valueCodecs()), 2));
        }
        DmlQueryCommand.GovernedQuery query = governedQuery(null);
        return configuredClient(query).selectOne(query.spec(), type);
    }

    private static <T> T single(List<T> rows) {
        if (rows.size() > 1) { throw new IllegalStateException("query expected zero or one row"); }
        return rows.isEmpty() ? null : rows.getFirst();
    }

    private <T> List<T> mapped(RowMapper<T> mapper, int rowLimit) {
        if (!governed()) {
            return executor.queryMapped(toRequest(dialect), null, mapper, rowLimit);
        }
        GovernedQuery query = governedQuery(null);
        return configuredClient(query).selectMapped(query.spec(), mapper, rowLimit);
    }

    /** 只读取首条结果；空结果返回 null，不查询总数。 */
    public DynamicRow first() { return firstRow(null); }
    /** 使用本次执行保护读取首条结果。 */
    public DynamicRow first(SqlExecutionOptions options) {
        return firstRow(Objects.requireNonNull(options, "SQL execution options must not be null"));
    }
    /** 按实体/record 映射首条结果。 */
    public <T> T first(Class<T> type) { return firstMapped(type, null); }
    /** 使用本次执行保护映射首条结果。 */
    public <T> T first(Class<T> type, SqlExecutionOptions options) {
        return firstMapped(type, Objects.requireNonNull(options, "SQL execution options must not be null"));
    }
    /** 统计行数或 groupBy 后的组数；忽略结果投影与排序。 */
    public long count() { return countRows(null); }
    /** 使用本次执行保护统计行数或组数。 */
    public long count(SqlExecutionOptions options) {
        return countRows(Objects.requireNonNull(options, "SQL execution options must not be null"));
    }

    private DynamicRow firstRow(SqlExecutionOptions options) {
        if (governed()) {
            DmlQueryCommand.GovernedQuery query = governedQuery(options);
            return configuredClient(query).first(query.spec());
        }
        SqlRequest request = toFirstRequest(dialect);
        List<DynamicRow> rows = options == null ? executor.query(request) : executor.query(request, options);
        return rows.isEmpty() ? null : rows.getFirst();
    }

    private <T> T firstMapped(Class<T> type, SqlExecutionOptions options) {
        if (governed()) {
            DmlQueryCommand.GovernedQuery query = governedQuery(options);
            return configuredClient(query).first(query.spec(), type);
        }
        RowMapper<T> mapper = formClient.entityModels().rawRowMapper(type, formClient.entityRenderer().valueCodecs());
        DynamicRow row = firstRow(options);
        return row == null ? null : mapper.map(row);
    }

    private long countRows(SqlExecutionOptions options) {
        if (governed()) {
            DmlQueryCommand.GovernedQuery query = governedCountQuery(options);
            return configuredClient(query).count(query.spec());
        }
        SqlRequest request = toCountRequest(dialect);
        List<DynamicRow> rows = options == null ? executor.query(request) : executor.query(request, options);
        return rows.isEmpty() ? 0L : RowMapper.count().map(rows.getFirst());
    }

    /** 一基页码分页，沿用当前条件与排序；元数据入口继续复用表单分页内核。 */
    public PageResult<DynamicRow> page(int page, int size) {
        if (!governed()) {
            return trustedPage(page, size, executor::query);
        }
        GovernedQuery query = governedQuery(null);
        return configuredClient(query).page(query.spec(), new PageQuery(page, size, query.spec().sorts()));
    }

    /** 按实体/record 映射分页结果，保留当前排序与字段治理。 */
    public <T> PageResult<T> page(int page, int size, Class<T> type) {
        if (!governed()) {
            RowMapper<T> mapper = formClient.entityModels().rawRowMapper(type, formClient.entityRenderer().valueCodecs());
            return trustedPage(page, size, request -> executor.queryMapped(request, null, mapper, 0));
        }
        DmlQueryCommand.GovernedQuery query = governedQuery(null);
        return configuredClient(query).page(query.spec(), new PageQuery(page, size, query.spec().sorts()), type);
    }

    private <T> PageResult<T> trustedPage(int page, int size, Function<SqlRequest, List<T>> read) {
        PageQuery paging = PageQuery.of(page, size);
        SqlRequest data = toPageRequest(paging, dialect);
        long total = countRows(null);
        return PageResult.of(total == 0L ? List.of() : read.apply(data), total, paging);
    }

    /** 支持复合排序与可空字段的键集分页。 */
    public KeysetPageResult<DynamicRow> keysetPage(KeysetPageQuery page) {
        GovernedQuery query = governedQuery(null);
        return configuredClient(query).keysetPage(query.spec(), page);
    }

    /** 按实体/record 映射复合排序与可空字段的键集分页结果。 */
    public <T> KeysetPageResult<T> keysetPage(KeysetPageQuery page, Class<T> type) {
        DmlQueryCommand.GovernedQuery query = governedQuery(null);
        return configuredClient(query).keysetPage(query.spec(), page, type);
    }

    /** 在当前条件与 Scope 下声明类型化报表聚合。 */
    public List<AggregateRow> aggregate(Consumer<AggregateSpec.Builder> consumer) {
        GovernedQuery query = governedQuery(null);
        var report = AggregateSpec.builder(query.spec());
        Objects.requireNonNull(consumer, "aggregate consumer must not be null").accept(report);
        return configuredClient(query).aggregate(report.build());
    }

    /** 对当前条件下的报表分组分页，total 统计 HAVING 后的分组数。 */
    public PageResult<AggregateRow> aggregatePage(Consumer<AggregateSpec.Builder> consumer, int page, int size) {
        DmlQueryCommand.GovernedQuery query = governedQuery(null);
        AggregateSpec.Builder report = AggregateSpec.builder(query.spec());
        Objects.requireNonNull(consumer, "aggregate consumer must not be null").accept(report);
        return configuredClient(query).aggregatePage(report.build(), PageQuery.of(page, size));
    }

    /** 使用默认执行保护返回完整的有界结果列表。 */
    public List<DynamicRow> fetch() {
        if (!governed()) {
            return executor.query(toRequest(dialect));
        }
        return executeGoverned(governedQuery(null));
    }

    /** 使用本次显式执行保护返回完整的有界结果列表。 */
    public List<DynamicRow> fetch(SqlExecutionOptions options) {
        if (!governed()) {
            return executor.query(toRequest(dialect), options);
        }
        return executeGoverned(governedQuery(
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
