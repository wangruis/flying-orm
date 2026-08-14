package com.flying.orm.rdb.operator;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.template.SqlTemplateEngine;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;

/**
 * 后端代码直接写原生 SQL 时使用的轻量执行入口。
 *
 * <p>SQL 中的业务值必须写成 {@code :name}，执行前会统一转换成位置参数并交给 R2DBC 绑定。SQL 本身只允许
 * 一条语句，也不能由前端请求拼出来。查询返回 Map、实体或自定义映射结果，写入返回数据库报告的影响行数。</p>
 *
 * <p>这个类故意不猜 SQL 结构，也不会偷偷向任意 SQL 中插入条件，因此租户、DataScope、逻辑删除和乐观锁
 * 不会自动生效。需要这些自动保护时应使用 FormClient、Repository 或 DML operator；必须写原生 SQL 时，
 * 调用方要把对应条件明确写进 SQL，并从可信的服务端上下文绑定参数。</p>
 *
 * <p>构建器内部保存本次调用的参数，只能在单次业务操作、单个线程中使用。执行器、codec 注册表和 RowMapper
 * 仍然可以作为单例并发共享。</p>
 *
 * @author wangr
 * @date 2026-08-02
 * @version v1.0
 */
public final class NativeSqlOperator {

    private final ReactiveSqlExecutor executor;

    private final ValueCodecRegistry valueCodecs;

    private final EntityModelRegistry entityModels;

    private final NativeSqlExecutionState state;

    NativeSqlOperator(ReactiveSqlExecutor executor,
                      ValueCodecRegistry valueCodecs,
                      EntityModelRegistry entityModels,
                      RdbDialect dialect,
                      String sql) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        ValueCodecRegistry safeValueCodecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        this.valueCodecs = safeValueCodecs;
        this.entityModels = Objects.requireNonNull(entityModels, "entity model registry must not be null");
        this.state = new NativeSqlExecutionState(safeValueCodecs, dialect, sql);
    }

    /**
     * 绑定一个命名参数。相同参数名再次绑定会覆盖旧值，SQL 中重复出现的占位符会重复使用最终值。
     *
     * @param name SQL 中 {@code :name} 的名称，不包含冒号
     * @param value 参数值；允许为 null
     * @return 当前构建器
     */
    public NativeSqlOperator bind(String name, Object value) {
        state.bind(name, value);
        return this;
    }

    /**
     * 一次绑定多个命名参数。Map 的迭代顺序不会影响最终参数顺序，顺序只由 SQL 中占位符的位置决定。
     *
     * @param values 参数名和值
     * @return 当前构建器
     */
    public NativeSqlOperator bindAll(Map<String, ?> values) {
        state.bindAll(values);
        return this;
    }

    /**
     * 给本次执行设置超时、最大返回行数等保护。没有调用时沿用执行器的默认保护。
     *
     * @param options 本次执行保护
     * @return 当前构建器
     */
    public NativeSqlOperator options(SqlExecutionOptions options) {
        state.options(options);
        return this;
    }

    /** 执行查询，每一行以紧凑、只读的动态结果返回。 */
    public Flux<DynamicRow> query() {
        SqlRequest request = toRequest();
        return state.options() == null ? executor.query(request) : executor.query(request, state.options());
    }

    /**
     * 执行查询并使用目标类型的缓存映射计划转换每一行。
     *
     * @param type record 或带无参构造器的 JavaBean 类型
     * @param <T> 结果类型
     * @return 类型化结果流
     */
    public <T> Flux<T> query(Class<T> type) {
        return query(entityModels.rowMapper(type, valueCodecs));
    }

    /**
     * 执行查询并使用调用方提供的映射器转换每一行。
     *
     * @param mapper 可并发复用的行映射器
     * @param <T> 结果类型
     * @return 类型化结果流
     */
    public <T> Flux<T> query(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        return query().map(safeMapper::map);
    }

    /**
     * 执行查询并要求结果最多一行。结果为空时返回空 Mono，多于一行时由 Reactor 明确报错。
     *
     * @return 零或一行 Map
     */
    public Mono<DynamicRow> one() {
        return query().singleOrEmpty();
    }

    /** 使用目标类型映射最多一行结果。 */
    public <T> Mono<T> one(Class<T> type) {
        return query(type).singleOrEmpty();
    }

    /** 使用自定义映射器映射最多一行结果。 */
    public <T> Mono<T> one(RowMapper<T> mapper) {
        return query(mapper).singleOrEmpty();
    }

    /**
     * 执行 insert、update、delete、merge 或 DDL 等不返回普通行集的 SQL。
     *
     * @return 数据库驱动汇总后的影响行数
     */
    public Mono<Long> execute() {
        SqlRequest request = toRequest();
        return state.options() == null ? executor.rowsUpdated(request) : executor.rowsUpdated(request, state.options());
    }

    /** update 语义的易读别名，执行行为与 {@link #execute()} 相同。 */
    public Mono<Long> update() {
        return execute();
    }

    /** insert 语义的易读别名，执行行为与 {@link #execute()} 相同。 */
    public Mono<Long> insert() {
        return execute();
    }

    /** delete 语义的易读别名，执行行为与 {@link #execute()} 相同。 */
    public Mono<Long> delete() {
        return execute();
    }

    /**
     * 编译当前 SQL 和参数。方法保持包内可见，只给同包测试和 operator 内部协作使用，
     * 对外执行仍统一走 query、one 或 execute，避免调用方绕开执行保护与观测。
     *
     * @return 不可变的原生 SQL 请求
     */
    SqlRequest toRequest() {
        return state.request();
    }
}
