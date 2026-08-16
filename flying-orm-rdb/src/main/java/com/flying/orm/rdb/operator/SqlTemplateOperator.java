package com.flying.orm.rdb.operator;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateEngine;
import com.flying.orm.rdb.template.SqlTemplateParameterProvider;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 执行启动阶段注册的服务端 SQL 模板。
 *
 * <p>复杂联表、CTE、聚合、窗口函数和数据库专有查询都原样交给数据库，flying-orm 只处理模板 ID、受控标识符、
 * 命名参数、服务端安全参数、结果映射和执行保护。这样不用解析或改写任意复杂 SQL，也不需要再建立一套报表
 * 执行器。</p>
 *
 * <p>本对象保存单次调用的参数，是轻量可变构建器，不能跨线程或跨业务请求共享。每次执行会先拍摄参数快照，
 * 服务端参数提供器则在每次订阅时调用，因此 Reactor Context 中的租户和用户不会在单例组装阶段被提前捕获。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class SqlTemplateOperator {

    private final ReactiveSqlExecutor executor;

    private final ValueCodecRegistry valueCodecs;

    private final EntityModelRegistry entityModels;

    private final SqlTemplateParameterProvider parameterProvider;

    private final SqlTemplateExecutionState state;

    SqlTemplateOperator(ReactiveSqlExecutor executor,
                        ValueCodecRegistry valueCodecs,
                        EntityModelRegistry entityModels,
                        SqlTemplateEngine engine,
                        SqlTemplate template,
                        Set<String> serverParameters,
                        SqlTemplateParameterProvider parameterProvider) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.valueCodecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        this.entityModels = Objects.requireNonNull(entityModels, "entity model registry must not be null");
        this.state = new SqlTemplateExecutionState(engine, template, serverParameters);
        this.parameterProvider = Objects.requireNonNull(
                parameterProvider, "SQL template parameter provider must not be null");
    }

    /**
     * 绑定普通业务参数。服务端安全参数只能由统一提供器生成，调用这里伪造时立即失败。
     *
     * @param name SQL 中 {@code :name} 的参数名
     * @param value 参数值，允许为 null
     * @return 当前单次调用构建器
     */
    public SqlTemplateOperator bind(String name, Object value) {
        state.bind(name, value);
        return this;
    }

    /** 一次绑定多个普通业务参数；参数顺序仍然由 SQL 中的出现顺序决定。 */
    public SqlTemplateOperator bindAll(Map<String, ?> values) {
        state.bindAll(values);
        return this;
    }

    /** 绑定模板注册时声明的动态表名或列名；最终值仍由方言按 SQL 标识符规则引用。 */
    public SqlTemplateOperator identifier(String name, String value) {
        state.identifier(name, value);
        return this;
    }

    /** 一次绑定全部受控标识符。 */
    public SqlTemplateOperator identifiers(Map<String, String> identifiers) {
        state.identifiers(identifiers);
        return this;
    }

    /** 给本次执行设置查询超时、最大行数、结果内存和 LOB 上限；连接等待由上层连接池治理。 */
    public SqlTemplateOperator options(SqlExecutionOptions options) {
        state.options(options);
        return this;
    }

    /** 执行查询，并以紧凑只读动态行返回结果。 */
    public Flux<DynamicRow> query() {
        SqlTemplateExecutionState.Snapshot snapshot = state.snapshot();
        return request(snapshot).flatMapMany(request -> snapshot.options() == null
                ? executor.query(request)
                : executor.query(request, snapshot.options()));
    }

    /** 使用实体缓存映射计划执行类型化查询。 */
    public <T> Flux<T> query(Class<T> type) {
        return query(entityModels.rowMapper(Objects.requireNonNull(type, "query result type must not be null"),
                                            valueCodecs));
    }

    /** 使用调用方提供的 RowMapper 执行类型化查询。 */
    public <T> Flux<T> query(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        return query().map(safeMapper::map);
    }

    /** 查询零或一行；结果超过一行时明确失败。 */
    public Mono<DynamicRow> one() {
        return query().singleOrEmpty();
    }

    /** 查询并映射零或一个类型化结果。 */
    public <T> Mono<T> one(Class<T> type) {
        return query(type).singleOrEmpty();
    }

    /** 使用自定义 RowMapper 查询零或一个结果。 */
    public <T> Mono<T> one(RowMapper<T> mapper) {
        return query(mapper).singleOrEmpty();
    }

    private Mono<SqlRequest> request(SqlTemplateExecutionState.Snapshot snapshot) {
        return Mono.defer(() -> {
            if (state.serverParameters().isEmpty()) {
                return Mono.fromSupplier(() -> state.render(snapshot, Map.of()));
            }
            Publisher<? extends Map<String, ?>> supplied = Objects.requireNonNull(
                    parameterProvider.parameters(state.templateId(), state.serverParameters()),
                    "SQL template parameter provider returned null Publisher");
            return Mono.from(supplied)
                       .switchIfEmpty(Mono.error(new IllegalArgumentException(
                               "SQL template server parameter provider returned no values")))
                       .map(serverValues -> state.render(snapshot, serverValues));
        });
    }
}
