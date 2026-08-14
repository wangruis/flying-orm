package com.flying.orm.rdb.operator;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateEngine;
import com.flying.orm.rdb.template.SyncSqlTemplateParameterProvider;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 已注册 SQL 模板的同步执行入口。
 *
 * <p>原生同步路径直接向 {@link SyncSqlExecutor} 提交已经渲染好的请求。可信服务端参数由
 * {@link SyncSqlTemplateParameterProvider} 同步提供，绝不把响应式 Publisher 拿到同步线程中等待。
 * 构建器本身保存一次调用的参数，不能跨线程或跨请求共享。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class SyncSqlTemplateOperator {

    private final SyncSqlExecutor executor;

    private final ValueCodecRegistry valueCodecs;

    private final EntityModelRegistry entityModels;

    private final SyncSqlTemplateParameterProvider parameterProvider;

    private final SqlTemplateExecutionState state;

    /** 为原生 JDBC 同步链路创建一次模板调用。 */
    SyncSqlTemplateOperator(SyncSqlExecutor executor,
                            ValueCodecRegistry valueCodecs,
                            EntityModelRegistry entityModels,
                            SqlTemplateEngine engine,
                            SqlTemplate template,
                            Set<String> serverParameters,
                            SyncSqlTemplateParameterProvider parameterProvider) {
        this.executor = Objects.requireNonNull(executor, "sync SQL executor must not be null");
        this.valueCodecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        this.entityModels = Objects.requireNonNull(entityModels, "entity model registry must not be null");
        this.parameterProvider = Objects.requireNonNull(parameterProvider,
                                                        "sync SQL template parameter provider must not be null");
        this.state = new SqlTemplateExecutionState(engine, template, serverParameters);
    }

    /**
     * 绑定一个普通业务参数。租户、用户等服务端安全参数仍然不能从这里传入。
     *
     * @param name SQL 中的命名参数
     * @param value 参数值，允许为 null
     * @return 当前单次调用构建器
     */
    public SyncSqlTemplateOperator bind(String name, Object value) {
        state.bind(name, value);
        return this;
    }

    /**
     * 一次绑定多个普通业务参数。
     *
     * @param values 参数名和值
     * @return 当前单次调用构建器
     */
    public SyncSqlTemplateOperator bindAll(Map<String, ?> values) {
        state.bindAll(values);
        return this;
    }

    /**
     * 绑定注册模板明确开放的动态表名或列名，值会由方言按标识符规则引用。
     *
     * @param name 模板里的标识符槽位名
     * @param value 实际表名或列名
     * @return 当前单次调用构建器
     */
    public SyncSqlTemplateOperator identifier(String name, String value) {
        state.identifier(name, value);
        return this;
    }

    /**
     * 一次绑定模板声明的全部动态标识符。
     *
     * @param identifiers 槽位名和实际标识符
     * @return 当前单次调用构建器
     */
    public SyncSqlTemplateOperator identifiers(Map<String, String> identifiers) {
        state.identifiers(identifiers);
        return this;
    }

    /**
     * 设置数据库执行保护；同步等待上限仍由创建同步门面时传入的时长单独控制。
     *
     * @param options 查询超时、最大返回行数等保护选项
     * @return 当前单次调用构建器
     */
    public SyncSqlTemplateOperator options(SqlExecutionOptions options) {
        state.options(options);
        return this;
    }

    /** @return 全部紧凑只读动态行；等待超过同步上限时失败 */
    public List<DynamicRow> query() {
        SqlTemplateExecutionState.Snapshot snapshot = state.snapshot();
        Map<String, ?> serverValues = state.serverParameters().isEmpty()
                ? Map.of()
                : Objects.requireNonNull(parameterProvider.parameters(state.templateId(), state.serverParameters()),
                                         "sync SQL template parameter provider returned null Map");
        return SyncSqlResultOperations.query(executor, state.render(snapshot, serverValues), snapshot.options());
    }

    /**
     * @param type 结果类型
     * @param <T> 结果类型
     * @return 映射后的全部结果
     */
    public <T> List<T> query(Class<T> type) {
        return SyncSqlResultOperations.map(query(), entityModels.rowMapper(type, valueCodecs));
    }

    /**
     * @param mapper 自定义行映射器
     * @param <T> 结果类型
     * @return 映射后的全部结果
     */
    public <T> List<T> query(RowMapper<T> mapper) {
        return SyncSqlResultOperations.map(query(), mapper);
    }

    /** @return 零行时返回 null，一行时返回动态行，多于一行时失败 */
    public DynamicRow one() {
        return SyncSqlResultOperations.one(query(), "SQL template one()");
    }

    /**
     * @param type 结果类型
     * @param <T> 结果类型
     * @return 零行时返回 null，否则返回唯一的映射结果
     */
    public <T> T one(Class<T> type) {
        return SyncSqlResultOperations.one(query(type), "SQL template one()");
    }

    /**
     * @param mapper 自定义行映射器
     * @param <T> 结果类型
     * @return 零行时返回 null，否则返回唯一的映射结果
     */
    public <T> T one(RowMapper<T> mapper) {
        return SyncSqlResultOperations.one(query(mapper), "SQL template one()");
    }
}
