package com.flying.orm.rdb.operator;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;

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
    private final DmlQueryCommand command;

    QueryOperator(ReactiveSqlExecutor executor, SqlRenderer renderer, DataScope defaultDataScope) {
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

    /** 设置结构化业务条件；调用方只能构造条件 AST，不能在这里传入 SQL 片段。 */
    public QueryOperator where(Consumer<WhereDsl> consumer) {
        command.where(consumer);
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
        return executor.query(command.toRequest());
    }

    /** 使用本次显式执行保护发起真正非阻塞的查询。 */
    public Flux<DynamicRow> fetchMap(SqlExecutionOptions options) {
        return executor.query(command.toRequest(), options);
    }

    /** 包内契约测试和同步门面复用同一不可变请求，不对外暴露绕过执行保护的入口。 */
    SqlRequest toRequest() {
        return command.toRequest();
    }
}
