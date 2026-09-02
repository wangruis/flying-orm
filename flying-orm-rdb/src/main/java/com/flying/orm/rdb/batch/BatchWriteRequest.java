package com.flying.orm.rdb.batch;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Objects;

/**
 * BatchWriteRequest 是分片批量写入执行器消费的共享请求，原生 JDBC 和 R2DBC 都按同一套 SQL 和参数顺序执行。
 *
 * <p>rows 在订阅时才开始消费，每次订阅都会重新执行一次写入。需要重复订阅时，调用方必须保证 Publisher
 * 本身可以重放，并确认重复写入符合业务幂等要求；不要把执行结果随手 cache 后当成普通查询结果复用。</p>
 *
 * <p>生产者把一行 {@code Object[]} 发给下游后，就把这次数组内容的使用权交给执行器。在本批执行完成前
 * 不要继续修改或循环复用同一个数组，否则分片缓冲里已经接收的参数也会跟着变化。请求对象会冻结参数类型列表，
 * 但不会复制每一行的大字段和参数数组，这样才能保持真正的流式批量和可控内存。</p>
 *
 * @param statement       唯一的已编译 SQL 结构计划
 * @param parameterTypes  每个参数位置的 Java 类型
 * @param rows            一行一组参数，不提前收集整批；发出后不能再修改该参数数组
 * @param options         批量写入选项
 * @param rowCountPolicy  是否逐行检查影响行数
 * @param generatedKeys   数据库生成键协作；普通批量为 none
 * @param completion      外部事务结束后的响应式协作
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
public record BatchWriteRequest(SqlStatementPlan statement,
                                List<Class<?>> parameterTypes,
                                Publisher<Object[]> rows,
                                BatchWriteOptions options,
                                BatchRowCountPolicy rowCountPolicy,
                                BatchGeneratedKeys generatedKeys,
                                BatchWriteCompletion completion) {

    /**
     * 检查批量请求的静态形状，具体行数据由执行器按流式方式消费。
     */
    public BatchWriteRequest {
        statement = Objects.requireNonNull(statement, "batch write statement plan must not be null");
        parameterTypes = List.copyOf(Objects.requireNonNull(parameterTypes,
                                                            "batch write parameter types must not be null"));
        if (parameterTypes.size() != statement.parameterCount()) {
            throw new IllegalArgumentException("batch write parameter type count must match parameter count");
        }
        rows = Objects.requireNonNull(rows, "batch write rows must not be null");
        options = Objects.requireNonNull(options, "batch write options must not be null");
        rowCountPolicy = Objects.requireNonNull(rowCountPolicy, "batch row count policy must not be null");
        generatedKeys = Objects.requireNonNull(generatedKeys, "batch generated keys must not be null");
        completion = Objects.requireNonNull(completion, "batch write completion must not be null");
        if (generatedKeys.required()
                && options.recovery().mode() == BatchWriteOptions.RecoveryMode.RECEIPT) {
            throw new IllegalArgumentException(
                    "batch receipt recovery does not support database-generated keys");
        }
    }

    public String sql() {
        return statement.sql();
    }

    public int parameterCount() {
        return statement.parameterCount();
    }

    public SqlBindMarkerStyle bindMarkerStyle() {
        return statement.bindMarkerStyle();
    }
}
