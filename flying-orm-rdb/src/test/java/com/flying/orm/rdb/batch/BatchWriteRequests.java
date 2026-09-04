package com.flying.orm.rdb.batch;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import org.reactivestreams.Publisher;

import java.util.List;

/** 仅供测试从不可信 canonical SQL 构造新的 statement-based batch 请求。 */
public final class BatchWriteRequests {

    private BatchWriteRequests() {
    }

    public static BatchWriteRequest request(String sql,
                                            int parameterCount,
                                            List<Class<?>> parameterTypes,
                                            SqlBindMarkerStyle bindMarkerStyle,
                                            Publisher<Object[]> rows,
                                            BatchWriteOptions options) {
        return request(sql, parameterCount, parameterTypes, bindMarkerStyle, rows, options,
                       BatchRowCountPolicy.ANY);
    }

    public static BatchWriteRequest request(String sql,
                                            int parameterCount,
                                            List<Class<?>> parameterTypes,
                                            SqlBindMarkerStyle bindMarkerStyle,
                                            Publisher<Object[]> rows,
                                            BatchWriteOptions options,
                                            BatchRowCountPolicy rowCountPolicy) {
        return request(sql, parameterCount, parameterTypes, bindMarkerStyle, rows, options,
                       rowCountPolicy, BatchGeneratedKeys.none(), BatchWriteCompletion.noop());
    }

    public static BatchWriteRequest request(String sql,
                                            int parameterCount,
                                            List<Class<?>> parameterTypes,
                                            SqlBindMarkerStyle bindMarkerStyle,
                                            Publisher<Object[]> rows,
                                            BatchWriteOptions options,
                                            BatchRowCountPolicy rowCountPolicy,
                                            BatchWriteCompletion completion) {
        return request(sql, parameterCount, parameterTypes, bindMarkerStyle, rows, options,
                       rowCountPolicy, BatchGeneratedKeys.none(), completion);
    }

    public static BatchWriteRequest request(String sql,
                                            int parameterCount,
                                            List<Class<?>> parameterTypes,
                                            SqlBindMarkerStyle bindMarkerStyle,
                                            Publisher<Object[]> rows,
                                            BatchWriteOptions options,
                                            BatchRowCountPolicy rowCountPolicy,
                                            BatchGeneratedKeys generatedKeys,
                                            BatchWriteCompletion completion) {
        return new BatchWriteRequest(
                SqlStatementPlan.canonical(sql, bindMarkerStyle, parameterCount),
                parameterTypes,
                rows,
                options,
                rowCountPolicy,
                generatedKeys,
                completion);
    }
}
