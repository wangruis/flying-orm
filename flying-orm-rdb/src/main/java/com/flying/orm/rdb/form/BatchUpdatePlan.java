package com.flying.orm.rdb.form;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Objects;

/**
 * 保存一批乐观更新共用的 SQL、参数类型和 bind marker 风格。首行建立计划，后续行只提取参数并验证
 * SQL 形状没有变化，才能安全复用同一个数据库 Statement。
 *
 * <p>批量更新强制 {@link BatchRowCountPolicy#EXACTLY_ONE}：每个实体必须恰好命中一行，0 行表示版本冲突，
 * 多于 1 行表示定位条件不安全，两者都会进入结构化冲突结果。</p>
 */
record BatchUpdatePlan(SqlStatementPlan statement,
                       List<Class<?>> parameterTypes) {

    BatchUpdatePlan {
        statement = Objects.requireNonNull(statement, "batch update statement must not be null");
        parameterTypes = List.copyOf(Objects.requireNonNull(parameterTypes,
                                                            "batch update parameter types must not be null"));
    }

    Object[] parameters(SqlRequest request, long rowIndex) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "batch update request must not be null");
        // 条件节点数量变化也会改变 SQL，占位符即使总数碰巧相同也不能混入同一批。
        if (!statement.sql().equals(safeRequest.sql())
                || statement.bindMarkerStyle() != safeRequest.bindMarkerStyle()) {
            throw new IllegalArgumentException("batch update row [" + rowIndex + "] has a different SQL shape");
        }
        if (safeRequest.parameters().size() != parameterTypes.size()) {
            throw new IllegalArgumentException("batch update row [" + rowIndex + "] has a different parameter count");
        }
        return safeRequest.parameters().toArray();
    }

    BatchWriteRequest request(Publisher<Object[]> rows, BatchWriteOptions options) {
        return request(rows, options, BatchWriteCompletion.noop());
    }

    BatchWriteRequest request(Publisher<Object[]> rows,
                              BatchWriteOptions options,
                              BatchWriteCompletion completion) {
        return new BatchWriteRequest(statement,
                                     parameterTypes,
                                     rows,
                                     options,
                                     BatchRowCountPolicy.EXACTLY_ONE,
                                     BatchGeneratedKeys.none(),
                                     completion);
    }
}
