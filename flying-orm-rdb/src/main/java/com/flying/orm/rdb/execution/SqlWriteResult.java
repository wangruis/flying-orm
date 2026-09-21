package com.flying.orm.rdb.execution;

import com.flying.orm.rdb.result.DynamicRow;

import java.util.List;
import java.util.Objects;

/**
 * 一次普通写入已经确认的影响行数和数据库生成键。
 *
 * @param affectedRows  驱动确认的影响行数
 * @param generatedKeys 按驱动返回顺序排列的生成键行，没有时为空
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public record SqlWriteResult(long affectedRows, List<DynamicRow> generatedKeys) {

    public SqlWriteResult {
        if (affectedRows < 0) {
            throw new IllegalArgumentException("sql affected rows must not be negative");
        }
        generatedKeys = List.copyOf(Objects.requireNonNull(generatedKeys, "generated keys must not be null"));
    }
}
