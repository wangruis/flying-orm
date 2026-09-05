package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.Objects;

/**
 * 一张表的类型化分区父表定义。
 *
 * @param strategy 受控分区策略
 * @param column 已完成实体映射的规范物理列名
 * @author wangr
 * @version v3.3
 */
public record TablePartitionDefinition(Strategy strategy, String column) {

    public TablePartitionDefinition {
        strategy = Objects.requireNonNull(strategy, "table partition strategy must not be null");
        column = Names.requireText(column, "table partition column");
    }

    /** 创建单列 RANGE 分区定义。 */
    public static TablePartitionDefinition range(String column) {
        return new TablePartitionDefinition(Strategy.RANGE, column);
    }

    /** 当前正式支持的分区策略。 */
    public enum Strategy {
        RANGE
    }
}
