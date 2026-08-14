package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.result.DynamicRow;

/**
 * 接收批量写入中某一行由数据库生成的主键。
 *
 * <p>{@code inputOffset} 是这一行在整批输入里的全局位置，不是分片内下标。执行内核必须按每个输入位置
 * 恰好调用一次，并且只能在已经确认拿到唯一、非空的生成键后调用。回调抛出的异常属于本次写入失败，
 * 必须进入当前 ATOMIC 整批或 INDEPENDENT 分片的回滚流程，不能继续提交。</p>
 *
 * <p>回填表示数据库已经分配了这个键，不等于事务已经提交。外部事务返回 ENLISTED、提交结果 UNKNOWN
 * 或之后发生回滚时，实体上的键也不会被清空；调用方必须用批量结果判断持久化终态。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
@FunctionalInterface
public interface BatchGeneratedKeyConsumer {

    /**
     * @param inputOffset 当前行在整批输入里的全局偏移，从 0 开始
     * @param generatedKey 驱动返回的单行生成键结果
     */
    void accept(long inputOffset, DynamicRow generatedKey);
}
