package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.result.DynamicRow;

/**
 * 接收批量写入中某一行由数据库生成的主键。
 *
 * <p>{@code inputOffset} 是这一行在整批输入里的全局位置，不是缓冲区内下标。执行内核必须按每个输入位置
 * 恰好调用一次，并且只能在已经确认拿到唯一、非空的生成键后调用。回调抛出的异常属于本次写入失败，
 * 必须停止后续批量 SQL；已经确认的执行事实与已回填键仍然保留。</p>
 *
 * <p>回填只表示数据库已经分配了这个键，不表示持久化终态。后续 SQL 或上层操作失败时，
 * ORM 不会清空已回填的键；最终持久化策略由上层负责。</p>
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
