package com.flying.orm.rdb.batch;

/**
 * 控制批量执行器是否检查每一行的影响行数。
 *
 * <p>普通插入和 upsert 只关心整片总数，使用 {@link #ANY} 可以继续走驱动原生批处理。
 * 乐观锁更新必须知道哪一行没有匹配到旧版本，使用 {@link #EXACTLY_ONE} 会逐行确认结果。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public enum BatchRowCountPolicy {

    /** 不检查单行结果，优先使用驱动的批处理吞吐能力。 */
    ANY,

    /** 每个输入必须刚好影响一行，0 行或多行都会作为冲突返回。 */
    EXACTLY_ONE
}
