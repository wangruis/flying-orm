package com.flying.orm.rdb.observation;

/**
 * 一次 SQL 或批量操作实际使用的事务边界。
 *
 * <p>这个值由执行器在订阅时根据当前连接来源判断，不要求使用方写死配置。它只描述本次执行事实，
 * 不绑定任何上层事务框架。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public enum SqlTransactionSource {

    /** 使用上层已经开始并绑定的事务，flying-orm 不提交、回滚或关闭这条连接。 */
    EXTERNAL,

    /** 事务由 flying-orm 为原子批量等操作开始并负责结束。 */
    INTERNAL,

    /** 没有显式事务，单条 SQL 使用连接本身的自动提交语义。 */
    AUTO_COMMIT
}
