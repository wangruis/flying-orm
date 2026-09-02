package com.flying.orm.rdb.observation;

/**
 * 真正执行 SQL 的底层协议。
 *
 * <p>它只描述本次调用最终走了 JDBC 还是 R2DBC，不参与方言选择，也不把两套执行模型硬塞进同一个接口。
 * {@link #UNKNOWN} 只用于兼容手工创建的旧观测事件；框架实际发出的事件必须给出明确后端。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public enum SqlExecutionBackend {

    /** 事件由旧调用或外部扩展手工创建，无法可靠判断执行后端。 */
    UNKNOWN,

    /** 使用 DataSource、Connection 和 PreparedStatement 同步执行。 */
    JDBC,

    /** 使用 R2DBC ConnectionFactory 和 Publisher 非阻塞执行。 */
    R2DBC
}
