package com.flying.orm.rdb.observation;

/**
 * 真正执行 SQL 的底层协议。
 *
 * <p>它只描述本次调用最终走了 JDBC 还是 R2DBC，不参与方言选择，也不把两套执行模型硬塞进同一个接口。
 * 每个事件均由创建方给出明确后端。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public enum SqlExecutionBackend {

    /** 使用上层提供的 JDBC Connection 和 ORM 创建的 PreparedStatement 同步执行。 */
    JDBC,

    /** 使用上层提供的 R2DBC Connection 和 Publisher 非阻塞执行。 */
    R2DBC
}
