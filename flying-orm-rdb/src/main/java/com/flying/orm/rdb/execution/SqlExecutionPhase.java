package com.flying.orm.rdb.execution;

/** 同连接执行序列的阶段，用来准确说明失败发生在会话准备、业务 SQL 还是会话清理。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum SqlExecutionPhase {
    SETUP,
    WORK,
    CLEANUP
}
