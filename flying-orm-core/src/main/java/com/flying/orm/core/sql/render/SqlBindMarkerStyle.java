package com.flying.orm.core.sql.render;

/**
 * SqlBindMarkerStyle 说明 SQL 里的参数标记由 flying-orm 处理，还是已经按数据库写好。
 *
 * @author wangr
 * @date 2026-07-23
 * @version v1.0
 */
public enum SqlBindMarkerStyle {

    /** flying-orm 生成的统一 `?` 标记，执行器可以按驱动改写。 */
    CANONICAL,

    /** 调用方写好的数据库原生标记和运算符，执行器必须原样传递。 */
    NATIVE
}
