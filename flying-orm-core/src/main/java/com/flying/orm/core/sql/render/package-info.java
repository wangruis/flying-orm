/**
 * 把 SQL AST 和条件 AST 渲染成参数化 SQL 请求。
 *
 * <p>{@link com.flying.orm.core.sql.render.SqlRenderer} 只把可信的标识符和固定语法写入 SQL 文本，业务值
 * 始终保存在参数列表中。自定义 {@link com.flying.orm.core.sql.render.SqlTermHandler} 处理业务 term 时，
 * 应通过 {@link com.flying.orm.core.sql.render.SqlRenderContext#identifier(String)} 校验标识符，并通过
 * {@link com.flying.orm.core.sql.render.SqlRenderContext#parameter(Object)} 复用应用级类型转换。</p>
 *
 * <p>渲染器不获取连接，也不执行请求，因此构造完成后可以被多个并发执行器共享。</p>
 */
package com.flying.orm.core.sql.render;
