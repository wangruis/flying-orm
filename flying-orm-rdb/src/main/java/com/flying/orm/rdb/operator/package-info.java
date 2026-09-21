/**
 * 面向业务调用的 DDL/DML 链式操作入口。
 *
 * <p>{@link com.flying.orm.rdb.operator.DatabaseOperator} 把动态建表、查询、更新、逻辑删除、物理删除和乐观锁
 * 等常用流程组织成易读的链式 API，底层仍复用动态表单客户端、条件 AST、方言和执行保护，不另起一套
 * SQL 实现。</p>
 *
 * <p>链式对象保存的是一次操作的构建状态，不应跨请求共享；组装完成的 DatabaseOperator 可以共享。</p>
 */
package com.flying.orm.rdb.operator;
