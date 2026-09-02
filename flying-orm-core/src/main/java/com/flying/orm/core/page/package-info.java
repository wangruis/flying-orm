/**
 * 分页请求和分页结果的稳定模型。
 *
 * <p>分页对象只保存页码、每页数量、总数和当前数据，不参与方言 SQL 拼接。不同数据库的 limit、offset、
 * fetch first 等语法由方言层处理，因此上层分页代码不需要知道当前连接的是哪种数据库。</p>
 */
package com.flying.orm.core.page;
