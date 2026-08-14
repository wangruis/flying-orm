/**
 * RDB 方言、版本能力和自动识别入口。
 *
 * <p>方言只承接分页、upsert、参数标记、类型和 DDL 等数据库差异，不包含数据源或连接池管理。
 * 上层通常交给 RdbDialectResolver 自动识别；只有测试或特殊多库路由才需要显式选择。</p>
 */
package com.flying.orm.rdb.dialect;
