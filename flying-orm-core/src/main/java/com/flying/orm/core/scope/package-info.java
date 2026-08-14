/**
 * 租户、数据、字段和时间范围等访问边界模型。
 *
 * <p>Scope 是已经完成业务授权判断后的数据库访问约束。ORM 会把它与业务 where 用 AND 合并，避免 OR
 * 条件绕过租户或数据范围。无租户系统可以只使用 DataScope；SaaS 系统通常同时使用 TenantScope 和
 * DataScope。</p>
 *
 * <p>这个包不负责登录、角色计算或组织权限决策。上层应先算出最终范围，再把结果交给 ORM。</p>
 */
package com.flying.orm.core.scope;
