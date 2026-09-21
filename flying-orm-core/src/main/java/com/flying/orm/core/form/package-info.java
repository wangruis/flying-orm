/**
 * 动态表单、动态字段以及表级安全约定。
 *
 * <p>{@link com.flying.orm.core.form.DynamicForm} 是动态 DDL、Map CRUD、结构化条件和元数据缓存共同使用的
 * 表结构描述。字段名、逻辑类型、主键、租户字段、逻辑删除字段等信息在这里收口，上层不需要为同一张表
 * 维护多份互相可能不一致的定义。</p>
 *
 * <p>这些对象只描述结构和规则，不持有数据库连接，也不执行 SQL。真正的读写和改表由 RDB 模块完成。</p>
 */
package com.flying.orm.core.form;
