/**
 * flying-orm 自己的实体映射注解。
 *
 * <p>这里故意不依赖 Jakarta Persistence 或其他 ORM 框架。注解只负责把实体模型说清楚，
 * 具体的 SQL、类型转换、主键回填和数据库差异由 core 之外的执行层实现。</p>
 */
package com.flying.orm.core.annotation;
