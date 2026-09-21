/**
 * PostgreSQL 原生数组的条件值和 term 扩展。
 *
 * <p>数组元素始终走驱动参数绑定，不生成 array literal。当前只承诺一维数组；在不支持原生数组的方言上，
 * 渲染阶段会明确报告不支持，不会静默改成字符串比较。</p>
 */
package com.flying.orm.rdb.array;
