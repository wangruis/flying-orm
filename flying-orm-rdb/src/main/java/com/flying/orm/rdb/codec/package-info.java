/**
 * 数据库字段感知的值转换工具，处理数组、标量、时间和大字段等驱动差异。
 *
 * <p>这些转换器不保存请求级状态，可以并发调用。LOB 的响应式读取不会调用 {@code block()}；大小限制来自执行选项。
 * 执行时限由上层或基础设施管理，codec 只应用字段物化容量限制。业务自定义类型优先通过 core 的
 * ValueCodecRegistry 扩展，不应修改这里的全局行为。</p>
 */
package com.flying.orm.rdb.codec;
