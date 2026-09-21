/**
 * 应用值和数据库参数之间的通用类型转换入口。
 *
 * <p>{@link com.flying.orm.core.codec.ValueCodecRegistry} 在应用启动时组装，构造完成后只读，可以安全地被
 * 条件渲染、动态表单写入、批量写入和实体回读共享。业务 codec 应只处理自己明确支持的 Java 类型，
 * 范围越窄的规则越要放在前面。</p>
 *
 * <p>这个包不负责判断数据库列类型，也不生成 SQL。JSON、数据库数组和 LOB 这类必须结合字段元数据
 * 才能正确转换的值，由 RDB 层的字段 codec 处理。</p>
 */
package com.flying.orm.core.codec;
