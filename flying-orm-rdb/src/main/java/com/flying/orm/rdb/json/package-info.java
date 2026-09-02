/**
 * JSON 字段编解码、结构化条件值和方言 term。
 *
 * <p>JSON path 会按结构化片段校验，业务值继续作为参数绑定，不能把前端路径或 JSON 文本直接拼进 SQL。
 * 反序列化只生成普通 Map、List 或 JsonNode，不根据输入中的类名实例化任意 Java 类型。</p>
 */
package com.flying.orm.rdb.json;
