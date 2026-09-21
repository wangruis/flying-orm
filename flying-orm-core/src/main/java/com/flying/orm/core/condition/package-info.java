/**
 * 参数驱动条件的 AST、前端结构化输入、安全策略和业务 term 扩展点。
 *
 * <p>前端只能提交字段、operator、参数值和 and/or 结构，不能提交 SQL、列片段或函数表达式。
 * {@link com.flying.orm.core.condition.StructuredConditionCompiler} 会先检查字段白名单、operator 白名单、
 * 嵌套深度和值形状，再生成只包含结构化语义的条件树。</p>
 *
 * <p>业务条件通过 term 扩展，不需要往编译器里增加硬编码分支。条件值仍要作为绑定参数进入 SQL，
 * 不能在 term 中把业务输入直接拼进 SQL 文本。</p>
 */
package com.flying.orm.core.condition;
