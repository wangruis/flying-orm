/**
 * flying-orm 对上层公开的稳定错误报告。
 *
 * <p>异常文本适合人看，不适合业务代码解析。需要返回前端或做统一异常处理时，应通过
 * {@link com.flying.orm.core.error.OrmErrorReportProvider} 读取分类、错误码和字段路径。</p>
 */
package com.flying.orm.core.error;
