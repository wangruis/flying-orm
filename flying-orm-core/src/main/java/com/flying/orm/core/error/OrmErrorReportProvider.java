package com.flying.orm.core.error;

/**
 * 实现它的异常可以直接转成统一错误报告，方便 HTTP、RPC 等上层适配器统一返回。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public interface OrmErrorReportProvider {

    /**
     * 提取稳定分类、错误码和定位信息。异常 message 只用于排查，不能代替错误码判断。
     *
     * @return 可安全交给上层协议适配器的错误报告
     */
    OrmErrorReport toErrorReport();
}
