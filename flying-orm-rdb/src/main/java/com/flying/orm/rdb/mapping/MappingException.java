package com.flying.orm.rdb.mapping;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.util.Objects;

/**
 * 行数据映射失败时抛出，说明对象结构和查询结果对不上。
 *
 * @author wangr
 * @date 2026-07-26
 * @version v1.0
 */
public final class MappingException extends RuntimeException implements OrmErrorReportProvider {

    private static final long serialVersionUID = 1L;

    public MappingException(String message, Throwable cause) {
        super(Objects.requireNonNull(message, "mapping error message must not be null"), cause);
    }

    public MappingException(String message) {
        super(Objects.requireNonNull(message, "mapping error message must not be null"));
    }

    /** @return 不暴露行数据内容的统一映射错误报告 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("MAPPING", "MAPPING_FAILED", null, null, null, "mapping failed");
    }
}
