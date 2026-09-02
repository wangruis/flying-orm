package com.flying.orm.rdb.execution;

import com.flying.orm.rdb.internal.InternalApi;

import java.util.Objects;

/**
 * INSERT 已经产生更新计数或生成键行，但生成键结果未能完整读取时的内部状态载体。
 *
 * <p>它只在执行器已经观察到数据库写入证据后创建，Repository 据此补充 UNKNOWN 或 ENLISTED 状态；
 * 连接获取、参数绑定和 INSERT 本身失败不得转换成这个异常。</p>
 *
 * @author wangr
 * @date 2026-08-17
 * @version v2.0
 */
@InternalApi
public final class GeneratedKeyReadException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final long affectedRows;

    /** 创建已执行写入后的生成键读取异常。 */
    public GeneratedKeyReadException(long affectedRows, Throwable cause) {
        super("generated primary key could not be read after database write", Objects.requireNonNull(
                cause, "generated key read failure cause must not be null"));
        if (affectedRows < 0L) {
            throw new IllegalArgumentException("generated key affected rows must not be negative");
        }
        this.affectedRows = affectedRows;
    }

    /** @return 驱动已报告或生成键行已证明的影响行数。 */
    public long affectedRows() {
        return affectedRows;
    }
}
