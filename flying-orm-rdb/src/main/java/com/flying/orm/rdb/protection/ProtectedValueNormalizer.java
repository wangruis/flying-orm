package com.flying.orm.rdb.protection;

/**
 * 把业务文本转换成保护搜索使用的稳定形式。
 *
 * <p>自定义实现必须无状态、并发安全且确定性；不得读取租户、时钟或随机数。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
@FunctionalInterface
public interface ProtectedValueNormalizer {

    /** @param value 原始业务文本；@return 规范化结果，不能为 null */
    String normalize(String value);
}
