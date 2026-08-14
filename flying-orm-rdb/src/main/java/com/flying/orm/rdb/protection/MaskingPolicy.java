package com.flying.orm.rdb.protection;

import com.flying.orm.core.protection.MaskedFieldDefinition;

/**
 * 把已解密业务文本转换为有界展示文本。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
@FunctionalInterface
public interface MaskingPolicy {

    /** @param value 完整业务文本；@param definition 字段声明；@return 脱敏文本 */
    String mask(String value, MaskedFieldDefinition definition);
}
