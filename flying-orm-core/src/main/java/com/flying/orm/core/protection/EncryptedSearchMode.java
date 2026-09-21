package com.flying.orm.core.protection;

/**
 * 受保护字段允许使用的数据库检索方式。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public enum EncryptedSearchMode {

    /** 使用字段和租户隔离的完整值 HMAC 做精确匹配。 */
    EXACT,

    /** 只为显式声明的固定长度后缀生成 HMAC。 */
    SUFFIX,

    /** 使用有界 trigram 辅助表检索候选并在解密后复核。 */
    CONTAINS
}
