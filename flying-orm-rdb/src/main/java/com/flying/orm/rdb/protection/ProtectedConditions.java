package com.flying.orm.rdb.protection;

import com.flying.orm.core.condition.TermCondition;

/**
 * 为受保护字段创建显式搜索条件；普通等号、LIKE 或范围条件不会被自动猜成盲索引查询。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
public final class ProtectedConditions {

    /** 精确保护搜索的稳定 term ID。 */
    public static final String EXACT = "protected-exact";

    /** 固定长度后缀保护搜索的稳定 term ID。 */
    public static final String SUFFIX = "protected-suffix";

    /** contains 保护搜索的稳定 term ID。 */
    public static final String CONTAINS = "protected-contains";

    private ProtectedConditions() {
    }

    /** @return 只会在显式声明 EXACT 的字段上生效的条件 */
    public static TermCondition exact(String field, Object value) {
        return TermCondition.of(field, EXACT, value);
    }

    /** @return 只会在显式声明对应长度 SUFFIX 的字段上生效的条件 */
    public static TermCondition suffix(String field, Object value) {
        return TermCondition.of(field, SUFFIX, value);
    }

    /** @return 只会在显式声明 CONTAINS 的字段上生效的条件 */
    public static TermCondition contains(String field, Object value) {
        return TermCondition.of(field, CONTAINS, value);
    }
}
