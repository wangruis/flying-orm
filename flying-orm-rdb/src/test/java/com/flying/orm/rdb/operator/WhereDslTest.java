package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.TermCondition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 验证 operator 短 DSL 完整复用 core 的严格、可选和 NULL 条件语义。
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
class WhereDslTest {

    @Test
    void delegatesOptionalAndNullConditionsToCoreBuilder() {
        ConditionGroup group = new WhereDsl()
                .is("name", "  张三  ")
                .isIfPresent("status", " ")
                .termIfPresent("org_id", "=", null)
                .isNull("deleted_at")
                .isNotNull("created_at")
                .build();

        assertEquals(3, group.children().size());
        assertEquals("张三", ((TermCondition) group.children().get(0)).value());
        assertNull(((TermCondition) group.children().get(1)).value());
        assertNull(((TermCondition) group.children().get(2)).value());
    }
}
