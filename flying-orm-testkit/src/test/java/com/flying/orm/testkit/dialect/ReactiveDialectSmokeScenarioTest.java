package com.flying.orm.testkit.dialect;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 这里只守住动态表名入口，真实库链路后面显式跑。
 *
 * @author wangr
 * @date 2026-07-26
 * @version v1.0
 */
class ReactiveDialectSmokeScenarioTest {

    @Test
    void rejectsUnsafeTableName() {
        assertThrows(IllegalArgumentException.class,
                     () -> ReactiveDialectSmokeScenario.forTable("Users;drop table Users"));
    }
}
