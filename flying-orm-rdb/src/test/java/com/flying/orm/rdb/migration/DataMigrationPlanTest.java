package com.flying.orm.rdb.migration;

import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证数据迁移计划在装配阶段拒绝不安全或无法执行的步骤定义。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class DataMigrationPlanTest {

    /** 重复步骤 ID 属于公开配置错误，异常消息不能回显调用方提供的无界原始 ID。 */
    @Test
    void rejectsDuplicateStepIdsWithoutEchoingRawId() {
        String rawStepId = "migration-step-secret-" + "x".repeat(8_192);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> new DataMigrationPlan("migration", List.of(
                        step(rawStepId),
                        step(rawStepId))));

        assertEquals("duplicate data migration step id", error.getMessage());
        assertFalse(error.getMessage().contains(rawStepId));
    }

    private static DataMigrationStep step(String id) {
        return new DataMigrationStep(id,
                                     new SqlRequest("update source set state = ?", List.of("before")),
                                     new SqlRequest("update source set state = ?", List.of("after")));
    }
}
