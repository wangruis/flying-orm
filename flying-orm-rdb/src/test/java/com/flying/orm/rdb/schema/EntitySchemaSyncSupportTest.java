package com.flying.orm.rdb.schema;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证实体模型启动同步的内部输入归一化边界，不允许将调用方的无界配置键写入公开异常。
 *
 * <p>本类只覆盖纯归一化逻辑，不触发 DDL 或连接资源；测试之间无共享可变状态。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class EntitySchemaSyncSupportTest {

    /** 验证同一表的空白变体配置冲突只返回稳定分类，不回显原始配置键。 */
    @Test
    void rejectsDuplicateApprovalTablesWithoutEchoingRawKey() {
        String rawTable = " ".repeat(4_096) + "approval-secret-table" + " ".repeat(4_096);
        Map<String, SchemaMigrationApproval> approvals = new LinkedHashMap<>();
        approvals.put("approval-secret-table", approval());
        approvals.put(rawTable, approval());

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> EntitySchemaSyncSupport.normalizedApprovals(approvals));

        assertEquals("duplicate schema approval table", error.getMessage());
        assertFalse(error.getMessage().contains(rawTable));
    }

    private static SchemaMigrationApproval approval() {
        return new SchemaMigrationApproval("fingerprint", "approved for test");
    }
}
