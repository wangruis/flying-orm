package com.flying.orm.rdb.operator;

import com.flying.orm.core.sql.render.SqlRenderer;

/** 单独验证命令规划，不为测试在生产代码保留额外工厂。 */
final class PlanningDynamicWrite {
    private PlanningDynamicWrite() { }

    static final class Update extends DmlWriteCommand.Update<Update> {
        Update(SqlRenderer renderer, String table) {
            super(renderer, table, null);
        }

        @Override protected PlanningDynamicWrite.Update self() { return this; }
    }

    static final class Delete extends DmlWriteCommand.Delete<Delete> {
        Delete(SqlRenderer renderer, String table) {
            super(renderer, table, null);
        }

        @Override protected PlanningDynamicWrite.Delete self() { return this; }
    }
}
