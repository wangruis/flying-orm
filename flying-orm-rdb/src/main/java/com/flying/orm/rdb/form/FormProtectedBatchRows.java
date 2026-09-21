package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;

import java.util.Arrays;
import java.util.Map;

/**
 * 组装动态表单受保护批量行的侧索引工作。
 *
 * <p>真实参数始终保留随机密文；侧索引工作由原生执行器在绑定前剥离，
 * 不会作为参数进入业务 SQL、结果或日志。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class FormProtectedBatchRows {

    private FormProtectedBatchRows() {
    }

    /** 组装 insert/upsert 行。 */
    static Object[] insert(FormProtectionSqlSupport.WriteOperation protection,
                           Map<String, Object> logical,
                           BatchInsertPlan plan,
                           Object[] parameters,
                           boolean upsert,
                           BatchLayout layout) {
        ProtectedWriteWork work = null;
        if (layout.contains() != null) {
            com.flying.orm.core.sql.render.SqlRequest request = new com.flying.orm.core.sql.render.SqlRequest(
                    plan.sql(), Arrays.asList(parameters), plan.bindMarkerStyle());
            work = protection.protectedWrite(
                    logical, request, null,
                    upsert ? ProtectedWriteWork.Kind.UPSERT : ProtectedWriteWork.Kind.INSERT,
                    protection.insertOwner(plan, parameters)).orElse(null);
        }
        return extend(parameters, work);
    }

    /** 组装逐行乐观更新参数。 */
    static Object[] update(FormProtectionSqlSupport.WriteOperation protection,
                           FormScopeSupport.PreparedBatchUpdate prepared,
                           Object[] parameters,
                           BatchLayout layout) {
        ProtectedWriteWork work = null;
        if (layout.contains() != null) {
            work = protection.protectedWrite(
                    prepared.logicalValues(), prepared.request(), prepared.ownerQuery(),
                    ProtectedWriteWork.Kind.UPDATE, Map.of()).orElse(null);
        }
        return extend(parameters, work);
    }

    /** @return 是否必须走能够理解内部受保护行元数据的执行入口 */
    static BatchLayout layout(DynamicForm form, BatchWriteOptions options) {
        DynamicForm safeForm = java.util.Objects.requireNonNull(form, "dynamic form must not be null");
        java.util.Objects.requireNonNull(options, "batch write options must not be null");
        return new BatchLayout(ProtectedContainsLayout.resolve(safeForm).orElse(null));
    }

    static boolean requiresProtectedExecution(BatchLayout layout) {
        return layout.contains() != null;
    }

    private static Object[] extend(Object[] parameters, ProtectedWriteWork work) {
        return work == null ? parameters : ProtectedBatchRows.extend(parameters, work);
    }

    record BatchLayout(ProtectedContainsLayout contains) {
    }
}
