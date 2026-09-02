package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 组装动态表单受保护批量行的侧索引工作和稳定回执身份。
 *
 * <p>真实参数始终保留随机密文；只有回执摘要行把密文位置替换为字段隔离 HMAC。该元数据由原生
 * R2DBC 执行器在绑定前剥离，不会进入业务 SQL、结果或日志。</p>
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
                           FormPreparedWrite write,
                           BatchInsertPlan plan,
                           Object[] parameters,
                           long rowIndex,
                           boolean upsert,
                           BatchLayout layout) {
        Object[] receipt = layout.receipt()
                ? insertReceiptParameters(
                        protection, logical, write, plan, parameters, rowIndex)
                : null;
        ProtectedWriteWork work = null;
        if (layout.contains() != null) {
            com.flying.orm.core.sql.render.SqlRequest request = new com.flying.orm.core.sql.render.SqlRequest(
                    plan.sql(), Arrays.asList(parameters), plan.bindMarkerStyle());
            work = protection.protectedWrite(
                    logical, request, null,
                    upsert ? ProtectedWriteWork.Kind.UPSERT : ProtectedWriteWork.Kind.INSERT).orElse(null);
        }
        return extend(parameters, work, receipt);
    }

    /** 组装逐行乐观更新参数。 */
    static Object[] update(FormProtectionSqlSupport.WriteOperation protection,
                           FormScopeSupport.PreparedBatchUpdate prepared,
                           BatchUpdatePlan plan,
                           Object[] parameters,
                           long rowIndex,
                           BatchLayout layout) {
        Object[] receipt = layout.receipt()
                ? updateReceiptParameters(
                        protection, prepared, plan, parameters, rowIndex)
                : null;
        ProtectedWriteWork work = null;
        if (layout.contains() != null) {
            work = protection.protectedWrite(
                    prepared.logicalValues(), prepared.request(), prepared.ownerQuery(),
                    ProtectedWriteWork.Kind.UPDATE).orElse(null);
        }
        return extend(parameters, work, receipt);
    }

    /** @return 是否必须走能够理解内部受保护行元数据的执行入口 */
    static BatchLayout layout(DynamicForm form, BatchWriteOptions options) {
        DynamicForm safeForm = java.util.Objects.requireNonNull(form, "dynamic form must not be null");
        BatchWriteOptions safeOptions = java.util.Objects.requireNonNull(
                options, "batch write options must not be null");
        return new BatchLayout(ProtectedContainsLayout.resolve(safeForm).orElse(null),
                               usesProtectedReceipt(safeForm, safeOptions));
    }

    static boolean requiresProtectedExecution(BatchLayout layout) {
        return layout.contains() != null || layout.receipt();
    }

    private static Object[] insertReceiptParameters(FormProtectionSqlSupport.WriteOperation protection,
                                                     Map<String, Object> logical,
                                                     FormPreparedWrite write,
                                                     BatchInsertPlan plan,
                                                     Object[] parameters,
                                                     long rowIndex) {
        Map<String, byte[]> identities = protection.receiptIdentities(logical);
        if (identities.isEmpty()) {
            return null;
        }
        Map<String, Object> receiptValues = new LinkedHashMap<>(write.values());
        identities.forEach(receiptValues::put);
        return requireMatchingShape(parameters, plan.parameters(receiptValues, rowIndex));
    }

    private static Object[] updateReceiptParameters(FormProtectionSqlSupport.WriteOperation protection,
                                                     FormScopeSupport.PreparedBatchUpdate prepared,
                                                     BatchUpdatePlan plan,
                                                     Object[] parameters,
                                                     long rowIndex) {
        Map<String, byte[]> identities = protection.receiptIdentities(prepared.logicalValues());
        if (identities.isEmpty()) {
            return null;
        }
        Map<String, Object> receiptValues = new LinkedHashMap<>(prepared.values());
        identities.forEach(receiptValues::put);
        FormPreparedWrite receiptWrite = new FormPreparedWrite(
                prepared.form(), receiptValues);
        return requireMatchingShape(parameters, plan.parameters(protection.update(
                receiptWrite, prepared.where(), prepared.lock()), rowIndex));
    }

    private static Object[] extend(Object[] parameters,
                                   ProtectedWriteWork work,
                                   Object[] receiptParameters) {
        return work == null && receiptParameters == null
                ? parameters : ProtectedBatchRows.extend(parameters, work, receiptParameters);
    }

    private static Object[] requireMatchingShape(Object[] parameters, Object[] receiptParameters) {
        if (receiptParameters.length != parameters.length) {
            throw new IllegalStateException("protected batch receipt shape does not match write parameters");
        }
        return receiptParameters;
    }

    private static boolean usesProtectedReceipt(DynamicForm form, BatchWriteOptions options) {
        return options.recovery().mode() == BatchWriteOptions.RecoveryMode.RECEIPT
                && !form.protections().encryptedFields().isEmpty();
    }

    record BatchLayout(ProtectedContainsLayout contains, boolean receipt) {
    }
}
