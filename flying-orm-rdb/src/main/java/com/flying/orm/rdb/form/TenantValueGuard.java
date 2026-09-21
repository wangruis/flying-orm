package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;

import java.util.Objects;

/**
 * 租户值比较只认框架明确支持的文本形状，不调用任意业务对象的 {@code toString()}。
 *
 * @author wangr
 * @version v3.2
 */
final class TenantValueGuard {

    private TenantValueGuard() {
    }

    static void requireMatching(DynamicForm form,
                                String tenantField,
                                Object suppliedValue,
                                Object scopedValue) {
        if (!equalValues(suppliedValue, scopedValue)) {
            throw new ScopeAccessException(
                    ScopeErrorCode.TENANT_VALUE_MISMATCH,
                    form.id(),
                    tenantField,
                    "tenant field [" + tenantField + "] does not match scope for form ["
                            + form.id() + "]");
        }
    }

    private static boolean equalValues(Object suppliedValue, Object scopedValue) {
        String suppliedText = canonicalText(suppliedValue);
        String scopedText = canonicalText(scopedValue);
        if (suppliedText != null && scopedText != null) {
            return suppliedText.equals(scopedText);
        }
        return Objects.deepEquals(suppliedValue, scopedValue);
    }

    private static String canonicalText(Object value) {
        return switch (value) {
            case CharSequence text -> text.toString();
            case Character character -> character.toString();
            case char[] characters -> new String(characters);
            case null, default -> null;
        };
    }
}
