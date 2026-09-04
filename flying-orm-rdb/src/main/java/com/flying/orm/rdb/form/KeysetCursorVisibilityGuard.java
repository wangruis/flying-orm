package com.flying.orm.rdb.form;

import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseOrigin;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;

/**
 * 审批 keyset 游标公开值的可见性。
 *
 * <p>{@code CursorPosition} 当前直接携带排序值，并不是不透明令牌。因此每个最终排序字段，包括 ORM
 * 自动补入的稳定键，都必须允许向调用方完整发布。该检查只发生在受治理查询的规划阶段。</p>
 */
final class KeysetCursorVisibilityGuard {

    private KeysetCursorVisibilityGuard() {
    }

    static void collectRequirements(FieldUseRequirements.Builder requirements,
                                    KeysetPageNormalizer.NormalizedKeysetPage page) {
        for (int index = 0; index < page.sorts().size(); index++) {
            String field = page.sorts().get(index).field();
            // CursorPosition 直接发布排序值，因此 SORT 之外还要按 caller PROJECT 审批明文可见性。
            requirements.require(field, FieldUse.PROJECT, FieldUseOrigin.CALLER);
            requirements.require(
                    field,
                    FieldUse.SORT,
                    index < page.callerSortCount()
                            ? FieldUseOrigin.CALLER : FieldUseOrigin.INTERNAL_TIE_BREAKER);
        }
    }

    static void requireFull(String resource,
                            KeysetPageNormalizer.NormalizedKeysetPage page,
                            FieldUseSnapshot snapshot) {
        for (var sort : page.sorts()) {
            String field = sort.field();
            if (snapshot.visibility(field) != FieldVisibility.FULL) {
                throw new ScopeAccessException(
                        ScopeErrorCode.FIELD_NOT_READABLE,
                        resource,
                        field,
                        "keyset cursor field [" + field + "] requires FULL visibility");
            }
        }
    }
}
