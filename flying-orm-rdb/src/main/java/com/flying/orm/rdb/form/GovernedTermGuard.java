package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.TermExtensionDescriptor;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.protection.ProtectedConditions;

import java.util.Objects;

/**
 * 受治理结构化查询的扩展 term 描述器与方言能力审批。
 *
 * @author wangr
 * @version v3.2
 */
final class GovernedTermGuard {

    private GovernedTermGuard() {
    }

    /** 标准 term 按对象身份直达；只有扩展 term 才读取描述器与能力集合。 */
    static void require(TermCondition term,
                        FieldUse use,
                        TermRegistry terms,
                        DialectCapabilities capabilities) {
        TermRegistry safeTerms = Objects.requireNonNull(terms, "term registry must not be null");
        if (safeTerms == TermRegistry.standard()) {
            return;
        }
        TermHandler handler = safeTerms.find(term.operator()).orElse(null);
        if (handler == null) {
            // 保护搜索由保护规划器校验并下沉；治理仍审批原逻辑字段的 FILTER 用途。
            // 注册的同名自定义 handler 必须继续经过下面的扩展描述器审批。
            if (use == FieldUse.FILTER
                    && (ProtectedConditions.EXACT.equals(term.operator())
                    || ProtectedConditions.SUFFIX.equals(term.operator())
                    || ProtectedConditions.CONTAINS.equals(term.operator()))) {
                return;
            }
            throw new IllegalArgumentException("term does not exist");
        }
        TermHandler standard = TermRegistry.standard().find(term.operator()).orElse(null);
        if (handler == standard) {
            return;
        }
        TermExtensionDescriptor descriptor = handler.descriptor().orElseThrow(() ->
                new IllegalArgumentException(
                        "governed term [" + term.operator() + "] requires an explicit extension descriptor"));
        if (descriptor.fieldUse() != use) {
            throw new IllegalArgumentException(
                    "term extension [" + descriptor.id() + "] is not allowed for " + use);
        }
        requireCapabilities(descriptor, capabilities);
    }

    private static void requireCapabilities(TermExtensionDescriptor descriptor,
                                            DialectCapabilities capabilities) {
        DialectCapabilities safeCapabilities = Objects.requireNonNull(
                capabilities, "dialect capabilities must not be null");
        for (String required : descriptor.requiredCapabilities()) {
            if (!supports(safeCapabilities, required)) {
                throw new UnsupportedOperationException(
                        "term extension [" + descriptor.id()
                                + "] requires dialect capability [" + required + "]");
            }
        }
    }

    private static boolean supports(DialectCapabilities capabilities, String required) {
        for (var available : capabilities.ids()) {
            if (required.equals(available.value())) {
                return true;
            }
        }
        return false;
    }
}
