package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.cache.BoundedCacheRegion;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;

import java.util.Objects;

/** 客户端级有界字段解码计划缓存；键和值都只描述表单结构。 */
final class FormFieldDecodingPlanCache implements MetadataCacheInvalidator {

    private final BoundedCacheRegion<String, FormFieldDecodingPlan> plans;

    private FormFieldDecodingPlanCache(CacheRegionPolicy policy) {
        this.plans = BoundedCacheRegion.create(
                Objects.requireNonNull(policy, "form decoding plan cache policy must not be null"),
                (fingerprint, plan) -> 1 + plan.size());
    }

    static FormFieldDecodingPlanCache create(CacheRegionPolicy policy) {
        return new FormFieldDecodingPlanCache(policy);
    }

    FormFieldDecodingPlan plan(DynamicForm form, FormDataSqlRenderer renderer) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        FormDataSqlRenderer safeRenderer = Objects.requireNonNull(
                renderer, "form data sql renderer must not be null");
        String fingerprint = safeForm.structureFingerprint();
        FormFieldDecodingPlan cached = plans.getIfPresent(fingerprint);
        if (cached != null) {
            return cached;
        }
        return plans.get(fingerprint, ignored -> FormFieldDecodingPlan.compile(safeForm, safeRenderer));
    }

    @Override
    public void invalidate(String table) {
        Objects.requireNonNull(table, "table must not be null");
        plans.invalidateAll();
    }

    @Override
    public void invalidateAll() {
        plans.invalidateAll();
    }
}
