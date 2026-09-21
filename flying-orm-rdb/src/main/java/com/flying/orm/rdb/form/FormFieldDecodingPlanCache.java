package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.cache.BoundedCacheRegion;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;

import java.util.Objects;

/** 客户端级有界字段解码计划缓存；自定义 codec 还依赖装配时的精确字段身份。 */
final class FormFieldDecodingPlanCache implements MetadataCacheInvalidator {

    private final CacheRegionPolicy policy;
    private final BoundedCacheRegion<Object, FormFieldDecodingPlan> plans;

    private FormFieldDecodingPlanCache(CacheRegionPolicy policy) {
        this.policy = Objects.requireNonNull(policy, "form decoding plan cache policy must not be null");
        this.plans = BoundedCacheRegion.create(
                this.policy,
                (key, plan) -> 1 + Math.max(plan.size(), key instanceof DynamicForm form ? form.fields().size() : 0));
    }

    static FormFieldDecodingPlanCache create(CacheRegionPolicy policy) {
        return new FormFieldDecodingPlanCache(policy);
    }

    /** codec 配置变化时隔离旧门面的缓存，容量与过期策略继续沿用。 */
    FormFieldDecodingPlanCache emptyCopy() {
        return create(policy);
    }

    FormFieldDecodingPlan plan(DynamicForm form, FormDataSqlRenderer renderer, boolean customFieldCodecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        FormDataSqlRenderer safeRenderer = Objects.requireNonNull(
                renderer, "form data sql renderer must not be null");
        // 普通客户端继续按结构共享；实体 codec 按字段实例挂接，结构相同不代表解码规则相同。
        // 直接复用不可变表单作键，不在每次查询中扫描字段或创建组合键；其字段数计入缓存权重。
        Object key = customFieldCodecs ? safeForm : safeForm.structureFingerprint();
        FormFieldDecodingPlan cached = plans.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        return plans.get(key, ignored -> FormFieldDecodingPlan.compile(safeForm, safeRenderer));
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
