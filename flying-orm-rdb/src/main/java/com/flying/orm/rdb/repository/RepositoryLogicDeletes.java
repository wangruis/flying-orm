package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.core.scope.DataScope;

import java.util.Optional;

/**
 * 在绑定 Repository 时把实体逻辑删除声明补进表单，执行期统一交给表单内核。
 *
 * <p>如果 DynamicForm 已经声明逻辑删除，以表单定义为准。绑定完成后，查询和写入不再逐次检查实体注解；
 * 聚合仍保留调用方绑定的原始表单身份，只补充绑定时确立的范围。</p>
 */
final class RepositoryLogicDeletes {

    private RepositoryLogicDeletes() {
    }

    /** 绑定时统一补齐注解逻辑删除；用户表单的物理身份、字段和显式治理声明保持优先。 */
    static DynamicForm bind(EntityMetadata<?> metadata, DynamicForm form) {
        Optional<EntityFieldMetadata> deletion = metadata.logicDeleteField();
        if (form.logicDelete().isPresent() || deletion.isEmpty()) {
            return form;
        }
        DynamicForm.Builder builder = form.relationIdentity()
                .map(identity -> DynamicForm.relationalBuilder(form.id(), identity))
                .orElseGet(() -> DynamicForm.builder(form.id(), form.table()));
        form.fields().forEach(builder::addField);
        form.tenant().ifPresent(tenant -> builder.tenant(tenant.fieldName(), tenant.strategy()));
        form.protections().encryptedFields().forEach(builder::encrypted);
        form.protections().maskedFields().forEach(builder::masked);
        EntityFieldMetadata field = deletion.orElseThrow();
        return builder.logicDelete(field.columnName(), field.logicNotDeletedValue(), field.logicDeletedValue()).build();
    }

    /** 保留调用方聚合规格及绑定表单身份，只把绑定时补齐的规则作为可信范围交给同一内核。 */
    static AggregateSpec aggregate(AggregateSpec spec, DynamicForm effectiveForm) {
        QuerySpec query = spec.query();
        if (query.form() == effectiveForm) {
            return spec;
        }
        var deletion = effectiveForm.logicDelete().orElseThrow();
        ConditionGroup active = ConditionGroup.and()
                .where(deletion.fieldName(), "=", deletion.notDeletedValue()).build();
        AggregateSpec.Builder builder = AggregateSpec.builder(
                query.withScope(query.scope().and(DataScope.where(active))));
        spec.groups().forEach(builder::group);
        spec.aggregates().forEach(builder::aggregate);
        spec.having().ifPresent(builder::having);
        return builder.build();
    }

}
