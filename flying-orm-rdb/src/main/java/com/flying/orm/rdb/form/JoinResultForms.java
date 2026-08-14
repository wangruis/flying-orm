package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.protection.ProtectedFormLayout;

import java.util.Objects;

/**
 * 根据显式 JOIN 投影创建只用于结果解码的紧凑表单。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class JoinResultForms {

    private JoinResultForms() {
    }

    static DynamicForm create(JoinQuerySpec spec) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        DynamicForm.Builder result = DynamicForm.builder("join-result", "join_result");
        for (JoinProjection projection : safeSpec.projections()) {
            DynamicForm sourceForm = projection.field().source().form();
            DynamicField source = ProtectedFormLayout.physical(sourceForm)
                                                       .field(projection.field().field());
            result.addField(new DynamicField(projection.alias(),
                                             source.dataType(),
                                             false,
                                             true,
                                             false,
                                             source.length(),
                                             source.precision(),
                                             source.scale(),
                                             null,
                                             ValueGeneration.none()));
        }
        return result.build();
    }
}
