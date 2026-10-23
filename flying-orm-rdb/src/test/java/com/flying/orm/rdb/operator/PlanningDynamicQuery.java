package com.flying.orm.rdb.operator;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;

/** 直接验证共同查询规划，不引入执行器或生产兼容构造。 */
final class PlanningDynamicQuery extends DmlQueryCommand<PlanningDynamicQuery> {
    PlanningDynamicQuery(SqlRenderer renderer, DataScope scope) {
        super(renderer, scope);
    }

    private PlanningDynamicQuery(PlanningDynamicQuery source) {
        super(source);
    }

    PlanningDynamicQuery copy() {
        return new PlanningDynamicQuery(this);
    }

    SqlRequest toRequest() {
        return toRequest(null);
    }

    @Override
    protected PlanningDynamicQuery self() {
        return this;
    }
}
