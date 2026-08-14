package com.flying.orm.rdb.vector;

import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderContext;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.sql.render.SqlTermPackage;

/** PostgreSQL Vector 条件 SQL 包。操作符固定，向量和阈值始终走参数绑定。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class VectorTermHandlers {

    private VectorTermHandlers() {
    }

    public static SqlTermPackage postgresql() {
        return SqlTermPackage.of("postgresql-vector",
                                 handler(VectorStructuredConditions.L2_LESS_THAN),
                                 handler(VectorStructuredConditions.COSINE_LESS_THAN),
                                 handler(VectorStructuredConditions.INNER_PRODUCT_GREATER_THAN));
    }

    private static SqlTermHandler handler(String id) {
        return SqlTermHandler.of(id, VectorTermHandlers::render);
    }

    private static SqlFragment render(TermCondition term, SqlRenderContext context) {
        if (!(term.value() instanceof VectorConditionValue value)) {
            throw new IllegalArgumentException("vector term value must be VectorConditionValue");
        }
        double threshold = value.metric() == VectorMetric.INNER_PRODUCT ? -value.threshold() : value.threshold();
        return SqlFragment.of("(" + context.identifier(term.field()) + " " + value.metric().operator()
                                      + " cast(? as vector)) < ?",
                              value.vector(),
                              threshold);
    }
}
