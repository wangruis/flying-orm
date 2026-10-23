package com.flying.orm.rdb.vector;

import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.TermExtensionDescriptor;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderContext;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.sql.render.SqlTermPackage;

import java.util.ArrayList;
import java.util.List;

/** PostgreSQL Vector 条件 SQL 包。操作符固定，向量和阈值始终走参数绑定。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class VectorTermHandlers {

    private VectorTermHandlers() {
    }

    public static SqlTermPackage postgresql() {
        List<SqlTermHandler> handlers = new ArrayList<>();
        addComparisons(handlers, "vector-l2", false);
        addComparisons(handlers, "vector-cosine", false);
        addComparisons(handlers, "vector-inner-product", true);
        return SqlTermPackage.of("postgresql-vector", handlers);
    }

    private static void addComparisons(List<SqlTermHandler> handlers, String prefix, boolean negativeDistance) {
        handlers.add(handler(prefix + "-eq", "="));
        handlers.add(handler(prefix + "-ne", "<>"));
        handlers.add(handler(prefix + "-lt", negativeDistance ? ">" : "<"));
        handlers.add(handler(prefix + "-le", negativeDistance ? ">=" : "<="));
        handlers.add(handler(prefix + "-gt", negativeDistance ? "<" : ">"));
        handlers.add(handler(prefix + "-ge", negativeDistance ? "<=" : ">="));
    }

    private static SqlTermHandler handler(String id, String comparison) {
        return SqlTermHandler.of(
                TermExtensionDescriptor.filter(id, java.util.Set.of("postgresql-vector"), 2, 1),
                com.flying.orm.core.condition.ConditionValueShape.SCALAR,
                (term, context) -> render(term, context, comparison));
    }

    private static SqlFragment render(TermCondition term, SqlRenderContext context, String comparison) {
        if (!(term.value() instanceof VectorConditionValue value)) {
            throw new IllegalArgumentException("vector term value must be VectorConditionValue");
        }
        double threshold = value.metric() == VectorMetric.INNER_PRODUCT ? -value.threshold() : value.threshold();
        return SqlFragment.of("(" + context.identifier(term.field()) + " " + value.metric().operator()
                                      + " cast(? as vector)) " + comparison + " ?",
                              value.ownedVector(),
                              threshold);
    }
}
