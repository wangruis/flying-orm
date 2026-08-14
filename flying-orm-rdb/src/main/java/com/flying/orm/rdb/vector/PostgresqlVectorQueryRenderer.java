package com.flying.orm.rdb.vector;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * 生成 PostgreSQL 最近邻查询。投影、表名和字段名全部来自 DynamicForm，Scope 合并后的 where AST 由调用方传入。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class PostgresqlVectorQueryRenderer {

    private static final int MAX_LIMIT = 10_000;

    private final SqlRenderer conditions;

    private final RdbDialect dialect;

    private PostgresqlVectorQueryRenderer(SqlRenderer conditions) {
        this.dialect = RdbDialect.postgresql();
        this.conditions = Objects.requireNonNull(conditions, "sql renderer must not be null")
                                 .withIdentifierRenderer(dialect.schema()::identifier);
    }

    public static PostgresqlVectorQueryRenderer create(SqlRenderer conditions) {
        return new PostgresqlVectorQueryRenderer(conditions);
    }

    public SqlRequest nearest(DynamicForm form,
                              List<String> projections,
                              String vectorField,
                              Object vector,
                              VectorMetric metric,
                              ConditionGroup where,
                              int limit) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        DynamicField field = safeForm.field(vectorField);
        if (!VectorValueCodec.isVectorDataType(field.dataType())) {
            throw new IllegalArgumentException("nearest query requires a VECTOR field: " + field.name());
        }
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("vector query limit must be between 1 and " + MAX_LIMIT);
        }

        List<String> safeProjections = List.copyOf(Objects.requireNonNull(projections,
                                                                          "vector projections must not be null"));
        if (safeProjections.isEmpty()) {
            throw new IllegalArgumentException("vector projections must not be empty");
        }
        StringJoiner selected = new StringJoiner(", ");
        for (String projection : safeProjections) {
            selected.add(dialect.schema().identifier(safeForm.field(projection).name()));
        }

        float[] parameter = VectorValueCodec.write(vector, field.length());
        String distance = dialect.schema().identifier(field.name()) + " "
                + Objects.requireNonNull(metric, "vector metric must not be null").operator()
                + " cast(? as vector)";
        StringBuilder sql = new StringBuilder("select ")
                .append(selected).append(", ").append(distance)
                .append(" as ").append(dialect.schema().identifier("_distance"))
                .append(" from ").append(dialect.schema().identifier(safeForm.table()));
        List<Object> parameters = new ArrayList<>();
        parameters.add(parameter);

        SqlFragment whereFragment = conditions.renderWhere(Objects.requireNonNull(where,
                                                                                   "where condition must not be null"));
        if (!whereFragment.sql().isBlank()) {
            sql.append(" where ").append(whereFragment.sql());
            parameters.addAll(whereFragment.parameters());
        }
        sql.append(" order by ").append(dialect.schema().identifier("_distance")).append(" asc limit ?");
        parameters.add(limit);
        return new SqlRequest(sql.toString(), parameters);
    }
}
