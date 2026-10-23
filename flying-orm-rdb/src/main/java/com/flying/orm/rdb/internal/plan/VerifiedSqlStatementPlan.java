package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.internal.dialect.DatabaseProduct;

import java.util.Objects;

/** 仅由统一编译器创建的可信执行就绪 SQL 计划。 */
final class VerifiedSqlStatementPlan extends SqlStatementPlan {
    final int insertValuesOffset;
    final boolean batchRowIndex;
    final boolean ignoreConflicts;

    VerifiedSqlStatementPlan(String sql,
                             SqlBindMarkerStyle bindMarkerStyle,
                             int parameterCount,
                             DatabaseProduct databaseProduct,
                             String transportSql) {
        super(validated(
                sql,
                bindMarkerStyle,
                parameterCount,
                Objects.requireNonNull(databaseProduct, "database product must not be null").name(),
                transportSql));
        insertValuesOffset = -1;
        batchRowIndex = false;
        ignoreConflicts = false;
    }

    VerifiedSqlStatementPlan(VerifiedSqlStatementPlan source, int valuesOffset,
                             DatabaseProduct product) {
        super(validated(source.sql(), source.bindMarkerStyle(), source.parameterCount(), product.name(),
                source.transportSql(product.name()).orElseThrow()));
        insertValuesOffset = valuesOffset;
        batchRowIndex = false;
        ignoreConflicts = false;
    }

    VerifiedSqlStatementPlan(VerifiedSqlStatementPlan source, DatabaseProduct product,
                             boolean batchRowIndex, boolean ignoreConflicts) {
        super(validated(source.sql(), source.bindMarkerStyle(), source.parameterCount(), product.name(),
                source.transportSql(product.name()).orElseThrow()));
        insertValuesOffset = -1;
        this.batchRowIndex = batchRowIndex;
        this.ignoreConflicts = ignoreConflicts;
    }

    boolean matches(DatabaseProduct actualProduct) {
        DatabaseProduct safeProduct = Objects.requireNonNull(
                actualProduct, "actual database product must not be null");
        return preparedFor(safeProduct.name());
    }
}
