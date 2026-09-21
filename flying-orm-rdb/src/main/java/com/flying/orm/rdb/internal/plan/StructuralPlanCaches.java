package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.BoundedCacheRegion;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.cache.OrmCacheSnapshot;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * SQL 结构计划与条件结构计划的统一缓存入口。
 *
 * <p>本类只管理完整 SQL 计划和表级失效；条件 AST 的分析、参数提取和缓存由独立协作者负责。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class StructuralPlanCaches implements MetadataCacheInvalidator {

    private final BoundedCacheRegion<SqlPlanSpec, SqlStructurePlan> sqlPlans;

    private final ConditionPlanCache conditions;

    private StructuralPlanCaches(OrmCachePolicy policy) {
        OrmCachePolicy safePolicy = Objects.requireNonNull(policy, "orm cache policy must not be null");
        sqlPlans = BoundedCacheRegion.create(safePolicy.sqlPlans(), StructuralPlanCaches::sqlWeight);
        conditions = ConditionPlanCache.create(safePolicy.conditionPlans());
    }

    public static StructuralPlanCaches create(OrmCachePolicy policy) {
        return new StructuralPlanCaches(policy);
    }

    /** 编译或复用完整 SQL 结构及 primitive 参数槽布局。 */
    public SqlStructurePlan sqlPlan(SqlPlanSpec spec, Supplier<SqlStructurePlan> compiler) {
        SqlPlanSpec safeSpec = Objects.requireNonNull(spec, "sql plan spec must not be null");
        Supplier<SqlStructurePlan> safeCompiler = Objects.requireNonNull(
                compiler, "sql plan compiler must not be null");
        return sqlPlans.get(safeSpec, ignored -> validatePlan(safeSpec, safeCompiler.get()));
    }

    /** 复用条件结构，同时从当前 AST 提取当前请求参数。 */
    public ConditionStructurePlan condition(String dialect,
                                            ConditionGroup where,
                                            SqlRenderer renderer) {
        return conditions.condition(dialect, where, renderer);
    }

    /** 清理指定表的 SQL 计划；带 schema 时精确清理。 */
    public void invalidateTable(String table) {
        invalidate(table);
    }

    @Override
    public void invalidate(String table) {
        String normalized = SqlPlanSpec.normalizeTable(table);
        int separator = normalized.indexOf('.');
        if (separator >= 0) {
            invalidate(normalized.substring(0, separator), normalized.substring(separator + 1));
            return;
        }
        sqlPlans.invalidateIf(key -> key.physicalTable().equals(normalized));
    }

    @Override
    public void invalidate(String schema, String table) {
        String normalizedSchema = requireIdentifierPart(schema, "plan cache schema");
        String normalizedTable = requireIdentifierPart(table, "plan cache table");
        sqlPlans.invalidateIf(key -> key.schema().equals(normalizedSchema)
                && key.physicalTable().equals(normalizedTable));
    }

    @Override
    public void invalidateAll() {
        sqlPlans.invalidateAll();
        conditions.invalidateAll();
    }

    public OrmCacheSnapshot sqlSnapshot() {
        return sqlPlans.snapshot();
    }

    public OrmCacheSnapshot conditionSnapshot() {
        return conditions.snapshot();
    }

    private static SqlStructurePlan validatePlan(SqlPlanSpec spec, SqlStructurePlan plan) {
        SqlStructurePlan safePlan = Objects.requireNonNull(plan, "compiled sql structure plan must not be null");
        if (!spec.operation().equalsIgnoreCase(safePlan.operation())) {
            throw new IllegalArgumentException("compiled SQL plan operation does not match its structure spec");
        }
        if (!spec.table().equals(SqlPlanSpec.normalizeTable(safePlan.table()))) {
            throw new IllegalArgumentException("compiled SQL plan table does not match its structure spec");
        }
        return safePlan;
    }

    private static int sqlWeight(SqlPlanSpec key, SqlStructurePlan plan) {
        String canonicalSql = plan.sql();
        int transportWeight = plan.statement().transportSql(key.dialect())
                .filter(transportSql -> !transportSql.equals(canonicalSql))
                .map(transportSql -> dividedLength(transportSql.length(), 32))
                .orElse(0);
        return 4 + dividedLength(canonicalSql.length(), 32)
                + transportWeight
                + dividedLength(key.table().length(), 16)
                + dividedLength(key.formFingerprint().length(), 32)
                + plan.parameterCount();
    }

    private static int dividedLength(int length, int divisor) {
        return Math.max(1, (length + divisor - 1) / divisor);
    }

    private static String requireIdentifierPart(String text, String name) {
        String normalized = Objects.requireNonNull(text, name + " must not be null").trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        if (normalized.indexOf('.') >= 0) {
            throw new IllegalArgumentException(name + " must not contain '.'");
        }
        return normalized;
    }
}
