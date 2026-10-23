package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.metadata.RelationIdentity;
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
    private final boolean sqlPlansEnabled;

    private final ConditionPlanCache conditions;

    private StructuralPlanCaches(OrmCachePolicy policy) {
        OrmCachePolicy safePolicy = Objects.requireNonNull(policy, "orm cache policy must not be null");
        sqlPlansEnabled = safePolicy.sqlPlans().enabled();
        sqlPlans = BoundedCacheRegion.create(safePolicy.sqlPlans(), StructuralPlanCaches::sqlWeight);
        conditions = ConditionPlanCache.create(safePolicy.conditionPlans());
    }

    public static StructuralPlanCaches create(OrmCachePolicy policy) {
        return new StructuralPlanCaches(policy);
    }

    /** 让调用方在构造完整结构键之前跳过已关闭的 SQL 缓存。 */
    public boolean sqlPlansEnabled() {
        return sqlPlansEnabled;
    }

    /** 编译或复用完整 SQL 结构及顺序参数数量。 */
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

    /** 清理目标关系的 SQL 计划，不清除无关表与独立条件计划。 */
    @Override
    public void invalidate(RelationIdentity relation) {
        RelationIdentity target = Objects.requireNonNull(relation, "plan cache relation must not be null");
        sqlPlans.invalidateIf(key -> key.table().table().equals(target.table())
                && (target.schema().isEmpty() || key.table().schema().equals(target.schema()))
                && (target.catalog().isEmpty() || key.table().catalog().equals(target.catalog())));
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
        if (!spec.table().equals(safePlan.table())) {
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
                + dividedLength(key.table().qualifiedName().length(), 16)
                + dividedLength(key.formFingerprint().length(), 32);
    }

    private static int dividedLength(int length, int divisor) {
        return Math.max(1, (length + divisor - 1) / divisor);
    }

}
