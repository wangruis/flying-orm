package com.flying.orm.benchmark;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 测条件 AST 渲染成参数化 SQL 的开销。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SqlRenderBenchmark {

    private SqlRenderer renderer;

    private ConditionGroup where;

    private FormDataSqlRenderer hotRenderer;

    private FormDataSqlRenderer coldRenderer;

    private DynamicForm form;

    /**
     * 准备一组带普通条件和集合条件的查询。
     */
    @Setup(Level.Trial)
    public void setUp() {
        renderer = SqlRenderer.builder()
                              .addTerm(SqlTermHandler.equalsTo())
                              .addTerm(SqlTermHandler.greaterThan())
                              .addTerm(SqlTermHandler.like())
                              .addTerm(SqlTermHandler.in())
                              .build();
        where = ConditionGroup.and()
                              .where("status", "=", "enabled")
                              .where("age", ">", 18)
                              .or(or -> or.where("name", "like", "wang")
                                          .where("org_id", "in", List.of(1L, 2L, 3L)))
                              .build();
        form = DynamicForm.builder("users", "users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("status", "VARCHAR"))
                          .addField(DynamicField.of("age", "INTEGER"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("org_id", "BIGINT"))
                          .build();
        hotRenderer = FormDataSqlRenderer.create(renderer, RdbDialect.h2());
        OrmCachePolicy disabled = OrmCachePolicy.builder()
                .sqlPlans(CacheRegionPolicy.disabled())
                .conditionPlans(CacheRegionPolicy.disabled())
                .build();
        coldRenderer = hotRenderer.withPlanCaches(StructuralPlanCaches.create(disabled));
        hotRenderer.select(form, where);
    }

    /**
     * 渲染 where 条件，结果交给 Blackhole 消费。
     *
     * @param blackhole JMH 黑洞
     */
    @Benchmark
    public void renderWhere(Blackhole blackhole) {
        blackhole.consume(renderer.renderWhere(where));
    }

    /**
     * 测量结构计划热命中；完整 SELECT SQL 已在 trial 准备阶段编译。
     *
     * @return 当前请求参数与复用 SQL 计划组成的请求
     */
    @Benchmark
    public SqlRequest renderSelectHotPlanHit() {
        return hotRenderer.select(form, where);
    }

    /**
     * 测量关闭结构缓存时每次完整编译 SELECT SQL 的冷路径。
     *
     * @return 每次重新编译的 SQL 请求
     */
    @Benchmark
    public SqlRequest renderSelectWithoutPlanCache() {
        return coldRenderer.select(form, where);
    }
}
