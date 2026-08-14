package com.flying.orm.benchmark;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.infra.Blackhole;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 轻量确认 JMH 入口能被普通构建发现，避免基准类悄悄编译坏掉。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
class BenchmarkSmokeTest {

    /**
     * 三个核心热路径基准至少能完成一次调用。
     */
    @Test
    void benchmarkEntrypointsCanRunOnce() {
        Blackhole blackhole = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");

        BatchInsertPlanBenchmark batch = new BatchInsertPlanBenchmark();
        batch.setUp();
        assertNotNull(batch.compileInsertPlan());
        assertNotNull(batch.mapBatchRows());
        assertNotNull(batch.compileUpsertPlan());
        assertNotNull(batch.mapUpsertRows());

        StructuredConditionBenchmark conditions = new StructuredConditionBenchmark();
        conditions.setUp();
        assertNotNull(conditions.compileStructuredConditions());

        SqlRenderBenchmark render = new SqlRenderBenchmark();
        render.setUp();
        render.renderWhere(blackhole);
        assertNotNull(render.renderSelectHotPlanHit());
        assertNotNull(render.renderSelectWithoutPlanCache());

        EntityMappingBenchmark mapping = new EntityMappingBenchmark();
        mapping.setUp();
        assertNotNull(mapping.mapRecord());
        assertNotNull(mapping.mapBean());
        assertNotNull(mapping.readRecordValues());
        assertNotNull(mapping.readBeanValues());

        ReactiveExecutorOverheadBenchmark executor = new ReactiveExecutorOverheadBenchmark();
        executor.setUp();
        assertNotNull(executor.directUpdate());
        assertNotNull(executor.protectedUpdate());
        assertNotNull(executor.observedUpdate());
    }
}
