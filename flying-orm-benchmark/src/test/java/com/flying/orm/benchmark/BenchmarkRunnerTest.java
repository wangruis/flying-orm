package com.flying.orm.benchmark;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.options.Options;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runner 测试只看 JMH 配置，不真的跑基准，避免普通构建变慢。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
class BenchmarkRunnerTest {

    /**
     * 默认跑 flying-orm 当前的几个热路径，并把结果写到 target 目录。
     */
    @Test
    void buildsDefaultFlyingOrmBenchmarkOptions() {
        Options options = BenchmarkRunner.options(new String[0]);

        assertTrue(options.getIncludes().contains("com.flying.orm.benchmark.*Benchmark"));
        assertEquals(1, options.getForkCount().get());
        assertEquals(3, options.getWarmupIterations().get());
        assertEquals(5, options.getMeasurementIterations().get());
        assertTrue(options.getResult().get().endsWith(Path.of("target",
                                                              "benchmark-results",
                                                              "flying-orm-jmh.json").toString()));
    }

    /**
     * 命令行可以收窄 include，方便只跑一条热路径。
     */
    @Test
    void acceptsIncludeAndResultFileArguments() {
        Options options = BenchmarkRunner.options(new String[]{"--include", ".*Batch.*", "--result", "target/out.json"});

        assertEquals(".*Batch.*", options.getIncludes().iterator().next());
        assertTrue(options.getResult().get().endsWith("target/out.json"));
    }

    /**
     * 正式对比时可以固定线程数、单轮时长和统计模式，避免两次结果口径不一致。
     */
    @Test
    void acceptsConcurrencyDurationAndModeArguments() {
        Options options = BenchmarkRunner.options(new String[]{"--threads", "8",
                                                               "--warmup-time", "2",
                                                               "--measurement-time", "3",
                                                               "--mode", "sample"});

        assertEquals(8, options.getThreads().get());
        assertEquals(2, options.getWarmupTime().get().getTime());
        assertEquals(3, options.getMeasurementTime().get().getTime());
        assertTrue(options.getBenchModes().contains(Mode.SampleTime));
    }
}
