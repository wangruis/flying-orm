package com.flying.orm.benchmark;

import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * BenchmarkRunner 给本项目一个固定的 JMH 入口，少靠手敲参数。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
public final class BenchmarkRunner {

    private static final String DEFAULT_INCLUDE = "com.flying.orm.benchmark.*Benchmark";

    private static final String DEFAULT_RESULT = Path.of("target",
                                                         "benchmark-results",
                                                         "flying-orm-jmh.json").toString();

    private BenchmarkRunner() {
    }

    /**
     * 运行 JMH。
     *
     * @param args include、结果文件、fork、线程、预热/测量次数和秒数、统计模式
     * @throws RunnerException JMH 执行失败
     * @throws IOException     创建结果目录失败
     */
    public static void main(String[] args) throws RunnerException, IOException {
        Options options = options(args);
        Path result = Path.of(options.getResult().get());
        Path parent = result.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        new Runner(options).run();
    }

    static Options options(String[] args) {
        Arguments arguments = Arguments.parse(args);
        ChainedOptionsBuilder builder = new OptionsBuilder()
                .include(arguments.include)
                .forks(arguments.forks)
                .threads(arguments.threads)
                .warmupIterations(arguments.warmupIterations)
                .warmupTime(TimeValue.seconds(arguments.warmupTimeSeconds))
                .measurementIterations(arguments.measurementIterations)
                .measurementTime(TimeValue.seconds(arguments.measurementTimeSeconds))
                .resultFormat(ResultFormatType.JSON)
                .result(arguments.result);
        if (arguments.mode != null) {
            builder.mode(arguments.mode);
        }
        return builder.build();
    }

    private record Arguments(String include,
                             String result,
                             int forks,
                             int threads,
                             int warmupIterations,
                             int warmupTimeSeconds,
                             int measurementIterations,
                             int measurementTimeSeconds,
                             Mode mode) {

        private static Arguments parse(String[] args) {
            String include = DEFAULT_INCLUDE;
            String result = DEFAULT_RESULT;
            int forks = 1;
            int threads = 1;
            int warmupIterations = 3;
            int warmupTimeSeconds = 1;
            int measurementIterations = 5;
            int measurementTimeSeconds = 1;
            Mode mode = null;
            String[] safeArgs = Objects.requireNonNull(args, "benchmark runner args must not be null");
            for (int i = 0; i < safeArgs.length; i++) {
                String option = safeArgs[i];
                switch (option) {
                    case "--include" -> include = value(safeArgs, ++i, option);
                    case "--result" -> result = value(safeArgs, ++i, option);
                    case "--forks" -> forks = positiveInt(value(safeArgs, ++i, option), option);
                    case "--threads" -> threads = positiveInt(value(safeArgs, ++i, option), option);
                    case "--warmup" -> warmupIterations = positiveInt(value(safeArgs, ++i, option), option);
                    case "--warmup-time" -> warmupTimeSeconds = positiveInt(value(safeArgs, ++i, option), option);
                    case "--measurement" -> measurementIterations = positiveInt(value(safeArgs, ++i, option), option);
                    case "--measurement-time" -> measurementTimeSeconds = positiveInt(value(safeArgs,
                                                                                              ++i,
                                                                                              option),
                                                                                 option);
                    case "--mode" -> mode = mode(value(safeArgs, ++i, option));
                    default -> throw new IllegalArgumentException("unknown benchmark option: " + option);
                }
            }
            return new Arguments(include,
                                 result,
                                 forks,
                                 threads,
                                 warmupIterations,
                                 warmupTimeSeconds,
                                 measurementIterations,
                                 measurementTimeSeconds,
                                 mode);
        }

        private static String value(String[] args, int index, String option) {
            if (index >= args.length || args[index].isBlank()) {
                throw new IllegalArgumentException(option + " requires a value");
            }
            return args[index];
        }

        private static int positiveInt(String value, String option) {
            int parsed;
            try {
                parsed = Integer.parseInt(value);
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException(option + " requires a positive integer", error);
            }
            if (parsed <= 0) {
                throw new IllegalArgumentException(option + " requires a positive integer");
            }
            return parsed;
        }

        private static Mode mode(String value) {
            return switch (value.toLowerCase(java.util.Locale.ROOT)) {
                case "throughput" -> Mode.Throughput;
                case "average" -> Mode.AverageTime;
                case "sample" -> Mode.SampleTime;
                case "single-shot" -> Mode.SingleShotTime;
                default -> throw new IllegalArgumentException(
                        "--mode requires throughput, average, sample or single-shot");
            };
        }
    }
}
