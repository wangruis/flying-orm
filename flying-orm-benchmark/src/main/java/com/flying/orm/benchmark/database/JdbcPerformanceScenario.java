package com.flying.orm.benchmark.database;

/** JDBC runner 支持的核心场景，名字和现有 R2DBC runner 保持一致。 */
enum JdbcPerformanceScenario {
    QUERY_BY_ID("queryById"),
    UPDATE_BY_ID("updateById"),
    ATOMIC_BATCH_INSERT("atomicBatchInsert"),
    INDEPENDENT_BATCH_INSERT("independentBatchInsert");

    final String name;

    JdbcPerformanceScenario(String name) {
        this.name = name;
    }

    static JdbcPerformanceScenario fromName(String value) {
        for (JdbcPerformanceScenario scenario : values()) {
            if (scenario.name.equalsIgnoreCase(value)) {
                return scenario;
            }
        }
        throw new IllegalArgumentException("unknown JDBC benchmark scenario: " + value);
    }
}
