package com.flying.orm.benchmark.database;

/** 性能入口允许执行的固定场景，命令行不能借场景名传入 SQL 或任意方法。 */
enum ReactivePerformanceScenario {
    QUERY_BY_ID("queryById"),
    RAW_QUERY_BY_ID("rawQueryById"),
    UPDATE_BY_ID("updateById"),
    TRANSACTIONAL_UPDATE_BATCH("transactionalUpdateBatch"),
    ATOMIC_BATCH_INSERT("atomicBatchInsert"),
    INDEPENDENT_BATCH_INSERT("independentBatchInsert");

    final String externalName;

    ReactivePerformanceScenario(String externalName) {
        this.externalName = externalName;
    }

    static ReactivePerformanceScenario fromExternalName(String name) {
        for (ReactivePerformanceScenario scenario : values()) {
            if (scenario.externalName.equals(name)) {
                return scenario;
            }
        }
        throw new IllegalArgumentException("unknown database performance scenario: " + name);
    }
}
