package com.flying.orm.benchmark.database;

/** 一个响应式性能目标的固定数据库信息。连接串只在运行时内存中使用，不会进入报告。 */
record ReactivePerformanceTarget(String key,
                                 String name,
                                 String url,
                                 String table,
                                 String dropSql,
                                 String createSql,
                                 String versionSql,
                                 String bindMarker) {
}
