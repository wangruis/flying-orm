package com.flying.orm.rdb.schema;

/** 迁移级观测回调。实现必须尽快返回，指标或日志故障不能反向打断 DDL 执行。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
@FunctionalInterface
public interface SchemaMigrationObserver {
    void onMigration(SchemaMigrationObservation observation);

    static SchemaMigrationObserver noop() {
        return ignored -> {
        };
    }
}
