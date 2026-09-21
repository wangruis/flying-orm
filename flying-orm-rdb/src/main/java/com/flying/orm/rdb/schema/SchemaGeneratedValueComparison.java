package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ValueGeneration;

/** Compares physical generated-value metadata only by creation semantics readers can prove. */
final class SchemaGeneratedValueComparison {

    private SchemaGeneratedValueComparison() {
    }

    static boolean same(ColumnMetadata current,
                        DynamicField target,
                        SchemaDialect.GeneratedValueStyle style) {
        ValueGeneration currentGeneration = current.generation();
        ValueGeneration targetGeneration = target.generation();
        if (currentGeneration.strategy() != targetGeneration.strategy()) {
            return false;
        }
        if (currentGeneration.strategy() == ValueGeneration.Strategy.NONE) {
            return true;
        }
        if (currentGeneration.strategy() == ValueGeneration.Strategy.SEQUENCE
                && !currentGeneration.sequenceName().equals(targetGeneration.sequenceName())) {
            return false;
        }
        // MySQL 不能完整回读每列创建参数，但目标仍必须是方言能够表达的恒等列语义。
        if (style == SchemaDialect.GeneratedValueStyle.MYSQL) {
            return currentGeneration.strategy() == ValueGeneration.Strategy.IDENTITY
                    && targetGeneration.startWith() == 1L
                    && targetGeneration.incrementBy() == 1L
                    && (targetGeneration.cacheSize() == 0L || targetGeneration.cacheSize() == 100L);
        }
        // 自定义方言没有可比较的参数事实，只比较上面的策略和序列名。
        if (style == SchemaDialect.GeneratedValueStyle.NONE) {
            return true;
        }
        if (currentGeneration.startWith() != targetGeneration.startWith()
                || currentGeneration.incrementBy() != targetGeneration.incrementBy()) {
            return false;
        }
        // 零表示调用方未指定缓存策略，不应被当成数据库物理值零。
        if (targetGeneration.cacheSize() == 0L) {
            return true;
        }
        if (style == SchemaDialect.GeneratedValueStyle.SQL_SERVER
                && targetGeneration.strategy() == ValueGeneration.Strategy.IDENTITY) {
            return targetGeneration.cacheSize() == 100L;
        }
        return currentGeneration.cacheSize() == targetGeneration.cacheSize();
    }
}
