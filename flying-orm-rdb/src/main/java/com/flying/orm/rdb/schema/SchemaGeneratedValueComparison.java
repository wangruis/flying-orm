package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ValueGeneration;

/** 比较生成策略的持久属性；无法回读的初始值只用于创建，不用于重置已有生成器。 */
final class SchemaGeneratedValueComparison {

    private SchemaGeneratedValueComparison() {
    }

    static boolean same(ValueGeneration currentGeneration,
                        ValueGeneration targetGeneration,
                        SchemaDialect.GeneratedValueStyle style) {
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
        // 自定义方言没有可比较的参数事实，只比较上面的策略和序列名。
        return style == SchemaDialect.GeneratedValueStyle.NONE
                || sameOptions(currentGeneration, targetGeneration, style);
    }

    static boolean sameOptions(ValueGeneration currentGeneration, ValueGeneration targetGeneration,
                               SchemaDialect.GeneratedValueStyle style) {
        if (currentGeneration.strategy() != targetGeneration.strategy()) {
            return false;
        }
        if (!currentGeneration.generated()) {
            return true;
        }
        // MySQL 的 AUTO_INCREMENT 是当前位置，不能作为创建时起点来比较或重置。
        if (style == SchemaDialect.GeneratedValueStyle.MYSQL) {
            return currentGeneration.strategy() == ValueGeneration.Strategy.IDENTITY
                    && targetGeneration.startWith() > 0L
                    && targetGeneration.incrementBy() == 1L
                    && (targetGeneration.cacheSize() == 0L || targetGeneration.cacheSize() == 100L);
        }
        if (hasObservableStart(style, targetGeneration)
                && currentGeneration.startWith() != targetGeneration.startWith()
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

    static boolean hasObservableStart(SchemaDialect.GeneratedValueStyle style, ValueGeneration generation) {
        return !(style == SchemaDialect.GeneratedValueStyle.MYSQL
                && generation.strategy() == ValueGeneration.Strategy.IDENTITY
                || style == SchemaDialect.GeneratedValueStyle.ORACLE
                && generation.strategy() == ValueGeneration.Strategy.SEQUENCE);
    }
}
