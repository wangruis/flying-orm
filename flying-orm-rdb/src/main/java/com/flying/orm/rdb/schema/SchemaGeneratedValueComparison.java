package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ValueGeneration;

/** Compares physical generated-value metadata only by creation semantics readers can prove. */
final class SchemaGeneratedValueComparison {

    private SchemaGeneratedValueComparison() {
    }

    static boolean same(ColumnMetadata current, DynamicField target) {
        ValueGeneration currentGeneration = current.generation();
        ValueGeneration targetGeneration = target.generation();
        if (currentGeneration.strategy() != targetGeneration.strategy()) {
            return false;
        }
        return currentGeneration.strategy() != ValueGeneration.Strategy.SEQUENCE
                || currentGeneration.sequenceName().equals(targetGeneration.sequenceName());
    }
}
