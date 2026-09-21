package com.flying.orm.rdb.metadata;

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlServerSequencePhysicalMetadataRegressionTest {

    @Test
    void sequenceMustMatchColumnTypeAndUseTypeDefaultBounds() {
        String sql = SqlServerMetadataQueries.queries().columnQuery().create("app", "orders")
                .sql().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");

        assertAll(
                () -> assertTrue(sql.contains("referenced_sequence.minimum_value")),
                () -> assertTrue(sql.contains("referenced_sequence.maximum_value")),
                () -> assertTrue(sql.contains("type_name(referenced_sequence.system_type_id)")),
                () -> assertTrue(sql.contains("generation_sequence.data_type = lower(c.data_type)")),
                () -> assertTrue(sql.contains("generation_sequence.default_bounds = 1")),
                () -> assertTrue(sql.contains("sequence data type differs from column")),
                () -> assertTrue(sql.contains("sequence bounds are not type defaults")));
    }
}
