package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SchemaExplicitTypeChangeTest {

    @Test
    void explicitTypeChangesExposeTheirForwardSql() {
        for (String[] types : List.of(
                new String[]{"DECIMAL(10,2)", "DECIMAL(10,1)"},
                new String[]{"VARCHAR(32)", "VARCHAR(16)"},
                new String[]{"TIMESTAMP(6)", "TIMESTAMP(3)"},
                new String[]{"DECIMAL(10,2)", "INTEGER"},
                new String[]{"VARCHAR(16)", "VARCHAR(32)"})) {
            SchemaMigrationPlan plan = review(types[0], types[1]);
            assertEquals(plan.requests(), plan.requestsForExecution());
        }
    }

    @Test
    void explicitDecimalNarrowingUsesDatabaseConversion() throws Exception {
        SchemaMigrationPlan plan = review("DECIMAL(10,2)", "DECIMAL(10,1)");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:decimal_type_change");
        try (Connection connection = source.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table amounts (amount decimal(10,2))");
            statement.execute("insert into amounts values (1.23)");
            for (SqlRequest request : plan.requests()) statement.execute(request.sql());
            try (ResultSet values = statement.executeQuery("select amount from amounts")) {
                assertTrue(values.next());
                assertEquals(new BigDecimal("1.2"), values.getBigDecimal(1));
            }
        }
    }

    private static SchemaMigrationPlan review(String source, String target) {
        RelationalTableDefinition current = RelationalTableDefinition.builder("amounts")
                .addColumn(DynamicField.of("amount", source).column()).build();
        DynamicForm desired = DynamicForm.builder("amounts", "amounts")
                .addField(DynamicField.of("amount", target)).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.h2().schema());
        SchemaMigrationPlan migration = renderer.migrateSafelyPlan(current, desired, List.of(),
                SchemaMigrationOptions.safe().allowColumnChange());
        assertFalse(migration.requests().isEmpty());
        return renderer.renderPlan(current, null, migration, SchemaMigrationOptions.safe());
    }
}
