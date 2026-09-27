package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.TableMetadata;
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

class SchemaTypeRollbackReviewTest {

    @Test
    void equivalentTypesAndCommentChangesDoNotCreateDataRollbackGaps() {
        TableMetadata current = TableMetadata.builder("amounts")
                .addColumn(ColumnMetadata.of("amount", "INTEGER")).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.h2().schema());
        for (DynamicField field : List.of(DynamicField.of("amount", "INT"),
                DynamicField.of("amount", "INTEGER").withComment("amount label"),
                DynamicField.of("amount", "INTEGER").withNullable(false))) {
            DynamicForm desired = DynamicForm.builder("amounts", "amounts").addField(field).build();
            SchemaMigrationPlan migration = renderer.migrateSafelyPlan(current, desired, List.of(),
                    SchemaMigrationOptions.safe().allowColumnChange());
            ReviewedSchemaMigrationPlan plan = SchemaMigrationReviewer.create(renderer).review(
                    current, migration, SchemaMigrationReviewPolicy.allowBlocking());
            assertTrue(plan.rollback().complete());
            assertFalse(plan.requiresExplicitApproval());
        }
    }

    @Test
    void narrowingAndTypeConversionsCannotClaimCompleteDataRollback() {
        for (String[] types : List.of(
                new String[]{"DECIMAL(10,2)", "DECIMAL(10,1)"},
                new String[]{"VARCHAR(32)", "VARCHAR(16)"},
                new String[]{"TIMESTAMP(6)", "TIMESTAMP(3)"},
                new String[]{"DECIMAL(10,2)", "INTEGER"},
                new String[]{"VARCHAR(16)", "VARCHAR(32)"})) {
            ReviewedSchemaMigrationPlan plan = review(types[0], types[1]);
            assertFalse(plan.rollback().complete(), types[0] + " -> " + types[1]);
            assertEquals(SchemaMigrationRiskLevel.CRITICAL, plan.riskLevel());
            assertTrue(plan.rollback().gaps().stream().anyMatch(
                    gap -> gap.kind() == SchemaRollbackGap.Kind.DATA_CANNOT_BE_RESTORED));
            SchemaMigrationRejectedException rejection = assertThrows(
                    SchemaMigrationRejectedException.class, plan::requestsForExecution);
            assertEquals(SchemaMigrationFailureCode.APPROVAL_REQUIRED, rejection.failureCode());
            assertEquals(plan.migration().requests(), plan.requestsForExecution(
                    SchemaMigrationApproval.approve(plan, "explicitly accepted conversion")));
        }
    }

    @Test
    void reversingDecimalDefinitionDoesNotRestoreRoundedValues() throws Exception {
        ReviewedSchemaMigrationPlan plan = review("DECIMAL(10,2)", "DECIMAL(10,1)");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:decimal_rollback_review");
        try (Connection connection = source.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table amounts (amount decimal(10,2))");
            statement.execute("insert into amounts values (1.23)");
            for (SqlRequest request : plan.migration().requests()) statement.execute(request.sql());
            for (SqlRequest request : plan.rollback().requests()) statement.execute(request.sql());
            try (ResultSet values = statement.executeQuery("select amount from amounts")) {
                assertTrue(values.next());
                assertEquals(new BigDecimal("1.20"), values.getBigDecimal(1));
            }
            assertFalse(plan.rollback().complete());
        }
    }

    private static ReviewedSchemaMigrationPlan review(String source, String target) {
        TableMetadata current = TableMetadata.builder("amounts")
                .addColumn(ColumnMetadata.of("amount", source)).build();
        DynamicForm desired = DynamicForm.builder("amounts", "amounts")
                .addField(DynamicField.of("amount", target)).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.h2().schema());
        SchemaMigrationPlan migration = renderer.migrateSafelyPlan(current, desired, List.of(),
                SchemaMigrationOptions.safe().allowColumnChange());
        assertFalse(migration.requests().isEmpty());
        return SchemaMigrationReviewer.create(renderer).review(current, migration,
                SchemaMigrationReviewPolicy.allowBlocking());
    }
}
