package com.flying.orm.rdb.operator;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** 执行冻结计划时不读取可变构建器，失效范围来自实际计划。 */
class DdlBuilderSurfaceConvergenceTest {

    @Test
    void syncExecutionInvalidatesTheReaderForTheFrozenPlanTarget() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER")).build();
        List<String> invalidated = new ArrayList<>();
        List<SqlRequest> executed = new ArrayList<>();
        JdbcFormMetadataReader reader = new JdbcFormMetadataReader() {
            public DynamicForm readForm(String id, RelationIdentity relation) {
                throw new AssertionError("execute must not replan");
            }

            @Override
            public RelationalTableDefinition readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).toTableDefinition(relation);
            }

            public void invalidate(RelationIdentity relation) { invalidated.add(relation.qualifiedName()); }
        };
        SyncSqlExecutor sql = new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { throw new AssertionError("execute must not replan"); }
            public long rowsUpdated(SqlRequest request) { executed.add(request); return 0; }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                throw new AssertionError("DDL must not request generated keys");
            }
        };
        SyncCreateOrAlterTableBuilder builder = new SyncCreateOrAlterTableBuilder(
                JdbcSchemaClient.create(sql, RdbDialect.postgresql()), reader, "unrelated_draft");
        SqlRequest request = new SqlRequest("alter table items add column note varchar(80)", List.of());
        SchemaMigrationPlan plan = new SchemaMigrationPlan(form.toTableDefinition(), true, List.of(request), List.of());

        builder.execute(plan);
        builder.execute(plan, SqlExecutionOptions.unlimited());

        assertEquals(List.of(request, request), executed);
        assertEquals(List.of("items", "items"), invalidated);
    }

}
