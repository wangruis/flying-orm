package com.flying.orm.testkit.dialect;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.schema.FormSchemaSqlRenderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 这不是测某个真实数据库，而是先把我们会生成什么 SQL 固定下来。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
class DialectSqlContractsTest {

    @Test
    void containsExpectedBuiltInDialects() {
        assertEquals(List.of("h2", "mysql", "postgresql", "oracle", "sqlserver"),
                     DialectSqlContracts.builtIns()
                                        .stream()
                                        .map(DialectSqlContractCase::name)
                                        .toList());
    }

    @ParameterizedTest
    @MethodSource("cases")
    void rendersBuiltInDialectSqlContracts(DialectSqlContractCase testCase) {
        DynamicForm form = form();
        FormDataSqlRenderer formRenderer = FormDataSqlRenderer.create(conditionRenderer(), testCase.dialect());

        SqlRequest createTable = FormSchemaSqlRenderer.create(testCase.dialect()).createTable(form).getFirst();
        SqlRequest select = formRenderer.select(form,
                                                ConditionGroup.and()
                                                              .where("name", "=", "王")
                                                              .build(),
                                                PageQuery.of(2, 10, PageSort.asc("id")));
        BatchWriteRequest upsert = formRenderer.upsertBatch(form,
                                                            List.of(orderedMap("id", 1L,
                                                                               "name", "王",
                                                                               "enabled", true,
                                                                               "created_at", "2026-07-24T18:00:00")));

        assertEquals(testCase.createTableSql(), createTable.sql());
        assertEquals(testCase.pagedSelectSql(), select.sql());
        assertEquals(testCase.upsertSql(), upsert.sql());
    }

    private static Stream<DialectSqlContractCase> cases() {
        return DialectSqlContracts.builtIns().stream();
    }

    private static SqlRenderer conditionRenderer() {
        return SqlRenderer.builder()
                          .addTerm(SqlTermHandler.equalsTo())
                          .build();
    }

    private static DynamicForm form() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("enabled", "BOOLEAN"))
                          .addField(DynamicField.of("created_at", "TIMESTAMP"))
                          .build();
    }

    private static Map<String, Object> orderedMap(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            values.put((String) pairs[i], pairs[i + 1]);
        }
        return values;
    }
}
