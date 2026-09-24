package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.ConnectionFactories;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtectedContainsExecutionLimitsTest {

    @Test
    void rendersTokensAndTotalParametersAbovePortableThresholds() {
        FormDataSqlRenderer renderer = renderer();
        for (List<ProtectedFieldRuntime.ContainsTokenGroup> groups : List.of(
                List.of(group("v1", 1001)),
                List.of(group("v1", 800), group("v2", 800), group("v3", 800)))) {
            ProtectedFieldRuntime.PreparedContainsQuery query = query(groups);
            SqlRequest request = assertDoesNotThrow(() -> renderer.protection().contains.rows(query, List.of()));
            assertTrue(request.parameters().size() >= groups.stream().mapToInt(g -> g.tokens().size() + 2).sum());
        }
    }

    @Test
    void nativeExecutorsHonorExplicitCandidateBudgetsWithoutAnImplicitThousandRowLimit() throws Exception {
        String database = "contains_budget_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database + ";DATABASE_TO_LOWER=TRUE");
        try (Connection connection = source.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table contains_rows(id bigint primary key, secret varchar(32))");
            statement.execute("create table contains_tokens(id bigint, field_tag varchar(32), token_hash varbinary(1))");
            statement.execute("insert into contains_rows select id, 'alphabet' from system_range(1, 1002) as r(id)");
            statement.execute("insert into contains_tokens select id, 'tag', X'01' from system_range(1, 1002) as r(id)");
            SqlRequest request = renderer().protection().contains.rows(query(List.of(group("v1", 1))), List.of());
            JdbcSqlExecutor jdbc = JdbcSqlExecutor.create(ConnectionAccessTestSupport.jdbc(source), RdbDialect.h2());
            R2dbcSqlExecutor reactive = R2dbcSqlExecutor.create(ConnectionAccessTestSupport.reactive(ConnectionFactories.get(
                    "r2dbc:h2:mem:///" + database + "?options=DATABASE_TO_LOWER=TRUE")), RdbDialect.h2());
            assertAll(
                    () -> assertEquals(1002, jdbc.query(request, SqlExecutionOptions.unlimited()).size()),
                    () -> assertEquals(1002, reactive.query(request, SqlExecutionOptions.unlimited()).count().block()),
                    () -> assertThrows(SqlRowLimitExceededException.class,
                            () -> jdbc.query(request, SqlExecutionOptions.maxRows(1001))),
                    () -> assertThrows(SqlRowLimitExceededException.class,
                            () -> reactive.query(request, SqlExecutionOptions.maxRows(1001)).collectList().block()),
                    () -> assertThrows(SqlResultMemoryLimitExceededException.class,
                            () -> jdbc.query(request, SqlExecutionOptions.unlimited().withMaxResultBytes(1))),
                    () -> assertThrows(SqlResultMemoryLimitExceededException.class,
                            () -> reactive.query(request, SqlExecutionOptions.unlimited().withMaxResultBytes(1))
                                    .collectList().block()));
        }
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
    }

    private static ProtectedFieldRuntime.ContainsTokenGroup group(String version, int count) {
        return new ProtectedFieldRuntime.ContainsTokenGroup(version, Collections.nCopies(count, new byte[]{1}));
    }

    private static ProtectedFieldRuntime.PreparedContainsQuery query(
            List<ProtectedFieldRuntime.ContainsTokenGroup> groups) {
        DynamicForm form = DynamicForm.builder("contains", "contains_rows")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR")).build();
        return new ProtectedFieldRuntime.PreparedContainsQuery(form, ConditionGroup.and().build(),
                List.of("id", "secret"), Set.of("secret"), "secret", "tag", "pha", groups,
                groups.getFirst().tokens().size(), List.of("id"), "contains_tokens");
    }
}
