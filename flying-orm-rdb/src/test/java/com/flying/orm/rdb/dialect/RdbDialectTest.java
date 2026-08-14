package com.flying.orm.rdb.dialect;

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
import com.flying.orm.rdb.schema.SchemaOnlineDdlSupport;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证 RDB 方言可以统一承载结构 SQL 与分页 SQL 的数据库差异。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
class RdbDialectTest {

    /**
     * 验证当前内置数据库方言只覆盖已确认需要支持的数据库。
     */
    @Test
    void exposesOnlyCurrentBuiltInDatabaseDialects() {
        Set<String> factories = Arrays.stream(RdbDialect.class.getDeclaredMethods())
                                      .filter(method -> Modifier.isPublic(method.getModifiers()))
                                      .filter(method -> Modifier.isStatic(method.getModifiers()))
                                      .filter(method -> method.getParameterCount() == 0)
                                      .filter(method -> RdbDialect.class.equals(method.getReturnType()))
                                      .map(Method::getName)
                                      .collect(Collectors.toUnmodifiableSet());

        assertEquals(Set.of("h2", "mysql", "postgresql", "oracle", "sqlServer"), factories);
    }

    /**
     * 验证 MySQL 方言同时提供反引号标识符和 limit/offset 分页策略。
     */
    @Test
    void mysqlDialectDrivesSchemaAndPaginationRendering() {
        DynamicForm form = DynamicForm.builder("userForm", "Users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .build();
        RdbDialect dialect = RdbDialect.mysql();

        SqlRequest createTable = FormSchemaSqlRenderer.create(dialect).createTable(form).get(0);
        SqlRequest select = FormDataSqlRenderer.create(conditionRenderer(), dialect)
                                               .select(form,
                                                       ConditionGroup.and()
                                                                     .where("name", "=", "王")
                                                                     .build(),
                                                       PageQuery.of(2, 10, PageSort.asc("id")));

        assertEquals("mysql", dialect.name());
        assertEquals("create table `Users` (`id` BIGINT primary key, `name` VARCHAR(255))", createTable.sql());
        assertEquals("select `id`, `name` from `Users` where `name` = ? order by `id` asc limit ? offset ?",
                     select.sql());
        assertEquals(List.of("王", 10, 10L), select.parameters());
    }

    /**
     * 验证 Oracle 方言先覆盖常用 DDL、分页和 merge upsert。
     */
    @Test
    void oracleDialectDrivesSchemaPaginationAndUpsertRendering() {
        DynamicForm form = DynamicForm.builder("userForm", "Users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("name", "VARCHAR2(64)"))
                                      .build();
        RdbDialect dialect = RdbDialect.oracle();

        SqlRequest createTable = FormSchemaSqlRenderer.create(dialect).createTable(form).get(0);
        SqlRequest select = FormDataSqlRenderer.create(conditionRenderer(), dialect)
                                               .select(form,
                                                       ConditionGroup.and()
                                                                     .where("name", "=", "王")
                                                                     .build(),
                                                       PageQuery.of(2, 10, PageSort.asc("id")));
        BatchWriteRequest upsert = FormDataSqlRenderer.create(conditionRenderer(), dialect)
                                                    .upsertBatch(form,
                                                                 List.of(orderedMap("id", 1L,
                                                                                    "name", "王")));

        assertEquals("oracle", dialect.name());
        assertEquals("create table \"Users\" (\"id\" NUMBER(19) primary key, \"name\" VARCHAR2(64))",
                     createTable.sql());
        assertEquals("select \"id\", \"name\" from \"Users\" where \"name\" = ? order by \"id\" asc offset ? rows fetch next ? rows only",
                     select.sql());
        assertEquals("merge into \"Users\" target using (select ? as \"id\", ? as \"name\" from dual) source "
                             + "on (target.\"id\" = source.\"id\") when matched then update set target.\"name\" = source.\"name\" "
                             + "when not matched then insert (\"id\", \"name\") values (source.\"id\", source.\"name\")",
                     upsert.sql());
    }

    /**
     * 验证 SQL Server 方言先覆盖常用 DDL、分页和 merge upsert。
     */
    @Test
    void sqlServerDialectDrivesSchemaPaginationAndUpsertRendering() {
        DynamicForm form = DynamicForm.builder("userForm", "Users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("name", "NVARCHAR(64)"))
                                      .build();
        RdbDialect dialect = RdbDialect.sqlServer();

        SqlRequest createTable = FormSchemaSqlRenderer.create(dialect).createTable(form).get(0);
        SqlRequest select = FormDataSqlRenderer.create(conditionRenderer(), dialect)
                                               .select(form,
                                                       ConditionGroup.and()
                                                                     .where("name", "=", "王")
                                                                     .build(),
                                                       PageQuery.of(2, 10, PageSort.asc("id")));
        BatchWriteRequest upsert = FormDataSqlRenderer.create(conditionRenderer(), dialect)
                                                    .upsertBatch(form,
                                                                 List.of(orderedMap("id", 1L,
                                                                                    "name", "王")));

        assertEquals("sqlserver", dialect.name());
        assertEquals("create table [Users] ([id] BIGINT primary key, [name] NVARCHAR(64))",
                     createTable.sql());
        assertEquals("select [id], [name] from [Users] where [name] = ? order by [id] asc offset ? rows fetch next ? rows only",
                     select.sql());
        assertEquals("merge into [Users] with (holdlock) as target using (values (?, ?)) as source ([id], [name]) "
                             + "on target.[id] = source.[id] when matched then update set target.[name] = source.[name] "
                             + "when not matched then insert ([id], [name]) values (source.[id], source.[name]);",
                     upsert.sql());
    }

    @Test
    void builtInDialectsExposeJsonTypePlaceholder() {
        assertEquals("JSON", RdbDialect.h2().schema().dataType("JSON"));
        assertEquals("JSON", RdbDialect.mysql().schema().dataType("JSON"));
        assertEquals("JSONB", RdbDialect.postgresql().schema().dataType("JSON"));
        assertEquals("CLOB", RdbDialect.oracle().schema().dataType("JSON"));
        assertEquals("NVARCHAR(max)", RdbDialect.sqlServer().schema().dataType("JSON"));
    }

    @Test
    void mapsOffsetTimeWithoutSilentlyDroppingItsOffset() {
        assertEquals("TIME WITH TIME ZONE", RdbDialect.h2().schema().dataType("OFFSET_TIME"));
        assertEquals("TIME WITH TIME ZONE", RdbDialect.postgresql().schema().dataType("OFFSET_TIME"));
        assertEquals("VARCHAR(32)", RdbDialect.mysql().schema().dataType("OFFSET_TIME"));
        assertEquals("VARCHAR2(32)", RdbDialect.oracle().schema().dataType("OFFSET_TIME"));
        assertEquals("VARCHAR(32)", RdbDialect.sqlServer().schema().dataType("OFFSET_TIME"));
    }

    @Test
    void productionDialectsMapLogicalClobTypes() {
        assertEquals("LONGTEXT", RdbDialect.mysql().schema().dataType("CLOB"));
        assertEquals("LONGTEXT", RdbDialect.mysql().schema().dataType("NCLOB"));
        assertEquals("TEXT", RdbDialect.postgresql().schema().dataType("CLOB"));
        assertEquals("TEXT", RdbDialect.postgresql().schema().dataType("NCLOB"));
        assertEquals("CLOB", RdbDialect.oracle().schema().dataType("CLOB"));
        assertEquals("NCLOB", RdbDialect.oracle().schema().dataType("NCLOB"));
        assertEquals("NVARCHAR(max)", RdbDialect.sqlServer().schema().dataType("CLOB"));
        assertEquals("NVARCHAR(max)", RdbDialect.sqlServer().schema().dataType("NCLOB"));
    }

    @Test
    void builtInDialectsDeclareConservativeOnlineDdlSupport() {
        assertEquals(SchemaOnlineDdlSupport.NONE, RdbDialect.h2().schema().onlineDdlSupport());
        assertEquals(SchemaOnlineDdlSupport.OPERATION_DEPENDENT,
                     RdbDialect.mysql().schema().onlineDdlSupport());
        assertEquals(SchemaOnlineDdlSupport.CONCURRENT_INDEX,
                     RdbDialect.postgresql().schema().onlineDdlSupport());
        assertEquals(SchemaOnlineDdlSupport.LICENSE_OR_EDITION_DEPENDENT,
                     RdbDialect.oracle().schema().onlineDdlSupport());
        assertEquals(SchemaOnlineDdlSupport.LICENSE_OR_EDITION_DEPENDENT,
                     RdbDialect.sqlServer().schema().onlineDdlSupport());
    }

    private static SqlRenderer conditionRenderer() {
        return SqlRenderer.builder()
                          .addTerm(SqlTermHandler.equalsTo())
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
