package com.flying.orm.rdb.api;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.schema.FormSchemaSqlRenderer;
import com.flying.orm.rdb.schema.SchemaDialect;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 验证结构 SQL 渲染器只保留显式方言工厂，使用方不会误用隐含默认值。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
class FormSchemaSqlRendererApiTest {

    /**
     * 通用结构规则仍可使用，但必须显式选择，不能因为漏传方言而悄悄回退。
     */
    @Test
    void createsExplicitStandardRendererFromExternalPackage() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(SchemaDialect.standard());

        SqlRequest request = renderer.createTable(form()).get(0);

        assertEquals("create table Users (id BIGINT primary key)", request.sql());
    }

    /**
     * 自定义结构规则走同一个工厂入口。
     */
    @Test
    void createsDialectRendererFromExternalPackage() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(SchemaDialect.builder()
                                                                                   .quoteIdentifiers('"')
                                                                                   .build());

        SqlRequest request = renderer.createTable(form()).get(0);

        assertEquals("create table \"Users\" (\"id\" BIGINT primary key)", request.sql());
    }

    @Test
    void keepsBuiltInLockTimeoutStyleInsideSchemaPackage() throws ClassNotFoundException {
        Class<?> style = Class.forName("com.flying.orm.rdb.schema.SchemaLockTimeoutStyle");
        Class<?> guard = Class.forName("com.flying.orm.rdb.schema.SchemaDdlSessionGuard");

        assertFalse(Modifier.isPublic(style.getModifiers()));
        assertFalse(Modifier.isPublic(guard.getModifiers()));
    }

    @Test
    void hidesConstructorsAndRequiresAnExplicitDialect() {
        assertEquals(0L,
                     Arrays.stream(FormSchemaSqlRenderer.class.getDeclaredConstructors())
                           .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
                           .count());
        assertEquals(2L,
                     Arrays.stream(FormSchemaSqlRenderer.class.getDeclaredMethods())
                           .filter(method -> method.getName().equals("create"))
                           .filter(method -> Modifier.isPublic(method.getModifiers()))
                           .count());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .build();
    }
}
