package com.flying.orm.rdb.array;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证前端数组条件只能生成参数化 PostgreSQL 数组 SQL，并在渲染前拒绝不安全形态。 */
class ArrayTermHandlersTest {

    @Test
    void compilesFrontendArrayConditionIntoOneTypedParameter() {
        DynamicForm form = arrayForm();
        StructuredConditionInput input = StructuredConditionInput.term(
                "tags",
                "array-overlaps",
                Map.of("values", List.of("admin", "auditor")));

        ConditionGroup where = ArrayStructuredConditions.postgresql().compile(form, input);
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(ArrayTermHandlers.postgresql())
                                          .build();
        SqlFragment fragment = renderer.renderWhere(where);

        assertEquals("tags && cast(? as varchar[])", fragment.sql());
        assertEquals(1, fragment.parameters().size());
        assertArrayEquals(new String[]{"admin", "auditor"}, (String[]) fragment.parameters().getFirst());
    }

    /** 编译后的数组条件不能继续引用调用方的可变文本，否则延迟渲染会改变最终绑定参数。 */
    @Test
    void snapshotsMutableArrayConditionElementsBeforeDeferredRendering() {
        StringBuilder mutableRole = new StringBuilder("admin");
        StructuredConditionInput input = StructuredConditionInput.term(
                "tags",
                "array-overlaps",
                List.of(mutableRole));
        ConditionGroup where = ArrayStructuredConditions.postgresql().compile(arrayForm(), input);

        mutableRole.replace(0, mutableRole.length(), "auditor");
        SqlFragment fragment = SqlRenderer.builder()
                                          .addTermPackage(ArrayTermHandlers.postgresql())
                                          .build()
                                          .renderWhere(where);

        assertArrayEquals(new String[]{"admin"}, (String[]) fragment.parameters().getFirst());
    }

    @Test
    void rejectsUnsafeArrayConditionShapesBeforeRenderingSql() {
        ArrayStructuredConditions conditions = ArrayStructuredConditions.postgresql();

        assertThrows(IllegalArgumentException.class,
                     () -> conditions.compile(arrayForm(),
                                              StructuredConditionInput.term(
                                                      "name",
                                                      "array-overlaps",
                                                      List.of("not-an-array-field"))));
        assertThrows(IllegalArgumentException.class,
                     () -> conditions.compile(arrayForm(),
                                              StructuredConditionInput.term(
                                                      "tags",
                                                      "array-overlaps",
                                                      List.of(List.of("nested")))));
        assertThrows(IllegalArgumentException.class,
                     () -> ArrayConditionValue.of(List.of("admin"), "VARCHAR); drop table users; --[]"));
    }

    /** 验证不受支持的调用方类型名不会进入公开错误消息。 */
    @Test
    void rejectsUnsupportedArrayTypeWithoutEchoingCallerInput() {
        String callerType = "secret_" + "x".repeat(8_192) + "[]";

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> ArrayConditionValue.of(List.of("admin"), callerType));

        assertEquals("unsupported PostgreSQL array condition type", failure.getMessage());
    }

    /** PostgreSQL information_schema 返回的内部数组类型名必须生成等价且精确的安全 cast。 */
    @Test
    void rendersExactCastsForPostgresqlArrayMetadataNames() {
        assertEquals("smallint[]", new ArrayConditionValue(List.of(1), "int2[]").postgresqlCastType());
        assertEquals("real[]", new ArrayConditionValue(List.of(1.5), "float4[]").postgresqlCastType());
        assertEquals("time with time zone[]",
                     new ArrayConditionValue(List.of("10:15:30+08:00"), "timetz[]").postgresqlCastType());
        assertEquals("timestamp with time zone[]",
                     new ArrayConditionValue(
                             List.of("2026-08-12T10:15:30+08:00"), "timestamptz[]").postgresqlCastType());
        assertEquals("character[]",
                     new ArrayConditionValue(List.of("A"), "bpchar[]").postgresqlCastType());
    }

    private static DynamicForm arrayForm() {
        return DynamicForm.builder("users", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("tags", "VARCHAR[]"))
                          .build();
    }
}
