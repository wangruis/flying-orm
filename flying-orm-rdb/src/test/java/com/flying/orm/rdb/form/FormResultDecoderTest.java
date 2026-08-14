package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 结果解码是表单查询内部的一项独立工作。这里直接锁住它的契约，避免以后调整客户端流程时，
 * JSON、数组等数据库值在没有报错的情况下悄悄退回驱动原始类型。
 */
class FormResultDecoderTest {

    @Test
    void decodesOnlyFieldsDeclaredByTheDynamicForm() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build(),
                RdbDialect.mysql());
        FormResultDecoder decoder = new FormResultDecoder(
                renderer,
                EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()));
        DynamicForm form = DynamicForm.builder("profiles", "profiles")
                                      .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                      .addField(DynamicField.of("profile", "JSON"))
                                      .addField(DynamicField.of("tags", "VARCHAR[]"))
                                      .build();
        DynamicRow row = DynamicRow.copyOf(Map.of(
                "id", "u1",
                "profile", "{\"name\":\"Alice\"}",
                "tags", new String[]{"admin", "editor"},
                "driver_extra", "unchanged"));

        StepVerifier.create(decoder.decodeRows(form, Flux.just(row), SqlExecutionOptions.safeDefaults()))
                    .assertNext(decoded -> {
                        assertEquals(Map.of("name", "Alice"), decoded.get("profile"));
                        assertEquals(List.of("admin", "editor"), decoded.get("tags"));
                        assertEquals("unchanged", decoded.get("driver_extra"));
                    })
                    .verifyComplete();
    }

    @Test
    void keepsDecodedJsonNullInsteadOfReturningTheDriverText() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build(),
                RdbDialect.mysql());
        FormResultDecoder decoder = new FormResultDecoder(
                renderer,
                EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()));
        DynamicForm form = DynamicForm.builder("profiles", "profiles")
                                      .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                      .addField(DynamicField.of("profile", "JSON"))
                                      .build();

        StepVerifier.create(decoder.decodeRows(
                            form,
                            Flux.just(DynamicRow.copyOf(Map.of("id", "u1", "profile", "null"))),
                            SqlExecutionOptions.safeDefaults()))
                    .assertNext(decoded -> {
                        assertNull(decoded.get("profile"));
                        assertTrue(decoded.containsKey("profile"));
                    })
                    .verifyComplete();
    }
}
