package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ReactiveFormBatchValueSnapshotTest {

    @Test
    void batchLayoutUsesTheSameVectorCodecAsSingleWrites() {
        DynamicField vector = DynamicField.of("embedding", "VECTOR").withLength(2);
        DynamicForm form = DynamicForm.builder("documents", "documents").addField(vector).build();
        BatchWriteRequest request = renderer().insertBatch(
                form, List.of(Map.of("embedding", List.of(1.5d, 2.5d))));

        Object[] parameters = Flux.from(request.rows()).blockFirst();

        float[] actual = assertInstanceOf(float[].class, parameters[0]);
        assertEquals(1.5f, actual[0]);
        assertEquals(2.5f, actual[1]);
    }

    @Test
    void listRendererEncodesContainersBeforeParameterConsumption() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                                      .addField(DynamicField.of("payload", "JSON"))
                                      .addField(DynamicField.of("embedding", "VECTOR").withLength(2))
                                      .build();
        List<Object> jsonItems = new ArrayList<>(List.of("first"));
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("items", jsonItems);
        List<Double> vector = new ArrayList<>(List.of(1.5d, 2.5d));

        BatchWriteRequest request = renderer().insertBatch(
                form, List.of(Map.of("payload", json, "embedding", vector)));
        jsonItems.add("later");
        vector.set(0, 9.5d);
        Object[] bound = Flux.from(request.rows()).blockFirst();

        String encodedJson = java.util.Arrays.stream(bound)
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .findFirst()
                .orElseThrow();
        float[] actual = java.util.Arrays.stream(bound)
                .filter(float[].class::isInstance)
                .map(float[].class::cast)
                .findFirst()
                .orElseThrow();
        assertEquals("{\"items\":[\"first\"]}", encodedJson);
        assertEquals(1.5f, actual[0]);
    }

    @Test
    void batchLayoutAppliesUuidToVarcharFallback() {
        DynamicField code = DynamicField.of("code", "VARCHAR");
        DynamicForm form = DynamicForm.builder("documents", "documents").addField(code).build();
        UUID source = UUID.fromString("ed82786d-c33b-4b2d-b3d7-c5769332a773");

        BatchWriteRequest request = renderer().insertBatch(form, List.of(Map.of("code", source)));
        Object[] parameters = Flux.from(request.rows()).blockFirst();

        assertEquals(source.toString(), parameters[0]);
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

}
