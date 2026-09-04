package com.flying.orm.rdb.form.spec;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 单条写入规格必须在发布时冻结标准可绑定值，而不只冻结 Map 和数组容器。 */
class WriteSpecValueSnapshotTest {

    @Test
    void freezesByteBufferAtSpecCreationAndDoesNotExposeMutableContent() {
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1, 2});
        WriteSpec spec = WriteSpec.insert(form(), Map.of("payload", source));

        source.put(0, (byte) 9);
        ByteBuffer published = (ByteBuffer) spec.values().get("payload");

        assertEquals(1, published.get(0));
        assertThrows(ReadOnlyBufferException.class, () -> published.put(0, (byte) 8));
        assertNotSame(ownedValues(spec).get("payload"), published);
    }

    @Test
    void derivedSpecsReuseAlreadyOwnedValues() {
        WriteSpec spec = WriteSpec.insert(form(), Map.of("payload", ByteBuffer.wrap(new byte[]{1, 2})));

        WriteSpec derived = spec.withScope(com.flying.orm.core.scope.DataScope.none());

        assertSame(ownedValues(spec), ownedValues(derived));
        assertSame(ownedValues(spec).get("payload"), ownedValues(derived).get("payload"));
    }

    @Test
    void freezesJsonAndArrayContainersAtTheFieldAwareBoundary() {
        List<Object> tags = new ArrayList<>(List.of("a"));
        List<Object> nested = new ArrayList<>(List.of("first"));
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("items", nested);
        WriteSpec spec = WriteSpec.insert(containerForm(), Map.of("payload", payload, "tags", tags));

        nested.add("later");
        tags.add("b");

        Map<?, ?> publishedJson = (Map<?, ?>) spec.values().get("payload");
        assertEquals(List.of("first"), publishedJson.get("items"));
        assertEquals(List.of("a"), spec.values().get("tags"));
    }

    @Test
    void preservesUnknownCustomValuesForTheConfiguredCodecContract() {
        Object custom = new Object();

        WriteSpec spec = WriteSpec.insert(form(), Map.of("payload", custom));

        assertSame(custom, ownedValues(spec).get("payload"));
    }

    private static DynamicForm form() {
        return DynamicForm.builder("documents", "documents")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("payload", "BLOB"))
                          .build();
    }

    private static DynamicForm containerForm() {
        return DynamicForm.builder("documents", "documents")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("payload", "JSON"))
                          .addField(DynamicField.of("tags", "VARCHAR[]"))
                          .build();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> ownedValues(WriteSpec spec) {
        Method method = java.util.Arrays.stream(WriteSpec.class.getDeclaredMethods())
                .filter(candidate -> candidate.getName().equals("ownedValues")
                        && candidate.getParameterCount() == 0)
                .findFirst()
                .orElseThrow(() -> new AssertionError("WriteSpec must expose an internal owned-values seam"));
        try {
            method.setAccessible(true);
            return (Map<String, Object>) method.invoke(spec);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("owned-values seam must be callable", failure);
        }
    }
}
