package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchOptimisticUpdateValueSnapshotTest {

    @Test
    void freezesMutableTextWithoutChangingTheLogicalCodecType() {
        StringBuilder builder = new StringBuilder("builder");
        StringBuffer buffer = new StringBuffer("buffer");
        CharBuffer chars = CharBuffer.wrap(new char[]{'c', 'h', 'a', 'r'});
        BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                Map.of("builder", builder, "buffer", buffer, "chars", chars),
                ConditionGroup.and().where("id", "=", 1L).build(),
                OptimisticLockOptions.increment("version", 1L));
        builder.setCharAt(0, 'X');
        buffer.setCharAt(0, 'X');
        chars.put(0, 'X');

        Map<String, Object> exported = update.values();
        StringBuilder exportedBuilder = assertInstanceOf(StringBuilder.class, exported.get("builder"));
        StringBuffer exportedBuffer = assertInstanceOf(StringBuffer.class, exported.get("buffer"));
        CharBuffer exportedChars = assertInstanceOf(CharBuffer.class, exported.get("chars"));
        assertEquals("builder", exportedBuilder.toString());
        assertEquals("buffer", exportedBuffer.toString());
        assertEquals("char", exportedChars.toString());
        exportedBuilder.setCharAt(0, 'Y');
        exportedBuffer.setCharAt(0, 'Y');
        exportedChars.position(1);

        assertEquals("builder", update.values().get("builder").toString());
        assertEquals("buffer", update.values().get("buffer").toString());
        assertEquals("char", update.values().get("chars").toString());
    }

    @Test
    void freezesMutableUpdateValuesAtConstruction() {
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1});
        BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                Map.of("payload", source),
                ConditionGroup.and().where("id", "=", 1L).build(),
                OptimisticLockOptions.increment("version", 1L));

        source.put(0, (byte) 9);

        ByteBuffer published = (ByteBuffer) update.values().get("payload");
        assertEquals(1, published.get(0));
        assertTrue(published.isReadOnly());
    }

    @Test
    void batchPlanningReusesTheRowValuesAlreadyOwnedAtConstruction() {
        BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                Map.of("payload", ByteBuffer.wrap(new byte[]{1})),
                ConditionGroup.and().where("id", "=", 1L).build(),
                OptimisticLockOptions.increment("version", 1L));
        Map<String, Object> owned = ownedValues(update);
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("payload", "VARBINARY"))
                                      .addField(DynamicField.of("version", "BIGINT"))
                                      .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeSupport scopes = new FormScopeSupport(
                renderer, StructuredConditionResolver.defaults(), DataScope.none());

        FormScopeSupport.PreparedBatchUpdate prepared =
                scopes.prepareBatchUpdate(form, update, DataScope.none());

        assertSame(owned, prepared.logicalValues());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> ownedValues(BatchOptimisticUpdate update) {
        Method method = java.util.Arrays.stream(BatchOptimisticUpdate.class.getDeclaredMethods())
                .filter(candidate -> candidate.getName().equals("ownedValues")
                        && candidate.getParameterCount() == 0)
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "BatchOptimisticUpdate must expose a non-public owned-values seam"));
        try {
            method.setAccessible(true);
            return (Map<String, Object>) method.invoke(update);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("owned-values seam must be callable", failure);
        }
    }
}
