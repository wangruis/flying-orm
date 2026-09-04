package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

class FormWriteOwnershipConvergenceTest {

    @Test
    void noTenantAndNoProtectionReuseTheWriteSpecOwnedValues() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .build();
        WriteSpec spec = WriteSpec.insert(form, Map.of("name", "created"));
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeGuard guard = new FormScopeGuard(
                renderer, StructuredConditionResolver.defaults(), DataScope.none());

        Map<String, Object> scoped = guard.prepareWriteValues(
                form, spec.ownedValues(), guard.effectiveScope(spec.scope()));
        FormPreparedWrite prepared = renderer.protection().prepareWrite(
                form, scoped, DataScope.none());

        assertSame(spec.ownedValues(), scoped);
        assertSame(scoped, prepared.values());
    }

    @Test
    void directPublicRendererStillSnapshotsMutableBinaryParameters() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("payload", "VARBINARY"))
                                      .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        byte[] source = {1, 2, 3};

        com.flying.orm.core.sql.render.SqlRequest request = renderer.insert(
                form, Map.of("payload", source));
        source[0] = 9;

        assertEquals(1, ((byte[]) request.parameters().getFirst())[0]);
    }

    @Test
    void taskThreeOwnedSeamsDoNotExpandThePublicReflectionSurface() {
        assertFalse(publicMethod(SqlRequest.class, "owned"));
        assertFalse(publicMethod(ProtectedFieldRuntime.class, "prepareOwnedWrite"));
        assertFalse(publicMethod(SqlTemplateRegistry.class, "entry"));
        assertFalse(Arrays.stream(SqlTemplateRegistry.class.getDeclaredClasses())
                          .anyMatch(type -> type.getSimpleName().equals("Entry")
                                  && Modifier.isPublic(type.getModifiers())));
        assertFalse(publicMethod(ArrayValueCodec.class, "readList"));
    }

    @Test
    void publicSqlRequestCannotForgeOwnedMutableValues() {
        byte[] bytes = {1};
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{2});
        SqlRequest request = new SqlRequest("select ?, ?", List.of(bytes, buffer));

        bytes[0] = 9;
        buffer.put(0, (byte) 8);

        assertEquals(1, ((byte[]) request.parameters().get(0))[0]);
        assertEquals(2, ((ByteBuffer) request.parameters().get(1)).get(0));
    }

    @Test
    void matchingCanonicalAutoTenantReusesTheOwnedMap() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("tenant_id", "BIGINT"))
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .tenant("tenant_id", TenantStrategy.AUTO)
                                      .build();
        WriteSpec spec = WriteSpec.insert(form, Map.of("tenant_id", 7L, "name", "created"));
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeGuard guard = new FormScopeGuard(
                renderer, StructuredConditionResolver.defaults(), DataScope.tenant("tenant_id", 7L));

        Map<String, Object> prepared = guard.prepareWriteValues(
                form, spec.ownedValues(), guard.effectiveScope(spec.scope()));

        assertSame(spec.ownedValues(), prepared);
    }

    private static boolean publicMethod(Class<?> type, String name) {
        return Arrays.stream(type.getDeclaredMethods())
                     .anyMatch(method -> method.getName().equals(name)
                             && Modifier.isPublic(method.getModifiers()));
    }
}
