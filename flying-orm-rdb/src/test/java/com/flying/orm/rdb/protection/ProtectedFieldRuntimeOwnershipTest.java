package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class ProtectedFieldRuntimeOwnershipTest {

    @Test
    void mutatingCodecCannotChangeRepeatedProtectedQueries() {
        DynamicForm form = DynamicForm.builder("customers", "customers")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("phone", "VARCHAR"))
                                      .encrypted("phone", EncryptedFieldDefinition.builder().build())
                                      .build();
        ConditionGroup where = ConditionGroup.and()
                                                 .add(ProtectedConditions.exact("phone", new byte[]{1}))
                                                 .build();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new MutatingBytesCodec());

        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            TermCondition first = firstTerm(runtime.prepareQuery(form, form, where, DataScope.none(), codecs));
            TermCondition second = firstTerm(runtime.prepareQuery(form, form, where, DataScope.none(), codecs));

            assertArrayEquals(firstToken(first), firstToken(second));
        }
    }

    @Test
    void publicNoProtectionPathStillOwnsMutableCallerValues() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("payload", "VARBINARY"))
                                      .build();
        byte[] payload = {1, 2, 3};
        Map<String, Object> callerValues = new LinkedHashMap<>();
        callerValues.put("payload", payload);

        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.withoutKeys()) {
            ProtectedFieldRuntime.PreparedWrite prepared = runtime.prepareWrite(
                    form, callerValues, DataScope.none(), ValueCodecRegistry.standard());
            callerValues.put("payload", new byte[]{9, 9, 9});
            assertNotSame(callerValues, prepared.ownedValues());
            assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) prepared.ownedValues().get("payload"));
        }
    }

    @Test
    void mixedProtectionPathStillOwnsUnencryptedMutableCallerValues() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("phone", "VARCHAR"))
                                      .addField(DynamicField.of("payload", "VARBINARY"))
                                      .encrypted("phone", EncryptedFieldDefinition.builder().build())
                                      .build();
        byte[] payload = {1, 2, 3};
        Map<String, Object> callerValues = new LinkedHashMap<>();
        callerValues.put("phone", "13800000000");
        callerValues.put("payload", payload);

        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            ProtectedFieldRuntime.PreparedWrite prepared = runtime.prepareWrite(
                    form, callerValues, DataScope.none(), ValueCodecRegistry.standard());
            payload[0] = 9;

            assertNotSame(payload, prepared.ownedValues().get("payload"));
            assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) prepared.ownedValues().get("payload"));
        }
    }

    @Test
    void protectedWorkReusesRequestsThatAlreadyOwnTheirParameters() {
        byte[] source = {1, 2, 3};
        SqlRequest request = new SqlRequest(
                "insert into customers(phone) values (?)", List.of(source));
        Object ownedParameter = request.parameters().getFirst();

        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                request,
                null,
                List.of("id"),
                Map.of("id", 7L),
                "id = ?",
                "delete from customer_tokens where id = ? and field_tag = ?",
                "insert into customer_tokens(id, field_tag, token_value) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{9}))));

        assertSame(request, work.writeRequest());
        assertSame(ownedParameter, work.writeRequest().parameters().getFirst());
    }

    private static TermCondition firstTerm(ProtectedFieldRuntime.PreparedQuery query) {
        return (TermCondition) query.where().children().getFirst();
    }

    private static byte[] firstToken(TermCondition term) {
        return (byte[]) ((java.util.List<?>) term.value()).getFirst();
    }

    private static final class MutatingBytesCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == byte[].class;
        }

        @Override
        public Object write(Object value) {
            byte[] bytes = (byte[]) value;
            return "phone-" + ++bytes[0];
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value;
        }
    }
}
