package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtectedContainsResultSupportTest {

    @Test
    void doesNotSnapshotTrustedProjectionForEveryVerifiedRow() {
        DynamicForm form = protectedForm();
        CountingProjectionList projection = new CountingProjectionList("secret", "id");
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            ProtectedContainsResultSupport support = new ProtectedContainsResultSupport(
                    FormDataSqlRenderer.create(
                            SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                            .withProtectedFields(runtime));

            List<DynamicRow> verified = support.finish(
                    form, query(form),
                    List.of(row(1L, "alphabet soup", "first"), row(2L, "alphabet", "second")),
                    projection, SensitiveDisplayMode.FULL);

            assertEquals(2, verified.size());
            assertEquals(0, projection.toArrayCalls);
        }
    }

    @Test
    void keepsCandidateLimitPlaintextVerificationMaskingAndProjection() {
        DynamicForm form = protectedForm();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            ProtectedContainsResultSupport support = new ProtectedContainsResultSupport(
                    FormDataSqlRenderer.create(
                            SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                            .withProtectedFields(runtime));
            List<DynamicRow> candidates = List.of(
                    row(1L, "alphabet soup", "first"),
                    row(2L, "goodbye", "second"));

            assertThrows(ProtectedSearchCandidateLimitExceededException.class,
                         () -> ProtectedContainsResultSupport.requireCandidateLimit(
                                 ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT + 1));

            List<DynamicRow> verified = support.finish(
                    form, query(form), candidates, List.of("secret", "id"), SensitiveDisplayMode.MASKED);

            assertEquals(1, verified.size());
            assertEquals(List.of("secret", "id"), verified.getFirst().keySet().stream().toList());
            assertEquals("*************", verified.getFirst().get("secret"));
            assertEquals(1L, verified.getFirst().get("id"));
            assertThrows(UnsupportedOperationException.class,
                         () -> verified.add(row(3L, "alphabet", "third")));
        }
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("protected-contains", "protected_contains")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("secret", "VARCHAR"))
                          .addField(DynamicField.of("note", "VARCHAR"))
                          .encrypted("secret", EncryptedFieldDefinition.builder()
                                                                        .searchModes(EncryptedSearchMode.CONTAINS)
                                                                        .build())
                          .masked("secret", MaskedFieldDefinition.builder("full").build())
                          .build();
    }

    private static ProtectedFieldRuntime.PreparedContainsQuery query(DynamicForm form) {
        return new ProtectedFieldRuntime.PreparedContainsQuery(
                form,
                ConditionGroup.and().build(),
                List.of("id", "secret", "note"),
                Set.of("secret"),
                "secret",
                "secret-tag",
                "pha",
                List.of(new ProtectedFieldRuntime.ContainsTokenGroup("v1", List.of(new byte[]{1}))),
                1,
                List.of("id"),
                "protected_contains_tokens");
    }

    private static DynamicRow row(long id, String secret, String note) {
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        values.put("id", id);
        values.put("secret", secret);
        values.put("note", note);
        return DynamicRow.copyOf(values);
    }

    private static final class CountingProjectionList extends AbstractList<String> {
        private final List<String> values;
        private int toArrayCalls;

        private CountingProjectionList(String... values) {
            this.values = List.of(values);
        }

        @Override
        public String get(int index) {
            return values.get(index);
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public Object[] toArray() {
            toArrayCalls++;
            return super.toArray();
        }
    }
}
