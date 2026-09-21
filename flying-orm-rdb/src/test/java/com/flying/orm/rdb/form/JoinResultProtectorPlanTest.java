package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class JoinResultProtectorPlanTest {

    @Test
    void h2CatalogFoldingDoesNotChangeProtectedSelfJoinProjectionAliases() throws Exception {
        for (boolean lower : List.of(false, true)) {
            var source = new org.h2.jdbcx.JdbcDataSource();
            source.setURL("jdbc:h2:mem:protected_alias_" + java.util.UUID.randomUUID()
                    + ";DATABASE_TO_LOWER=" + lower);
            try (var connection = source.getConnection(); var statement = connection.createStatement();
                 ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                         ProtectedFieldKeyRing.single("v1", new byte[32]))) {
                statement.execute("create table people(id bigint primary key, phone varchar(32))");
                statement.execute("insert into people values(1, '13800138000')");
                DynamicForm people = DynamicForm.builder("alias-people", "people")
                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                        .addField(DynamicField.of("phone", "VARCHAR"))
                        .masked("phone", MaskedFieldDefinition.builder("partial").prefix(3).suffix(2).build())
                        .build();
                JoinQuerySpec.Builder builder = JoinQuerySpec.builder(people);
                JoinSource owner = builder.root();
                JoinSource peer = builder.join(com.flying.orm.core.join.JoinType.LEFT, people, owner, "id", "id");
                JoinQuerySpec spec = builder.selectAs(owner, "id", "owner_id")
                        .selectAs(owner, "phone", "OwnerPhone")
                        .selectAs(peer, "phone", "peer_phone").masked().build();
                FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2()).withProtectedFields(runtime);
                var request = renderer.joinQueries().select(spec, Map.of());
                DynamicRow row = com.flying.orm.rdb.sync.SyncSqlExecutor.jdbc(
                        com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(source), RdbDialect.h2()).query(request).getFirst();
                DynamicRow transformed = new JoinResultProtector(renderer)
                        .plan(spec, Map.of(owner, DataScope.none(), peer, DataScope.none()), SensitiveDisplayMode.MASKED)
                        .transform(row);
                assertEquals(List.of("owner_id", "OwnerPhone", "peer_phone"), new ArrayList<>(transformed.keySet()));
                assertEquals(1L, ((Number) transformed.get("owner_id")).longValue());
                assertEquals("138******00", transformed.get("OwnerPhone"));
                assertEquals("138******00", transformed.get("peer_phone"));
            }
        }
    }

    @Test
    void returnsDecodedRowDirectlyWhenProjectedFieldsNeedNoProtection() {
        DynamicForm form = DynamicForm.builder("people", "people")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource root = builder.root();
        JoinQuerySpec spec = builder.selectAs(root, "id", "person_id")
                                    .selectAs(root, "name", "person_name")
                                    .build();
        DynamicRow row = DynamicRow.copyOf(Map.of("person_id", 7L, "person_name", "Ada"));
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());

        DynamicRow transformed = new JoinResultProtector(renderer)
                .plan(spec, Map.of(root, DataScope.none()), spec.sensitiveDisplayMode())
                .transform(row);

        assertSame(row, transformed,
                   "an ordinary JOIN row must not be split into source maps and rebuilt");
    }

    @Test
    void returnsDecodedRowDirectlyWhenMaskedProjectionRequestsFullDisplay() {
        DynamicForm form = DynamicForm.builder("people-full", "people_full")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("phone", "VARCHAR"))
                                      .masked("phone", MaskedFieldDefinition.builder("partial")
                                                                             .prefix(3)
                                                                             .suffix(2)
                                                                             .build())
                                      .build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource root = builder.root();
        JoinQuerySpec spec = builder.selectAs(root, "phone", "contact")
                                    .showSensitive()
                                    .build();
        DynamicRow row = DynamicRow.copyOf(Map.of("contact", "13800138000"));
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());

        DynamicRow transformed = new JoinResultProtector(renderer)
                .plan(spec, Map.of(root, DataScope.none()), SensitiveDisplayMode.FULL)
                .transform(row);

        assertSame(row, transformed,
                   "FULL display must not rebuild a row that only declares masking");
    }

    @Test
    void decryptsAndMasksProjectedFieldsWithoutChangingAliasesOrOuterJoinNulls() {
        DynamicForm people = DynamicForm.builder("people-protected", "people_protected")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("phone", "VARCHAR"))
                                        .encrypted("phone", EncryptedFieldDefinition.builder().build())
                                        .masked("phone", MaskedFieldDefinition.builder("partial")
                                                                               .prefix(3)
                                                                               .suffix(2)
                                                                               .build())
                                        .build();
        DynamicForm owners = DynamicForm.builder("owners", "owners")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("person_id", "BIGINT"))
                                        .addField(DynamicField.of("label", "VARCHAR"))
                                        .build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(people);
        JoinSource person = builder.root();
        JoinSource owner = builder.join(com.flying.orm.core.join.JoinType.LEFT,
                                        owners, person, "id", "person_id");
        JoinQuerySpec spec = builder.selectAs(person, "phone", "contact")
                                    .selectAs(owner, "label", "owner_label")
                                    .masked()
                                    .build();

        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                    .withProtectedFields(runtime);
            Object ciphertext = runtime.prepareWrite(
                    people, Map.of("phone", "13800138000"), DataScope.none(), renderer.valueCodecs())
                                       .ownedValues()
                                       .get("phone");
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("contact", ciphertext);
            values.put("owner_label", null);
            DynamicRow row = DynamicRow.copyOf(values);

            DynamicRow transformed = new JoinResultProtector(renderer)
                    .plan(spec,
                          Map.of(person, DataScope.none(), owner, DataScope.none()),
                          SensitiveDisplayMode.MASKED)
                    .transform(row);

            assertEquals("138******00", transformed.get("contact"));
            assertNull(transformed.get("owner_label"));
            assertEquals(List.of("contact", "owner_label"),
                         new ArrayList<>(transformed.keySet()),
                         "projection order and aliases must remain unchanged");
        }
    }
}
