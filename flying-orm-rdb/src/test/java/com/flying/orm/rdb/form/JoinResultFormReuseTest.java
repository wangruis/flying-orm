package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import reactor.core.publisher.Flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class JoinResultFormReuseTest {

    @Test
    void ordinaryAndJoinedRowsDoNotCopyAlreadyDecodedValues() {
        DynamicForm form = DynamicForm.builder("plain_rows", "plain_rows")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeSupport scopes = new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none());
        SqlExecutionOptions options = SqlExecutionOptions.unlimited();
        JoinQuerySpec.Builder joined = JoinQuerySpec.builder(form);
        FormReadPlanner.RowReadPlan joinPlan = new JoinQueryPlanner(renderer, scopes, options)
                .plan(joined.select(joined.root(), "id").build(), null);
        FormReadPlanner.RowReadPlan plainPlan = new FormReadPlanner(renderer, scopes, options)
                .select(QuerySpec.of(form, ConditionGroup.and().build()));
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);
            DynamicRow row = DynamicRow.copyOf(Map.of("id", 1L));
            Flux<DynamicRow> rows = Flux.just(row);
            for (FormReadPlanner.RowReadPlan plan : List.of(plainPlan, joinPlan)) {
                assertSame(row, decoder.rowDecoder(plan).map(row));
                assertSame(row, decoder.decodeRows(plan, rows).blockLast());
            }
        }
    }

    @Test
    void sharedDecoderPreservesProtectedAliasesAndOuterJoinNulls() {
        DynamicForm form = DynamicForm.builder("protected_rows", "protected_rows")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("phone", "VARCHAR"))
                .encrypted("phone", EncryptedFieldDefinition.builder().build())
                .masked("phone", MaskedFieldDefinition.builder("partial").prefix(3).suffix(2).build()).build();
        JoinQuerySpec.Builder joined = JoinQuerySpec.builder(form);
        JoinSource peer = joined.join(com.flying.orm.core.join.JoinType.LEFT, form, joined.root(), "id", "id");
        JoinQuerySpec spec = joined.selectAs(joined.root(), "phone", "contact")
                .selectAs(peer, "phone", "peer_contact").masked().build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(ProtectedFieldKeyRing.single("v1", new byte[32]));
             EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()).withProtectedFields(runtime);
            FormReadPlanner.RowReadPlan plan = new JoinQueryPlanner(renderer,
                    new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                    SqlExecutionOptions.unlimited()).plan(spec, null);
            Object ciphertext = runtime.writeOperation(form, ProtectedFormLayout.physical(form),
                    DataScope.none(), renderer.valueCodecs()).prepare(Map.of("phone", "13800138000"))
                    .ownedValues().get("phone");
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("contact", ciphertext);
            values.put("peer_contact", null);
            DynamicRow row = DynamicRow.copyOf(values);
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);
            for (DynamicRow decoded : List.of(decoder.rowDecoder(plan).map(row),
                    decoder.decodeRows(plan, Flux.just(row)).blockLast())) {
                assertEquals("138******00", decoded.get("contact"));
                assertNull(decoded.get("peer_contact"));
                assertEquals(List.of("contact", "peer_contact"), List.copyOf(decoded.keySet()));
            }
        }
    }

    @Test
    void selectAndPageKeepPhysicalTypesAndAliasesForMixedProtectedProjections() {
        DynamicForm form = DynamicForm.builder("join_accounts", "join_accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR").withLength(100))
                .addField(DynamicField.of("note", "VARCHAR").withLength(40))
                .encrypted("secret", EncryptedFieldDefinition.builder().build())
                .build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource root = builder.root();
        JoinQuerySpec spec = builder.selectAs(root, "id", "account_id")
                .selectAs(root, "secret", "protected_value")
                .selectAs(root, "note", "account_note")
                .orderBy(root, "id", PageSort.Direction.ASC).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                    .withProtectedFields(runtime);
            JoinQueryPlanner planner = new JoinQueryPlanner(renderer,
                    new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                    SqlExecutionOptions.unlimited());

            FormReadPlanner.PlannedQuery selectPlan = planner.plan(spec, null);
            FormReadPlanner.PlannedQuery pagePlan = planner.page(spec, PageQuery.of(1, 10), null);
            DynamicForm selected = selectPlan.form();
            DynamicForm paged = pagePlan.form();
            assertNull(selectPlan.countRequest(), "fetch must not introduce a count request");
            assertEquals(List.of(10, 0L), pagePlan.request().parameters());
            assertEquals("select count(*) as total from \"join_accounts\" \"t0\"", pagePlan.countRequest().sql());
            assertEquals(PageQuery.of(1, 10), pagePlan.page());

            assertEquals(List.of("account_id", "protected_value", "account_note"),
                    selected.fields().stream().map(DynamicField::name).toList());
            assertEquals(ProtectedFormLayout.physical(form).field("secret").databaseType(),
                    selected.field("protected_value").databaseType());
            assertEquals(40, selected.field("account_note").length());
            assertEquals(selected.structureFingerprint(), paged.structureFingerprint());
        }
    }
}
