package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class FormFieldDecodingPlanCacheTest {

    @Test
    void explicitFieldCodecsRemainStableAfterTheInputMapChanges() {
        EntitySchemaDescriptor<Account> descriptor = descriptor();
        DynamicForm form = descriptor.form();
        Map<DynamicField, EntityTypeMappingRegistry.Mapping> mappings = new IdentityHashMap<>();
        mappings.put(form.field("number"), mappings().resolve("account-number", AccountNumber.class));
        FormDataSqlRenderer renderer = renderer().withEntityFieldCodecs(mappings);
        FormFieldDecodingPlan cached = renderer.resultDecodingPlan(form);

        mappings.clear();
        assertSame(cached, renderer.resultDecodingPlan(form));
        renderer.resultPlanInvalidator().invalidate("accounts");
        assertNotSame(FormFieldDecodingPlan.EMPTY, renderer.resultDecodingPlan(form));
        assertEquals(List.of("A-17"), renderer.insert(form,
                Map.of("number", new AccountNumber("A-17"))).parameters());
    }

    @Test
    void compiledEntityFieldsBindReadAndWriteWithoutASchemaDescriptor() {
        EntityMetadata<Account> model = EntityMetadataResolver.compileModel(Account.class, mappings()).metadata();
        FormDataSqlRenderer renderer = renderer().withEntityModels(List.of(model));
        assertEquals(List.of("A-17"), renderer.insert(model.toDynamicForm(),
                Map.of("number", new AccountNumber("A-17"))).parameters());
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);
            DynamicRow decoded = decoder.decodeRows(model.toDynamicForm(),
                    Flux.just(DynamicRow.copyOf(Map.of("number", "A-17"))),
                    SqlExecutionOptions.unlimited()).single().block();
            assertEquals(new AccountNumber("A-17"), decoded.get("number"));
        }
    }

    @Test
    void explicitFieldCodecsDoNotLeakToEqualButUnregisteredFields() {
        DynamicForm entity = descriptor().form();
        DynamicField field = entity.field("number");
        DynamicField other = new DynamicField(field.column(), field.primaryKey(), field.unique(),
                field.runtimeDefault(), field.features());
        DynamicForm plain = DynamicForm.relationalBuilder(entity.id(), entity.relationIdentity())
                .addField(other).build();
        assertEquals(field, plain.field("number"));
        assertNotSame(field, plain.field("number"));
        FormDataSqlRenderer renderer = renderer().withEntityFieldCodecs(Map.of(
                field, mappings().resolve("account-number", AccountNumber.class)));

        assertNotSame(FormFieldDecodingPlan.EMPTY, renderer.resultDecodingPlan(entity));
        assertSame(FormFieldDecodingPlan.EMPTY, renderer.resultDecodingPlan(plain));
        assertEquals(List.of("plain"), renderer.insert(plain, Map.of("number", "plain")).parameters());
    }

    @Test
    void ordinaryFormsShareTheEmptyDecodingPlan() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        DynamicForm form = DynamicForm.builder("items", "items")
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .build();

        FormFieldDecodingPlan first = FormFieldDecodingPlan.compile(form, renderer);
        FormFieldDecodingPlan second = FormFieldDecodingPlan.compile(form, renderer);

        assertSame(first, second);
    }

    @Test
    void reusesAFormStructurePlanAcrossEquivalentFormInstancesAndInvalidatesWithMetadata() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        DynamicForm firstForm = DynamicForm.builder("items", "items")
                                           .addField(DynamicField.of("payload", "JSON"))
                                           .build();
        DynamicForm secondForm = DynamicForm.builder("items", "items")
                                            .addField(DynamicField.of("payload", "JSON"))
                                            .build();
        FormFieldDecodingPlan first = renderer.resultDecodingPlan(firstForm);
        FormFieldDecodingPlan second = renderer.resultDecodingPlan(secondForm);

        assertSame(first, second);
        renderer.resultPlanInvalidator().invalidate("items");
        assertNotSame(first, renderer.resultDecodingPlan(secondForm));
    }

    @TestFactory
    Stream<DynamicTest> sameStructureDoesNotShareEntityCodecsWithPlainForms() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(entityFirst ->
                DynamicTest.dynamicTest("reactive=" + reactive + ", entityFirst=" + entityFirst, () -> {
                    EntitySchemaDescriptor<Account> descriptor = descriptor();
                    DynamicForm entity = descriptor.form();
                    DynamicField field = entity.field("number");
                    DynamicForm.Builder plainBuilder = DynamicForm.relationalBuilder(entity.id(), entity.relationIdentity());
                    DynamicForm plain = plainBuilder.addField(new DynamicField(
                            field.column(), field.primaryKey(), field.unique())).build();
                    assertEquals(entity.structureFingerprint(), plain.structureFingerprint());
                    assertNotSame(field, plain.field("number"));
                    FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                            SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2(),
                            StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()),
                            CacheRegionPolicy.sqlPlanDefaults(), ProtectedFieldRuntime.withoutKeys(),
                            List.of(descriptor.metadata()));
                    try (EntityModelRegistry models = EntityModelRegistry.create(
                            CacheRegionPolicy.entityMappingDefaults())) {
                        FormResultDecoder decoder = new FormResultDecoder(renderer, models);
                        List<DynamicForm> order = entityFirst ? List.of(entity, plain) : List.of(plain, entity);
                        for (DynamicForm form : order) {
                            DynamicRow raw = DynamicRow.copyOf(Map.of("number", "A-17"));
                            DynamicRow result = reactive
                                    ? decoder.decodeRows(form, Flux.just(raw), SqlExecutionOptions.unlimited())
                                            .single().block()
                                    : decoder.rowDecoder(form, SqlExecutionOptions.unlimited(),
                                            com.flying.orm.core.scope.DataScope.none(),
                                            com.flying.orm.core.protection.SensitiveDisplayMode.DECLARED).map(raw);
                            assertEquals(form == entity ? new AccountNumber("A-17") : "A-17", result.get("number"));
                        }
                        assertSame(renderer.resultDecodingPlan(entity), renderer.resultDecodingPlan(entity));
                        FormFieldDecodingPlan cached = renderer.resultDecodingPlan(entity);
                        renderer.resultPlanInvalidator().invalidate("accounts");
                        assertNotSame(cached, renderer.resultDecodingPlan(entity));
                    }
                })));
    }

    @Test
    void changingEntityCodecsDoesNotReuseThePreviousRendererCache() {
        EntitySchemaDescriptor<Account> descriptor = descriptor();
        FormDataSqlRenderer original = renderer().withEntityModels(java.util.List.of(descriptor.metadata()));
        FormFieldDecodingPlan custom = original.resultDecodingPlan(descriptor.form());
        FormDataSqlRenderer plain = original.withEntityFieldCodecs(Map.of());

        assertSame(FormFieldDecodingPlan.EMPTY, plain.resultDecodingPlan(descriptor.form()));
        assertSame(custom, original.resultDecodingPlan(descriptor.form()));
        assertNotSame(custom, plain.withEntityModels(java.util.List.of(descriptor.metadata()))
                .resultDecodingPlan(descriptor.form()));
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
    }

    private static EntitySchemaDescriptor<Account> descriptor() {
        return EntitySchemaDescriptor.builder(Account.class).typeMappings(mappings()).build();
    }

    private static EntityTypeMappingRegistry mappings() {
        return EntityTypeMappingRegistry.builder()
                .register("account-number", AccountNumber.class, DatabaseType.of("VARCHAR(32)"), new ValueCodec() {
                    @Override
                    public boolean supports(Class<?> type) {
                        return type == AccountNumber.class;
                    }

                    @Override
                    public Object write(Object value) {
                        return value == null ? null : ((AccountNumber) value).value();
                    }

                    @Override
                    public Object read(Object value, Class<?> type) {
                        return value == null ? null : new AccountNumber(value.toString());
                    }
                }).build();
    }

    @TableName("accounts")
    private record Account(@TableColumn(databaseTypeId = "account-number") AccountNumber number) {
    }

    private record AccountNumber(String value) {
    }
}
