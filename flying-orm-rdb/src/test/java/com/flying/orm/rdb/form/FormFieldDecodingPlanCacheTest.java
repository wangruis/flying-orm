package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class FormFieldDecodingPlanCacheTest {

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
                    DynamicForm.Builder plainBuilder = entity.relationIdentity()
                            .map(identity -> DynamicForm.relationalBuilder(entity.id(), identity))
                            .orElseGet(() -> DynamicForm.builder(entity.id(), entity.table()));
                    DynamicForm plain = plainBuilder.addField(new DynamicField(
                            field.identity(), field.databaseType(), field.primaryKey(), field.nullable(),
                            field.unique(), field.length(), field.precision(), field.scale(), field.comment(),
                            field.generation())).build();
                    assertEquals(entity.structureFingerprint(), plain.structureFingerprint());
                    assertNotSame(field, plain.field("number"));
                    FormDataSqlRenderer renderer = renderer()
                            .withEntityFieldCodecs(descriptor.customFieldCodecs())
                            .withResultPlanCachePolicy(CacheRegionPolicy.sqlPlanDefaults());
                    try (EntityModelRegistry models = EntityModelRegistry.create(
                            CacheRegionPolicy.entityMappingDefaults())) {
                        FormResultDecoder decoder = new FormResultDecoder(renderer, models);
                        List<DynamicForm> order = entityFirst ? List.of(entity, plain) : List.of(plain, entity);
                        for (DynamicForm form : order) {
                            DynamicRow raw = DynamicRow.copyOf(Map.of("number", "A-17"));
                            DynamicRow result = reactive
                                    ? decoder.decodeRows(form, Flux.just(raw), SqlExecutionOptions.safeDefaults())
                                            .single().block()
                                    : decoder.decodeRows(form, List.of(raw), SqlExecutionOptions.safeDefaults()).getFirst();
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
        FormDataSqlRenderer original = renderer().withEntityFieldCodecs(descriptor.customFieldCodecs());
        FormFieldDecodingPlan custom = original.resultDecodingPlan(descriptor.form());
        FormDataSqlRenderer plain = original.withEntityFieldCodecs(Map.of());

        assertSame(FormFieldDecodingPlan.EMPTY, plain.resultDecodingPlan(descriptor.form()));
        assertSame(custom, original.resultDecodingPlan(descriptor.form()));
        assertNotSame(custom, plain.withEntityFieldCodecs(descriptor.customFieldCodecs())
                .resultDecodingPlan(descriptor.form()));
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
    }

    private static EntitySchemaDescriptor<Account> descriptor() {
        EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
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
        return EntitySchemaDescriptor.builder(Account.class).typeMappings(mappings).build();
    }

    @TableName("accounts")
    private record Account(@TableColumn(databaseTypeId = "account-number") AccountNumber number) {
    }

    private record AccountNumber(String value) {
    }
}
