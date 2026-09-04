package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.protection.MaskingPolicyRegistry;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedValueNormalizerRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtectedOperationPlanReuseTest {

    @Test
    void derivesProtectedTenantIdentityOncePerBatchPlan() {
        AtomicInteger writes = new AtomicInteger();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard()
                                                      .withFirst(new CountingTenantCodec(writes));
        DynamicForm form = DynamicForm.builder("contacts", "contacts")
                                      .addField(DynamicField.of("tenant_id", "VARCHAR"))
                                      .addField(DynamicField.of("phone", "VARCHAR"))
                                      .tenant("tenant_id", TenantStrategy.AUTO)
                                      .encrypted("phone", EncryptedFieldDefinition.builder().build())
                                      .build();
        TenantValue tenant = new TenantValue("tenant-a");
        BatchSpec spec = BatchSpec.insert(form, Flux.just(
                                           Map.of("phone", "13800000001"),
                                           Map.of("phone", "13800000002")))
                                  .withScope(DataScope.tenant("tenant_id", tenant));

        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build().withValueCodecs(codecs),
                    RdbDialect.postgresql()).withProtectedFields(runtime);
            NativeSyncFormBatchOperations operations = new NativeSyncFormBatchOperations(
                    new ConsumingExecutor(), renderer, StructuredConditionResolver.defaults(),
                    DataScope.none(), BatchWriteOptions.defaults(),
                    com.flying.orm.core.scope.FieldUsePolicy.unrestricted());

            operations.writeBatch(spec);
        }

        // 两行 SQL 参数各编码一次；受保护字段上下文属于同一批次，只派生一次。
        assertEquals(3, writes.get());
    }

    @Test
    void plansFixedProtectedScopeOncePerSyncBatchUpdate() {
        AtomicInteger normalizations = new AtomicInteger();
        ProtectedValueNormalizerRegistry normalizers = ProtectedValueNormalizerRegistry.standard()
                .with("counting", value -> {
                    normalizations.incrementAndGet();
                    return value;
                });
        DynamicForm form = DynamicForm.builder("protected-sync-update", "protected_sync_update")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("note", "VARCHAR"))
                                      .addField(DynamicField.of("secret", "VARCHAR"))
                                      .addField(DynamicField.of("version", "BIGINT"))
                                      .encrypted("secret", EncryptedFieldDefinition.builder()
                                              .searchModes(EncryptedSearchMode.EXACT)
                                              .normalizer("counting")
                                              .build())
                                      .build();
        BatchOptimisticUpdate first = new BatchOptimisticUpdate(
                Map.of("note", "first"),
                ConditionGroup.and().where("id", "=", 1L).build(),
                OptimisticLockOptions.increment("version", 0L));
        BatchOptimisticUpdate second = new BatchOptimisticUpdate(
                Map.of("note", "second"),
                ConditionGroup.and().where("id", "=", 2L).build(),
                OptimisticLockOptions.increment("version", 0L));
        DataScope scope = DataScope.where(ConditionGroup.and()
                .add(ProtectedConditions.exact("secret", "classified"))
                .build());
        BatchSpec spec = BatchSpec.update(form, Flux.just(first, second)).withScope(scope);

        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), normalizers,
                MaskingPolicyRegistry.standard())) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                                                          .withProtectedFields(runtime);
            NativeSyncFormBatchOperations operations = new NativeSyncFormBatchOperations(
                    new ConsumingExecutor(), renderer, StructuredConditionResolver.defaults(),
                    DataScope.none(), BatchWriteOptions.defaults(),
                    com.flying.orm.core.scope.FieldUsePolicy.unrestricted());

            operations.writeBatch(spec);
        }

        assertEquals(1, normalizations.get(),
                     "the fixed protected Scope must be planned once per synchronous batch");
    }

    @Test
    void derivesProtectedTenantIdentityOncePerResultOperation() {
        AtomicInteger writes = new AtomicInteger();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard()
                                                      .withFirst(new CountingTenantCodec(writes));
        DynamicForm form = DynamicForm.builder("contacts", "contacts")
                                      .addField(DynamicField.of("tenant_id", "VARCHAR"))
                                      .addField(DynamicField.of("phone", "VARCHAR"))
                                      .tenant("tenant_id", TenantStrategy.AUTO)
                                      .encrypted("phone", EncryptedFieldDefinition.builder().build())
                                      .build();
        TenantValue tenant = new TenantValue("tenant-a");
        DataScope scope = DataScope.tenant("tenant_id", tenant);

        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]));
             EntityModelRegistry models = EntityModelRegistry.create(
                     CacheRegionPolicy.entityMappingDefaults())) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build().withValueCodecs(codecs),
                    RdbDialect.postgresql()).withProtectedFields(runtime);
            Object ciphertext = renderer.protection()
                                        .prepareWrite(form, Map.of("phone", "13800000001"), scope)
                                        .values().get("phone");
            writes.set(0);

            List<DynamicRow> decoded = new FormResultDecoder(renderer, models).decodeRows(
                    form,
                    List.of(DynamicRow.copyOf(Map.of("phone", ciphertext)),
                            DynamicRow.copyOf(Map.of("phone", ciphertext))),
                    SqlExecutionOptions.safeDefaults(),
                    scope,
                    SensitiveDisplayMode.DECLARED);

            assertEquals(List.of("13800000001", "13800000001"),
                         decoded.stream().map(row -> row.get("phone")).toList());
        }

        assertEquals(1, writes.get());
    }

    @Test
    void derivesProtectedTenantIdentityOncePerJoinResultPlan() {
        AtomicInteger writes = new AtomicInteger();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard()
                                                      .withFirst(new CountingTenantCodec(writes));
        DynamicForm form = DynamicForm.builder("contacts", "contacts")
                                      .addField(DynamicField.of("tenant_id", "VARCHAR"))
                                      .addField(DynamicField.of("phone", "VARCHAR"))
                                      .tenant("tenant_id", TenantStrategy.AUTO)
                                      .encrypted("phone", EncryptedFieldDefinition.builder().build())
                                      .build();
        TenantValue tenant = new TenantValue("tenant-a");
        DataScope scope = DataScope.tenant("tenant_id", tenant);
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource source = builder.root();
        JoinQuerySpec spec = builder.selectAs(source, "phone", "contact").build();

        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build().withValueCodecs(codecs),
                    RdbDialect.postgresql()).withProtectedFields(runtime);
            Object ciphertext = renderer.protection()
                                        .prepareWrite(form, Map.of("phone", "13800000001"), scope)
                                        .values().get("phone");
            writes.set(0);
            JoinResultProtector.ResultPlan plan = new JoinResultProtector(renderer).plan(
                    spec, Map.of(source, scope), SensitiveDisplayMode.DECLARED);

            List<Object> decoded = List.of(
                    plan.transform(DynamicRow.copyOf(Map.of("contact", ciphertext))).get("contact"),
                    plan.transform(DynamicRow.copyOf(Map.of("contact", ciphertext))).get("contact"));

            assertEquals(List.of("13800000001", "13800000001"), decoded);
        }

        assertEquals(1, writes.get());
    }

    private record TenantValue(String value) {
    }

    private record CountingTenantCodec(AtomicInteger writes) implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == TenantValue.class;
        }

        @Override
        public Object write(Object value) {
            writes.incrementAndGet();
            return ((TenantValue) value).value();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return new TenantValue(value.toString());
        }
    }

    private static final class ConsumingExecutor implements SyncBatchExecutor {

        @Override
        public BatchWriteResult writeBatch(BatchWriteRequest request) {
            Flux.from(request.rows()).collectList().block();
            return BatchWriteResult.empty(request.options().mode());
        }

        @Override
        public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            throw new UnsupportedOperationException("chunk results are not used by this test");
        }
    }
}
