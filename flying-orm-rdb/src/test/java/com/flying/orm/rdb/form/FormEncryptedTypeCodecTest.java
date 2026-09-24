package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.MaskedField;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedFieldReprotection;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormEncryptedTypeCodecTest {

    @TestFactory
    Stream<DynamicTest> mixedCaseEncryptedColumnKeepsTheExplicitCodec() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("reactive=" + reactive, () -> {
            try (Fixture fixture = new Fixture(false)) {
                EntitySchemaDescriptor<MixedCaseRow> descriptor = EntitySchemaDescriptor.builder(MixedCaseRow.class)
                        .typeMappings(fixture.mappings).build();
                DynamicForm form = descriptor.form();
                FormDataSqlRenderer renderer = fixture.renderer.withEntityFieldCodecs(descriptor.customFieldCodecs());
                FormPreparedWrite write = renderer.protection().prepareWrite(
                        form, Map.of("SecretText", "payload"), DataScope.none());
                DynamicRow raw = DynamicRow.copyOf(Map.of("SecretText", write.values().get("SecretText")));
                FormResultDecoder decoder = new FormResultDecoder(renderer, fixture.models);
                DynamicRow decoded = reactive ? decoder.decodeRows(form, Flux.just(raw), SqlExecutionOptions.safeDefaults(),
                        DataScope.none(), SensitiveDisplayMode.FULL).single().block()
                        : decoder.decodeRows(form, List.of(raw), SqlExecutionOptions.safeDefaults(),
                                DataScope.none(), SensitiveDisplayMode.FULL).getFirst();
                assertEquals("payload", decoded.get("SecretText"));
            }
        }));
    }

    @TableName("mixed_case")
    private record MixedCaseRow(@com.flying.orm.core.annotation.TableField("SecretText")
            @TableColumn(databaseTypeId = "coded-text") @EncryptedField CharSequence text) { }

    @TestFactory
    Stream<DynamicTest> joinsHonorTheBudgetAfterEncryptedFieldDecoding() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(page ->
                DynamicTest.dynamicTest("reactive=" + reactive + ",page=" + page, () -> {
                    try (Fixture fixture = new Fixture(false, 600)) {
                        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(fixture.form);
                        JoinQuerySpec spec = builder.selectAs(builder.root(), "text", "selected_text")
                                .orderBy(builder.root(), "id", com.flying.orm.core.page.PageSort.Direction.ASC)
                                .showSensitive().build();
                        DynamicRow raw = DynamicRow.copyOf(Map.of("selected_text", fixture.encryptedRow().get("text")));
                        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults().withMaxResultBytes(1024);
                        java.util.function.Function<SqlRequest, List<DynamicRow>> rows = request ->
                                request.sql().toLowerCase(java.util.Locale.ROOT).contains("count(")
                                        ? List.of(DynamicRow.copyOf(Map.of("total", 1L))) : List.of(raw);
                        if (reactive) {
                            ReactiveFormClient forms = ReactiveFormClient.create(new ReactiveSqlExecutor() {
                                public Flux<DynamicRow> query(SqlRequest request) { return Flux.fromIterable(rows.apply(request)); }
                                public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new AssertionError()); }
                            }, fixture.renderer);
                            Mono<?> result = page ? forms.pageJoin(spec, PageQuery.of(1, 10), options)
                                    : forms.selectJoin(spec, options).collectList();
                            assertThrows(SqlResultMemoryLimitExceededException.class, result::block);
                            assertThrows(SqlResultMemoryLimitExceededException.class, result::block);
                        } else {
                            SyncFormClient forms = SyncFormClient.create(new SyncSqlExecutor() {
                                public List<DynamicRow> query(SqlRequest request) { return rows.apply(request); }
                                public long rowsUpdated(SqlRequest request) { throw new AssertionError(); }
                                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions settings) {
                                    throw new AssertionError();
                                }
                            }, (request, completion) -> { throw new AssertionError(); }, fixture.renderer);
                            assertThrows(SqlResultMemoryLimitExceededException.class, () -> {
                                if (page) forms.pageJoin(spec, PageQuery.of(1, 10), options);
                                else forms.selectJoin(spec, options);
                            });
                        }
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> exactSearchAndKeyRotationKeepTheFieldCodecRepresentation() {
        return Stream.of(false, true).map(broad -> DynamicTest.dynamicTest("broad=" + broad, () -> {
            try (Fixture fixture = new Fixture(broad)) {
                FormPreparedWrite original = fixture.renderer.protection().prepareWrite(
                        fixture.form, Map.of("id", 1L, "text", "payload"), DataScope.none());
                ConditionGroup exact = ConditionGroup.and().add(ProtectedConditions.exact("text", "payload")).build();
                ProtectedFieldRuntime.PreparedQuery query = fixture.renderer.protection().prepareQuery(
                        fixture.form, exact, DataScope.none(), List.of("text"));
                TermCondition token = (TermCondition) query.where().children().getFirst();
                byte[] writtenToken = (byte[]) original.values().get(token.field());
                assertTrue(((List<?>) token.value()).stream().anyMatch(value -> Arrays.equals(writtenToken, (byte[]) value)));

                byte[] nextKey = new byte[32];
                Arrays.fill(nextKey, (byte) 2);
                try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.builder()
                        .current("v2", nextKey).readable("v1", new byte[32]).build();
                     ProtectedFieldRuntime current = ProtectedFieldRuntime.create(keys)) {
                    Map<String, Object> rewrite = ProtectedFieldReprotection.create(keys).valuesNeedingReprotection(
                            fixture.form, original.values(), DataScope.none(), fixture.mappings.valueCodecs());
                    WriteSpec spec = WriteSpec.update(fixture.form, rewrite,
                            ConditionGroup.and().where("id", "=", 1L).build());
                    FormDataSqlRenderer renderer = fixture.renderer.withProtectedFields(current);
                    FormPreparedWrite rotated = renderer.protection().prepareWrite(
                            fixture.form, spec.ownedValues(), DataScope.none());
                    DynamicRow decoded = new FormResultDecoder(renderer, fixture.models).decodeRows(
                            fixture.form, List.of(DynamicRow.copyOf(Map.of("text", rotated.values().get("text")))),
                            SqlExecutionOptions.safeDefaults(), DataScope.none(), SensitiveDisplayMode.FULL).getFirst();
                    assertEquals("payload", decoded.get("text"));
                    assertTrue(ProtectedFieldReprotection.create(keys).valuesNeedingReprotection(
                            fixture.form, rotated.values(), DataScope.none(), fixture.mappings.valueCodecs()).isEmpty());
                }
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> containsHonorsTheExplicitBudgetAfterCustomDecoding() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of("select", "page", "cursor").map(shape ->
                DynamicTest.dynamicTest(reactive + "/" + shape, () -> {
                    try (Fixture fixture = new Fixture(false, 600)) {
                        DynamicRow raw = fixture.encryptedRow();
                        QuerySpec spec = QuerySpec.of(fixture.form, ConditionGroup.and()
                                .add(ProtectedConditions.contains("text", "payload")).build()).showSensitive()
                                .withExecutionOptions(SqlExecutionOptions.safeDefaults().withMaxResultBytes(1024));
                        assertThrows(SqlResultMemoryLimitExceededException.class, () -> {
                            if (reactive) {
                                ReactiveFormClient forms = ReactiveFormClient.create(new ReactiveSqlExecutor() {
                                    public Flux<DynamicRow> query(SqlRequest request) { return Flux.just(raw); }
                                    public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new AssertionError()); }
                                }, fixture.renderer);
                                switch (shape) {
                                    case "select" -> forms.select(spec).collectList().block();
                                    case "page" -> forms.page(spec, PageQuery.of(1, 10)).block();
                                    case "cursor" -> forms.cursorPage(spec, CursorPageQuery.first(10, CursorSort.asc("id"))).block();
                                    default -> throw new AssertionError(shape);
                                }
                            } else {
                                SyncFormClient forms = SyncFormClient.create(new SyncSqlExecutor() {
                                    public List<DynamicRow> query(SqlRequest request) { return List.of(raw); }
                                    public long rowsUpdated(SqlRequest request) { throw new AssertionError(); }
                                    public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                                        throw new AssertionError();
                                    }
                                }, (request, completed) -> { throw new AssertionError(); }, fixture.renderer);
                                switch (shape) {
                                    case "select" -> forms.select(spec);
                                    case "page" -> forms.page(spec, PageQuery.of(1, 10));
                                    case "cursor" -> forms.cursorPage(spec, CursorPageQuery.first(10, CursorSort.asc("id")));
                                    default -> throw new AssertionError(shape);
                                }
                            }
                        });
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> encryptedEnumQueriesUseTheSameEnumValueAsWrites() {
        return Stream.of(ProtectedConditions.EXACT, ProtectedConditions.SUFFIX, ProtectedConditions.CONTAINS)
                .map(operator -> DynamicTest.dynamicTest(operator, () -> {
                    EntityValues<EnumRow> values = EntityValues.createUncached(EnumRow.class);
                    FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                            SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
                    Object normalized = values.normalizeConditionValue("state", operator, State.ACTIVE,
                            renderer.conditionRenderer().terms());
                    assertEquals(values.read(new EnumRow(State.ACTIVE)).get("state"), normalized);
                    assertEquals("active-code", normalized);
                }));
    }

    private record EnumRow(@EncryptedField State state) { }

    private enum State {
        ACTIVE("active-code");
        @EnumValue
        private final String value;
        State(String value) { this.value = value; }
    }

    @TestFactory
    Stream<DynamicTest> encryptedFieldsUseTheirExplicitCodecBeforeMasking() {
        return Stream.of(false, true).flatMap(broad -> Stream.of(false, true)
                .flatMap(reactive -> Stream.of(SensitiveDisplayMode.values()).map(display ->
                        DynamicTest.dynamicTest(broad + "/" + reactive + "/" + display, () -> {
                            try (Fixture fixture = new Fixture(broad)) {
                                DynamicRow raw = fixture.encryptedRow();
                                assertEquals("db:payload", fixture.plaintext(raw).get("text"));
                                DynamicRow decoded = reactive
                                        ? fixture.decoder.decodeRows(fixture.form, Flux.just(raw),
                                                SqlExecutionOptions.safeDefaults(), DataScope.none(), display)
                                                .single().block()
                                        : fixture.decoder.decodeRows(fixture.form, List.of(raw),
                                                SqlExecutionOptions.safeDefaults(), DataScope.none(), display).getFirst();
                                assertEquals(display == SensitiveDisplayMode.FULL ? "payload" : "*******",
                                        decoded.get("text"));
                                assertEquals(1, fixture.reads.get());
                            }
                        }))));
    }

    @TestFactory
    Stream<DynamicTest> containsVerifiesEncodedTextBeforeDecodingAndMasking() {
        return Stream.of(false, true).flatMap(broad -> Stream.of(SensitiveDisplayMode.values()).map(display ->
                DynamicTest.dynamicTest(broad + "/" + display, () -> {
                    try (Fixture fixture = new Fixture(broad)) {
                        ConditionGroup where = ConditionGroup.and().add(
                                ProtectedConditions.contains("text", "payload")).build();
                        ProtectedFieldRuntime.PreparedContainsQuery query = fixture.renderer.protection()
                                .prepareContainsQuery(fixture.form, fixture.form, where, DataScope.none()).orElseThrow();
                        assertEquals("db:payload", query.normalizedValue());
                        List<DynamicRow> result = new ProtectedContainsResultSupport(fixture.renderer).finish(
                                fixture.form, query, List.of(fixture.plaintext(fixture.encryptedRow())),
                                List.of("id", "text"), display, SqlExecutionOptions.safeDefaults());
                        assertEquals(1, result.size());
                        assertEquals(display == SensitiveDisplayMode.FULL ? "payload" : "*******",
                                result.getFirst().get("text"));
                        assertEquals(1, fixture.reads.get());
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> joinAliasesRetainTheSourceEncryptedCodec() {
        return Stream.of(false, true).map(broad -> DynamicTest.dynamicTest("broad=" + broad, () -> {
            try (Fixture fixture = new Fixture(broad)) {
                JoinQuerySpec.Builder builder = JoinQuerySpec.builder(fixture.form);
                JoinQuerySpec query = builder.selectAs(builder.root(), "text", "selected_text").showSensitive().build();
                DynamicRow result = new JoinResultProtector(fixture.renderer)
                        .plan(query, Map.of(builder.root(), DataScope.none()), SensitiveDisplayMode.FULL)
                        .transform(DynamicRow.copyOf(Map.of("selected_text", fixture.encryptedRow().get("text"))));
                assertEquals("payload", result.get("selected_text"));
                assertEquals(1, fixture.reads.get());
            }
        }));
    }

    @TableName("encrypted_codec")
    private record CodedRow(@TableId(type = IdType.INPUT) Long id,
                            @TableColumn(databaseTypeId = "coded-text")
                            @EncryptedField(search = {EncryptedSearchMode.EXACT, EncryptedSearchMode.CONTAINS})
                            @MaskedField(policy = "full") CharSequence text) {
    }

    private static final class Fixture implements AutoCloseable {
        private final AtomicInteger reads = new AtomicInteger();
        private final ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]));
        private final EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults());
        private final EntityTypeMappingRegistry mappings;
        private final DynamicForm form;
        private final FormDataSqlRenderer renderer;
        private final FormResultDecoder decoder;

        private Fixture(boolean broad) {
            this(broad, 1);
        }

        private Fixture(boolean broad, int expansion) {
            mappings = EntityTypeMappingRegistry.builder().register(
                    "coded-text", CharSequence.class, DatabaseType.of("VARCHAR"), new ValueCodec() {
                        public boolean supports(Class<?> type) {
                            return broad ? CharSequence.class.isAssignableFrom(type) : type == CharSequence.class;
                        }
                        public Object write(Object value) { return value == null ? null : "db:" + value; }
                        public Object read(Object value, Class<?> type) {
                            reads.incrementAndGet();
                            return value == null ? null : value.toString().substring(3).repeat(expansion);
                        }
                    }).build();
            EntitySchemaDescriptor<CodedRow> descriptor = EntitySchemaDescriptor.builder(CodedRow.class)
                    .typeMappings(mappings).build();
            form = descriptor.form();
            renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build()
                    .withValueCodecs(mappings.valueCodecs()), RdbDialect.postgresql())
                    .withEntityFieldCodecs(descriptor.customFieldCodecs()).withProtectedFields(runtime);
            decoder = new FormResultDecoder(renderer, models);
        }

        private DynamicRow encryptedRow() {
            FormPreparedWrite write = renderer.protection().prepareWrite(
                    form, Map.of("id", 1L, "text", "payload"), DataScope.none());
            return DynamicRow.copyOf(Map.of("id", 1L, "text", write.values().get("text")));
        }

        private DynamicRow plaintext(DynamicRow row) {
            return runtime.transformResult(form, row, DataScope.none(), SensitiveDisplayMode.FULL, mappings.valueCodecs());
        }

        @Override
        public void close() {
            models.close();
            runtime.close();
        }
    }
}
