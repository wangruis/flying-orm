package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateHaving;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntitySchemaRepositoryContractTest {

    @TestFactory
    Stream<DynamicTest> typedRawQueriesApplyAssignableCustomCodecsExactlyOnce() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(template ->
                DynamicTest.dynamicTest("typed raw custom codec reactive=" + reactive + " template=" + template, () -> {
                    AtomicInteger reads = new AtomicInteger();
                    ValueCodec codec = new ValueCodec() {
                        @Override
                        public boolean supports(Class<?> targetType) {
                            return targetType == CharSequence.class;
                        }

                        @Override
                        public Object write(Object value) {
                            return value == null ? null : "db:" + value;
                        }

                        @Override
                        public Object read(Object value, Class<?> targetType) {
                            reads.incrementAndGet();
                            return value == null ? null : new StatusText(value.toString().substring(3));
                        }
                    };
                    EntitySchemaDescriptor<CodedText> descriptor = EntitySchemaDescriptor.builder(CodedText.class)
                            .typeMappings(EntityTypeMappingRegistry.builder()
                                    .register("coded-text", CharSequence.class, DatabaseType.of("VARCHAR"), codec)
                                    .build()).build();
                    String sql = "select text from coded_text";
                    CapturingExecutor executor = new CapturingExecutor(row("text", "db:payload"));
                    FlyingOrmClientBuilder builder = reactive
                            ? FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                            : FlyingOrmClients.builder(executor.dataSource()).configuredDialect("h2");
                    try (FlyingOrmClients clients = builder.entitySchema(descriptor)
                            .sqlTemplates(SqlTemplateRegistry.builder()
                                    .register(SqlTemplate.query("coded-text", sql, Set.of())).build()).build()) {
                        CodedText expected = new CodedText(new StatusText("payload"));
                        CodedText control = reactive
                                ? clients.repository(CodedText.class).select(ConditionGroup.and().build()).single().block()
                                : clients.syncRepository(CodedText.class).select(ConditionGroup.and().build()).getFirst();
                        assertEquals(expected, control);
                        assertEquals(1, reads.get(), "Repository must decode the source field exactly once");
                        reads.set(0);
                        CodedText selected = reactive
                                ? (template ? clients.operator().sqlTemplate("coded-text").query(CodedText.class)
                                        : clients.operator().unsafeNativeSql(sql).query(CodedText.class)).single().block()
                                : (template ? clients.syncOperator().sqlTemplate("coded-text").query(CodedText.class)
                                        : clients.syncOperator().unsafeNativeSql(sql).query(CodedText.class)).getFirst();
                        assertEquals(expected, selected);
                        assertEquals(1, reads.get(), "Raw typed queries must decode the target field exactly once");
                        reads.set(0);
                        CodedText one = reactive
                                ? (template ? clients.operator().sqlTemplate("coded-text").one(CodedText.class)
                                        : clients.operator().unsafeNativeSql(sql).one(CodedText.class)).block()
                                : (template ? clients.syncOperator().sqlTemplate("coded-text").one(CodedText.class)
                                        : clients.syncOperator().unsafeNativeSql(sql).one(CodedText.class));
                        assertEquals(expected, one);
                        assertEquals(1, reads.get(), "Raw typed one() must decode the target field exactly once");
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> customTextValuesReachBatchUpdateCodecsWithTheirLogicalTypes() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                "batch custom text reactive=" + reactive, () -> {
                    EntitySchemaDescriptor<VersionedText> descriptor = versionedTextDescriptor();
                    CapturingExecutor executor = new CapturingExecutor();
                    FlyingOrmClientBuilder builder = reactive
                            ? FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                            : FlyingOrmClients.builder(executor.dataSource()).configuredDialect("h2");
                    VersionedText first = versionedText(1L, "first");
                    VersionedText second = versionedText(2L, "second");
                    ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();
                    try (FlyingOrmClients clients = builder.entitySchema(descriptor).build()) {
                        if (reactive) {
                            ReactiveFormRepository<VersionedText> repository = clients.repository(VersionedText.class);
                            assertEquals(1L, repository.update(first, where).block());
                            assertEquals(List.of("db:first", "builder:first"),
                                    executor.singleWrite.parameters().subList(0, 2));
                            repository.insertBatch(List.of(first, second)).block();
                            assertInsertedTextRows(executor);
                            repository.updateBatch(List.of(first, second)).block();
                        } else {
                            SyncFormRepository<VersionedText> repository = clients.syncRepository(VersionedText.class);
                            assertEquals(1L, repository.update(first, where));
                            assertEquals(List.of("db:first", "builder:first"),
                                    executor.singleWrite.parameters().subList(0, 2));
                            repository.insertBatch(List.of(first, second));
                            assertInsertedTextRows(executor);
                            repository.updateBatch(List.of(first, second));
                        }
                        assertEquals(List.of("db:first", "builder:first", 1L, 1L),
                                Arrays.asList(executor.batchRows.getFirst()));
                        assertEquals(List.of("db:second", "builder:second", 2L, 1L),
                                Arrays.asList(executor.batchRows.get(1)));
                    }
                }));
    }

    @Test
    void publicWriteSpecRetainsCustomTextUntilFieldEncoding() {
        EntitySchemaDescriptor<VersionedText> descriptor = versionedTextDescriptor();
        CapturingExecutor executor = new CapturingExecutor();
        try (FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                .entitySchema(descriptor).build()) {
            assertEquals(1L, clients.forms().insert(WriteSpec.insert(descriptor.form(),
                    Map.of("text", new StatusText("direct")))).block());
            assertEquals(List.of("db:direct"), executor.singleWrite.parameters());
        }
    }

    @Test
    void standardWhereRetainsCustomTextUntilFieldEncoding() {
        EntitySchemaDescriptor<VersionedText> descriptor = versionedTextDescriptor();
        CapturingExecutor executor = new CapturingExecutor();
        try (FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                .entitySchema(descriptor).build()) {
            clients.forms().select(QuerySpec.of(descriptor.form(), ConditionGroup.and()
                    .where("text", "=", new StatusText("filter")).build())).collectList().block();
            assertEquals(List.of("db:filter"), executor.lastQuery.parameters());
        }
    }

    @Test
    void aggregateGroupRetainsTheDecodedCustomTextType() {
        EntitySchemaDescriptor<VersionedText> descriptor = versionedTextDescriptor();
        CapturingExecutor executor = new CapturingExecutor(row("text_group", "db:group", "total", 2L));
        GroupSelection group = GroupSelection.of("text", "text_group");
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(descriptor.form(), ConditionGroup.and().build()))
                .group(group).aggregate(AggregateExpression.count("id", "total")).build();
        try (FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                .entitySchema(descriptor).build()) {
            AggregateRow selected = clients.forms().aggregate(spec).single().block();
            assertEquals(new StatusText("group"), selected.get(group, StatusText.class));
        }
    }

    @TestFactory
    Stream<DynamicTest> aggregateHavingUsesTheCustomCodecOfItsOffsetTimeSource() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(governed ->
                DynamicTest.dynamicTest("offset time having reactive=" + reactive + ", governed=" + governed, () -> {
                    EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
                            .register("clock-code", ClockValue.class, DatabaseType.of("OFFSET_TIME"),
                                    new ClockCodec()).build();
                    EntitySchemaDescriptor<ClockEvent> descriptor = EntitySchemaDescriptor.builder(ClockEvent.class)
                            .typeMappings(mappings).build();
                    ClockValue value = new ClockValue(OffsetTime.parse("10:30:00+08:00"));
                    GroupSelection group = GroupSelection.of("clock", "clock_group");
                    AggregateExpression<Long> count = AggregateExpression.count("clock", "total");
                    AggregateExpression<ClockValue> earliest = AggregateExpression.min(
                            "clock", "earliest", LogicalType.OFFSET_TIME, ClockValue.class);
                    AggregateExpression<ClockValue> latest = AggregateExpression.max(
                            "clock", "latest", LogicalType.OFFSET_TIME, ClockValue.class);
                    QuerySpec query = QuerySpec.of(descriptor.form(), ConditionGroup.and()
                            .where("clock", "=", value).build());
                    AggregateSpec control = AggregateSpec.builder(query).group(group).aggregate(count)
                            .aggregate(earliest).aggregate(latest).build();
                    AggregateSpec having = AggregateSpec.builder(query).group(group).aggregate(count)
                            .aggregate(earliest).aggregate(latest)
                            .having(AggregateHaving.of(ConditionGroup.and()
                                    .where("clock_group", "=", value)
                                    .where("earliest", ">=", value).where("latest", "<=", value)
                                    .where("total", ">=", 1L).build())).build();
                    FieldUsePolicy policy = governed ? FieldUsePolicy.builder()
                            .allow("clock", FieldUse.FILTER, FieldUse.GROUP, FieldUse.PROJECT, FieldUse.HAVING,
                                    FieldUse.AGGREGATE)
                            .visibility("clock", FieldVisibility.FULL).build() : FieldUsePolicy.unrestricted();
                    CapturingExecutor executor = new CapturingExecutor(row(
                            "clock_group", value.value(), "total", 2L,
                            "earliest", value.value(), "latest", value.value()));
                    FlyingOrmClientBuilder builder = reactive
                            ? FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                            : FlyingOrmClients.builder(executor.dataSource()).configuredDialect("h2");
                    try (FlyingOrmClients clients = builder.entitySchema(descriptor).build()) {
                        AggregateRow selected = reactive
                                ? clients.forms().withFieldUsePolicy(policy).aggregate(control).single().block()
                                : clients.syncForms().withFieldUsePolicy(policy).aggregate(control).getFirst();
                        assertEquals(value, selected.get(group, ClockValue.class));
                        assertEquals(2L, selected.get(count));
                        assertEquals(value, selected.get(earliest));
                        assertEquals(value, selected.get(latest));
                        assertEquals(List.of(value.value()), executor.lastQuery.parameters());
                        selected = reactive
                                ? clients.forms().withFieldUsePolicy(policy).aggregate(having).single().block()
                                : clients.syncForms().withFieldUsePolicy(policy).aggregate(having).getFirst();
                        assertEquals(value, selected.get(group, ClockValue.class));
                        assertEquals(2L, selected.get(count));
                        assertEquals(value, selected.get(earliest));
                        assertEquals(value, selected.get(latest));
                        assertEquals(List.of(value.value(), value.value(), value.value(), value.value(), 1L),
                                executor.lastQuery.parameters());
                    }
                })));
    }

    private static VersionedText versionedText(long id, String value) {
        return new VersionedText(id, 1L, new StatusText(value), new StringBuilder(value));
    }

    private static void assertInsertedTextRows(CapturingExecutor executor) {
        assertEquals(List.of(1L, 1L, "db:first", "builder:first"),
                Arrays.asList(executor.batchRows.getFirst()));
        assertEquals(List.of(2L, 1L, "db:second", "builder:second"),
                Arrays.asList(executor.batchRows.get(1)));
    }

    private static EntitySchemaDescriptor<VersionedText> versionedTextDescriptor() {
        return EntitySchemaDescriptor.builder(VersionedText.class).typeMappings(EntityTypeMappingRegistry.builder()
                .register("status-text", StatusText.class, DatabaseType.of("VARCHAR"), new StatusTextCodec())
                .register("builder-text", StringBuilder.class, DatabaseType.of("VARCHAR"), new BuilderTextCodec())
                .build()).build();
    }

    @Test
    void registeredDescriptorDrivesRepositoryWritesBatchAndEntityDecoding() {
        EntitySchemaDescriptor<Account> descriptor = accountDescriptor();
        CapturingExecutor executor = new CapturingExecutor(row(
                "id", 3L,
                "account_number", "db:A-3"));

        try (FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                .entitySchema(descriptor)
                .build()) {
            ReactiveFormRepository<Account> repository = clients.repository(Account.class);

            assertSame(descriptor, clients.forms().entityModels().schemaDescriptor(Account.class));
            assertSame(descriptor.metadata(), clients.forms().entityModels().metadata(Account.class));

            assertEquals(1L, repository.insert(new Account(1L, new AccountPayload("A-1"))).block());
            repository.insertBatch(List.of(new Account(2L, new AccountPayload("A-2")))).block();
            Account selected = repository.select(ConditionGroup.and().build()).single().block();

            assertEquals(List.of(1L, "db:A-1"), executor.singleWrite.parameters());
            assertEquals(List.of(2L, "db:A-2"), Arrays.asList(executor.batchRows.getFirst()));
            assertEquals(new Account(3L, new AccountPayload("A-3")), selected);
        }
    }

    @Test
    void registeredEnumCodecReceivesTheDomainValueBeforeLegacyStorage() {
        EntityTypeMappingRegistry typeMappings = EntityTypeMappingRegistry.builder()
                .register("status-code", Status.class, DatabaseType.of("VARCHAR(8)"), new StatusCodec())
                .build();
        EntitySchemaDescriptor<StatusAccount> descriptor = EntitySchemaDescriptor.builder(StatusAccount.class)
                .typeMappings(typeMappings)
                .build();
        CapturingExecutor executor = new CapturingExecutor(row("status", "A"));

        try (FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                .entitySchema(descriptor)
                .build()) {
            ReactiveFormRepository<StatusAccount> repository = clients.repository(StatusAccount.class);
            assertEquals(new StatusAccount(Status.ACTIVE),
                         repository.select(ConditionGroup.and().build()).single().block());
            assertEquals(1L, clients.forms().insert(WriteSpec.insert(
                    descriptor.form(), Map.of("status", Status.ACTIVE))).block());
            assertEquals(List.of("A"), executor.singleWrite.parameters());

            assertEquals(1L, repository.insert(new StatusAccount(Status.ACTIVE)).block());
            assertEquals(List.of("A"), executor.singleWrite.parameters());
            repository.insertBatch(List.of(new StatusAccount(Status.ACTIVE))).block();
            assertEquals(List.of("A"), Arrays.asList(executor.batchRows.getFirst()));
        }
    }

    @Test
    void beanCustomCodecsRetainDomainTextAndExtractDeclaredEnumMembers() {
        EntityTypeMappingRegistry typeMappings = EntityTypeMappingRegistry.builder()
                .register("status-code", Status.class, DatabaseType.of("VARCHAR(8)"), new StatusCodec())
                .register("status-text", StatusText.class, DatabaseType.of("VARCHAR"), new StatusTextCodec())
                .build();
        EntitySchemaDescriptor<StatusBean> descriptor = EntitySchemaDescriptor.builder(StatusBean.class)
                .typeMappings(typeMappings)
                .build();
        CapturingExecutor executor = new CapturingExecutor(row(
                "status", "A", "text", "db:label", "member", "db:member"));

        try (FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                .entitySchema(descriptor)
                .build()) {
            ReactiveFormRepository<StatusBean> repository = clients.repository(StatusBean.class);
            StatusBean bean = new StatusBean();
            bean.status = Status.ACTIVE;
            bean.text = new StatusText("label");
            bean.member = MemberStatus.ACTIVE;

            assertEquals(1L, repository.insert(bean).block());
            assertEquals(List.of("A", "db:label", "db:member"), executor.singleWrite.parameters());
            StatusBean selected = repository.select(ConditionGroup.and().build()).single().block();
            assertEquals(bean.status, selected.status);
            assertEquals(bean.text, selected.text);
            assertEquals(bean.member, selected.member);

            assertEquals(1L, clients.repository(LegacyStatus.class)
                    .insert(new LegacyStatus(Status.ACTIVE, new StringBuilder("plain"))).block());
            assertEquals(List.of("ACTIVE", "plain"), executor.singleWrite.parameters());
        }
    }

    @Test
    void unregisteredEntityKeepsTheLegacyRepositoryMapping() {
        CapturingExecutor executor = new CapturingExecutor();

        try (FlyingOrmClients clients = FlyingOrmClientBuilder.reactive(executor, RdbDialect.h2())
                .entitySchema(accountDescriptor())
                .build()) {
            assertEquals(1L, clients.repository(LegacyNote.class)
                    .insert(new LegacyNote(7L, "plain"))
                    .block());

            assertEquals(List.of(7L, "plain"), executor.singleWrite.parameters());
        }
    }

    @Test
    void descriptorRegistrationRejectsAmbiguousStartupConfiguration() {
        EntitySchemaDescriptor<Account> descriptor = accountDescriptor();
        FlyingOrmClientBuilder builder = FlyingOrmClientBuilder.reactive(
                new CapturingExecutor(), RdbDialect.h2());

        // 同一个启动配置对象重复交给框架是幂等的，方便上层模块汇总配置时去重。
        builder.entitySchema(descriptor).entitySchema(descriptor);

        assertThrows(IllegalArgumentException.class,
                     () -> builder.entitySchema(accountDescriptor()));
        assertThrows(IllegalArgumentException.class,
                     () -> builder.entitySchema(EntitySchemaDescriptor.builder(LegacyNote.class).build()));
    }

    private static EntitySchemaDescriptor<Account> accountDescriptor() {
        EntityTypeMappingRegistry typeMappings = EntityTypeMappingRegistry.builder()
                .register("account-payload", AccountPayload.class, DatabaseType.of("JSON"),
                          new AccountPayloadCodec())
                .build();
        return EntitySchemaDescriptor.builder(Account.class)
                .typeMappings(typeMappings)
                .build();
    }

    private static DynamicRow row(Object... entries) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            values.put((String) entries[index], entries[index + 1]);
        }
        return DynamicRow.copyOf(values);
    }

    @TableName("accounts")
    private record Account(
            @TableId(type = IdType.INPUT) Long id,
            @TableColumn(databaseTypeId = "account-payload") AccountPayload accountNumber) {
    }

    private static final class AccountPayload extends LinkedHashMap<String, Object> {

        private AccountPayload(String number) {
            put("number", number);
        }

        private String number() {
            return (String) get("number");
        }
    }

    @TableName("legacy_notes")
    private record LegacyNote(@TableId(type = IdType.INPUT) Long id, String text) {
    }

    @TableName("coded_text")
    private record CodedText(@TableColumn(databaseTypeId = "coded-text") CharSequence text) {
    }

    @TableName("versioned_text")
    private record VersionedText(@TableId(type = IdType.INPUT) Long id, @Version Long version,
                                 @TableColumn(databaseTypeId = "status-text") StatusText text,
                                 @TableColumn(databaseTypeId = "builder-text") StringBuilder builder) {
    }

    @TableName("clock_events")
    private record ClockEvent(@TableId(type = IdType.INPUT) Long id,
                              @TableColumn(databaseTypeId = "clock-code") ClockValue clock) {
    }

    private record ClockValue(OffsetTime value) implements Comparable<ClockValue> {
        @Override
        public int compareTo(ClockValue other) {
            return value.compareTo(other.value);
        }
    }

    private static final class ClockCodec implements ValueCodec {
        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == ClockValue.class;
        }

        @Override
        public Object write(Object value) {
            return value == null ? null : ((ClockValue) value).value();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value == null ? null : new ClockValue((OffsetTime) value);
        }
    }

    private static final class BuilderTextCodec implements ValueCodec {
        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == StringBuilder.class;
        }

        @Override
        public Object write(Object value) {
            return value == null ? null : "builder:" + ((StringBuilder) value).toString();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value == null ? null : new StringBuilder(value.toString().substring(8));
        }
    }

    @TableName("status_accounts")
    private record StatusAccount(@TableColumn(databaseTypeId = "status-code") Status status) {
    }

    private enum Status { ACTIVE }

    @TableName("status_beans")
    private static final class StatusBean {
        @TableColumn(databaseTypeId = "status-code")
        private Status status;
        @TableColumn(databaseTypeId = "status-text")
        private StatusText text;
        @TableColumn(databaseTypeId = "status-text")
        private MemberStatus member;
    }

    @TableName("legacy_status")
    private record LegacyStatus(Status status, StringBuilder text) {
    }

    private enum MemberStatus {
        ACTIVE(new StatusText("member"));

        @EnumValue
        private final StatusText code;

        MemberStatus(StatusText code) {
            this.code = code;
        }
    }

    private record StatusText(String value) implements CharSequence {
        @Override
        public int length() {
            return value.length();
        }

        @Override
        public char charAt(int index) {
            return value.charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return value.subSequence(start, end);
        }

        @Override
        public String toString() {
            return value;
        }
    }

    private static final class StatusTextCodec implements ValueCodec {
        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == StatusText.class;
        }

        @Override
        public Object write(Object value) {
            return value == null ? null : "db:" + ((StatusText) value).value();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value == null ? null : new StatusText(value.toString().substring(3));
        }
    }

    private static final class StatusCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == Status.class;
        }

        @Override
        public Object write(Object value) {
            return value == null ? null : switch ((Status) value) {
                case ACTIVE -> "A";
            };
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            if (value == null) {
                return null;
            }
            if ("A".equals(value)) {
                return Status.ACTIVE;
            }
            throw new IllegalArgumentException("unknown stored status");
        }
    }

    /** 测试 codec 故意加上可见前缀，让写入与读取是否真正经过同一映射一眼可辨。 */
    private static final class AccountPayloadCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == AccountPayload.class;
        }

        @Override
        public Object write(Object value) {
            return value == null ? null : "db:" + ((AccountPayload) value).number();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            if (value == null) {
                return null;
            }
            String stored = value.toString();
            return new AccountPayload(stored.startsWith("db:") ? stored.substring(3) : stored);
        }
    }

    /**
     * 这个替身只保留契约测试需要的三项证据：单条参数、批量参数行和数据库原始查询行。
     * 它不模拟连接或事务，避免测试把执行器实现细节带进 Repository 契约。
     */
    private static final class CapturingExecutor implements ReactiveSqlExecutor {

        private final List<DynamicRow> queryRows;
        private SqlRequest lastQuery;
        private SqlRequest singleWrite;
        private List<Object[]> batchRows = List.of();

        private CapturingExecutor(DynamicRow... queryRows) {
            this.queryRows = List.of(queryRows);
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            lastQuery = request;
            return Flux.fromIterable(queryRows);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            singleWrite = request;
            return Mono.just(1L);
        }

        @Override
        public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
            return Flux.from(request.rows()).collectList().map(rows -> {
                batchRows = List.copyOf(rows);
                return new BatchWriteResult(
                        request.options().mode(),
                        BatchWriteResult.Status.COMMITTED,
                        rows.size(),
                        rows.size(),
                        List.of());
            });
        }

        private DataSource dataSource() {
            return proxy(DataSource.class, (instance, method, args) -> method.getName().equals("getConnection")
                    ? proxy(Connection.class, (connection, operation, parameters) -> switch (operation.getName()) {
                        case "prepareStatement" -> statement((String) parameters[0]);
                        case "getAutoCommit" -> true;
                        default -> defaultValue(operation.getReturnType());
                    }) : defaultValue(method.getReturnType()));
        }

        private PreparedStatement statement(String sql) {
            Map<Integer, Object> parameters = new java.util.TreeMap<>();
            List<Object[]> rows = new ArrayList<>();
            return proxy(PreparedStatement.class, (instance, method, args) -> switch (method.getName()) {
                case "setObject" -> { parameters.put((Integer) args[0], args[1]); yield null; }
                case "executeLargeUpdate" -> {
                    singleWrite = new SqlRequest(sql, List.copyOf(parameters.values()));
                    yield 1L;
                }
                case "addBatch" -> { rows.add(parameters.values().toArray()); yield null; }
                case "executeBatch" -> {
                    batchRows = List.copyOf(rows);
                    int[] counts = new int[rows.size()];
                    Arrays.fill(counts, 1);
                    yield counts;
                }
                case "executeQuery" -> {
                    lastQuery = new SqlRequest(sql, List.copyOf(parameters.values()));
                    yield resultSet();
                }
                default -> defaultValue(method.getReturnType());
            });
        }

        private ResultSet resultSet() {
            DynamicRow layout = queryRows.getFirst();
            ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (instance, method, args) ->
                    switch (method.getName()) {
                        case "getColumnCount" -> layout.columnCount();
                        case "getColumnLabel", "getColumnName" -> layout.columnName((int) args[0] - 1);
                        default -> defaultValue(method.getReturnType());
                    });
            AtomicInteger index = new AtomicInteger(-1);
            return proxy(ResultSet.class, (instance, method, args) -> switch (method.getName()) {
                case "getMetaData" -> metadata;
                case "next" -> index.incrementAndGet() < queryRows.size();
                case "getObject" -> queryRows.get(index.get()).value((int) args[0] - 1);
                default -> defaultValue(method.getReturnType());
            });
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }

    private static Object defaultValue(Class<?> type) {
        return type.isPrimitive() && type != void.class ? Array.get(Array.newInstance(type, 1), 0) : null;
    }
}
