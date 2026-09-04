package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntitySchemaRepositoryContractTest {

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
        private SqlRequest singleWrite;
        private List<Object[]> batchRows = List.of();

        private CapturingExecutor(DynamicRow... queryRows) {
            this.queryRows = List.of(queryRows);
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
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
    }
}
