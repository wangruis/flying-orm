package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReactiveRepositoryPublisherDefaultsTest {

    private static final Duration WAIT = Duration.ofSeconds(10);

    @Test
    void publisherOverloadsRetainDefaultsLazyInputAndOptimisticUpdates() throws Exception {
        String database = "publisher_defaults_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database + ";DATABASE_TO_LOWER=TRUE");
        ConnectionFactory factory = ConnectionFactories.get(
                "r2dbc:h2:mem:///" + database + "?options=DATABASE_TO_LOWER=TRUE");
        try (Connection keepAlive = source.getConnection();
             Statement statement = keepAlive.createStatement();
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2")
                     .batchWriteOptions(BatchWriteOptions.of(1).withMemoryLimits(2, 2048)).build()) {
            statement.execute("create table publisher_items(id int primary key, name varchar(10000), version bigint)");
            ReactiveFormRepository<Item> repository = clients.repository(Item.class);
            AtomicInteger subscriptions = new AtomicInteger();
            Mono<BatchExecutionEvidence> pending = repository.insertBatch(
                    Flux.just(new Item(1, "first", 0L), new Item(2, "second", 0L))
                            .doOnSubscribe(subscription -> subscriptions.incrementAndGet()));
            assertEquals(0, subscriptions.get());
            assertEquals(0, clients.syncRepository(Item.class).createQuery().fetch().size());
            assertEquals(2, pending.block(WAIT).successfulCount());
            assertEquals(1, subscriptions.get());

            assertEquals(1, repository.upsertBatch(Flux.just(new Item(1, "upserted", 0L)))
                    .block(WAIT).successfulCount());
            assertEquals("upserted", clients.syncRepository(Item.class).createQuery()
                    .where(Item::getId, 1).one().getName());

            Item updated = new Item(1, "updated", 0L);
            assertEquals(1, repository.updateBatch(Flux.just(updated)).block(WAIT).successfulCount());
            Item stored = clients.syncRepository(Item.class).createQuery().where(Item::getId, 1).one();
            assertEquals("updated", stored.getName());
            assertEquals(1L, stored.getVersion());
            assertEquals(0L, updated.getVersion()); // 批量更新不回填实体版本，数据库版本已递增。

            // 单参数入口必须沿用已装配的内存预算，不能自行改用默认选项。
            BatchExecutionEvidenceException error = assertThrows(BatchExecutionEvidenceException.class,
                    () -> repository.insertBatch(Flux.just(new Item(3, "x".repeat(8192), 0L))).block(WAIT));
            BatchMemoryLimitExceededException limit = assertInstanceOf(BatchMemoryLimitExceededException.class,
                    error.getCause());
            assertEquals(2048, limit.limit());
            assertEquals(0, error.evidence().successfulCount());
            assertEquals(2, clients.syncRepository(Item.class).createQuery().fetch().size());
        }
    }

    @TableName("publisher_items")
    public static class Item {
        @TableId(type = IdType.INPUT)
        private Integer id;
        private String name;
        @Version
        private Long version;

        public Item() { }
        Item(Integer id, String name, Long version) {
            this.id = id;
            this.name = name;
            this.version = version;
        }
        public Integer getId() { return id; }
        public void setId(Integer id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public Long getVersion() { return version; }
        public void setVersion(Long version) { this.version = version; }
    }
}
