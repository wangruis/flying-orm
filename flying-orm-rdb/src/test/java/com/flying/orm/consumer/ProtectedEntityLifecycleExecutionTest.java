package com.flying.orm.consumer;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.MaskedField;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.lock.OptimisticLockConflictException;
import com.flying.orm.rdb.mapping.FlyingTenant;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Public entity APIs must compose protection, tenant isolation and version checks on real SQL. */
class ProtectedEntityLifecycleExecutionTest {

    @TestFactory
    Stream<DynamicTest> protectedEntityLifecycle() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                reactive ? "R2DBC protected entity lifecycle" : "JDBC protected entity lifecycle",
                () -> verify(reactive)));
    }

    private static void verify(boolean reactive) throws Exception {
        String database = "protected_entity_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database);
        source.setUser("sa");
        source.setPassword("");
        ConnectionFactory factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "h2").option(ConnectionFactoryOptions.PROTOCOL, "mem")
                .option(ConnectionFactoryOptions.DATABASE, database).option(ConnectionFactoryOptions.USER, "sa")
                .option(ConnectionFactoryOptions.PASSWORD, "").build());
        try (Connection keeper = source.getConnection(); Statement statement = keeper.createStatement();
             FlyingOrmClients base = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2")
                     .protectedFields(ProtectedFieldKeyRing.single("v1", new byte[32])).build();
             FlyingOrmClients tenant7 = base.withDefaultDataScope(DataScope.tenant("tenant_id", 7L));
             FlyingOrmClients tenant9 = base.withDefaultDataScope(DataScope.tenant("tenant_id", 9L))) {
            DynamicForm form = base.syncForms().entityModels().metadata(Contact.class).toDynamicForm();
            if (reactive) base.schema().createTable(form).block();
            else base.syncSchema().createTable(form);
            SyncFormRepository<Contact> sync = tenant7.syncRepository(Contact.class);
            ReactiveFormRepository<Contact> async = tenant7.repository(Contact.class);
            Contact first = new Contact(1L, 7L, 1L, "13800001234");
            Contact foreign = new Contact(2L, 9L, 1L, "13800001234");
            assertEquals(1L, reactive ? async.insert(first).block() : sync.insert(first));
            assertEquals(1L, reactive ? tenant9.repository(Contact.class).insert(foreign).block()
                    : tenant9.syncRepository(Contact.class).insert(foreign));
            byte[] originalCiphertext = stored(statement, 1L, 7L, 1L);
            byte[] foreignCiphertext = stored(statement, 2L, 9L, 1L);
            assertFalse(Arrays.equals("13800001234".getBytes(java.nio.charset.StandardCharsets.UTF_8), originalCiphertext));
            assertFalse(Arrays.equals(originalCiphertext, foreignCiphertext));
            assertEquals(List.of(new Contact(1L, 7L, 1L, "138****1234")), reactive
                    ? async.createQuery().exactEncrypted(Contact::phone, first.phone()).fetch().collectList().block()
                    : sync.createQuery().exactEncrypted(Contact::phone, first.phone()).fetch());
            assertEquals(first, reactive ? async.createQuery().where(Contact::id, 1L).showSensitive().one().block()
                    : sync.createQuery().where(Contact::id, 1L).showSensitive().one());

            Contact changed = new Contact(1L, 7L, 1L, "13900005678");
            if (reactive) {
                Mono<Long> pending = async.update(changed);
                assertArrayEquals(originalCiphertext, stored(statement, 1L, 7L, 1L), "assembly must not write");
                assertEquals(1L, pending.block());
                assertThrows(OptimisticLockConflictException.class, pending::block);
            } else {
                assertEquals(1L, sync.update(changed));
                assertThrows(OptimisticLockConflictException.class, () -> sync.update(changed));
            }
            byte[] changedCiphertext = stored(statement, 1L, 7L, 2L);
            assertFalse(Arrays.equals(originalCiphertext, changedCiphertext));
            assertEquals(0L, reactive ? async.createQuery().exactEncrypted(Contact::phone, first.phone()).count().block()
                    : sync.createQuery().exactEncrypted(Contact::phone, first.phone()).count());
            PageResult<Contact> page = reactive
                    ? async.createQuery().exactEncrypted(Contact::phone, changed.phone()).page(1, 10).block()
                    : sync.createQuery().exactEncrypted(Contact::phone, changed.phone()).page(1, 10);
            assertNotNull(page);
            assertEquals(1L, page.total());
            assertEquals(List.of(new Contact(1L, 7L, 2L, "139****5678")), page.rows());

            Contact trespass = new Contact(2L, 7L, 1L, "must-not-write");
            assertThrows(OptimisticLockConflictException.class, () -> {
                if (reactive) async.update(trespass).block();
                else sync.update(trespass);
            });
            assertNull(reactive ? async.createQuery().where(Contact::id, 2L).one().block()
                    : sync.createQuery().where(Contact::id, 2L).one());
            assertArrayEquals(foreignCiphertext, stored(statement, 2L, 9L, 1L));
            assertArrayEquals(changedCiphertext, stored(statement, 1L, 7L, 2L));

            // Local SET must update both ciphertext and exact-search index under the same version predicate.
            assertEquals(1L, reactive ? async.createUpdate().set(Contact::phone, "13700009012")
                    .where(Contact::id, 1L).optimisticLock(2L).execute().block()
                    : sync.createUpdate().set(Contact::phone, "13700009012")
                    .where(Contact::id, 1L).optimisticLock(2L).execute());
            assertEquals(0L, reactive ? async.createQuery().exactEncrypted(Contact::phone, changed.phone()).count().block()
                    : sync.createQuery().exactEncrypted(Contact::phone, changed.phone()).count());
            Contact finalValue = new Contact(1L, 7L, 3L, "13700009012");
            assertEquals(finalValue, reactive ? async.createQuery().exactEncrypted(Contact::phone, finalValue.phone())
                    .showSensitive().one().block() : sync.createQuery().exactEncrypted(Contact::phone, finalValue.phone())
                    .showSensitive().one());
            assertThrows(OptimisticLockConflictException.class, () -> {
                if (reactive) async.delete(changed).block();
                else sync.delete(changed);
            });
            stored(statement, 1L, 7L, 3L);
            assertEquals(1L, reactive ? async.delete(finalValue).block() : sync.delete(finalValue));
            assertEquals(0L, reactive ? async.createQuery().count().block() : sync.createQuery().count());
            assertArrayEquals(foreignCiphertext, stored(statement, 2L, 9L, 1L));
            assertEquals(foreign, reactive ? tenant9.repository(Contact.class).createQuery()
                    .exactEncrypted(Contact::phone, foreign.phone()).showSensitive().one().block()
                    : tenant9.syncRepository(Contact.class).createQuery()
                    .exactEncrypted(Contact::phone, foreign.phone()).showSensitive().one());
        }
    }

    private static byte[] stored(Statement statement, long id, long tenant, long version) throws Exception {
        try (ResultSet rows = statement.executeQuery("select tenant_id,version,phone "
                + "from protected_contacts where id=" + id)) {
            assertTrue(rows.next());
            assertEquals(tenant, rows.getLong(1));
            assertEquals(version, rows.getLong(2));
            byte[] ciphertext = rows.getBytes(3);
            assertNotNull(ciphertext);
            assertTrue(ciphertext.length > 11);
            assertFalse(rows.next());
            return ciphertext;
        }
    }

    @TableName("protected_contacts")
    public record Contact(@TableId(type = IdType.INPUT) Long id,
                          @TableField("tenant_id") @FlyingTenant Long tenantId,
                          @Version Long version,
                          @EncryptedField @MaskedField(prefix = 3, suffix = 4) String phone) { }
}
