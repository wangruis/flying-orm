package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CustomDialectCrudBoundaryTest {

    @Test
    void suppliedReaderEnablesCustomDialectDdlAndInvalidatesItsCachedTables() throws Exception {
        RdbDialect base = RdbDialect.h2();
        RdbDialect custom = RdbDialect.of("application_dialect", base.schema(), base.pagination(),
                base.upsert(), base.json());
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:custom_metadata_" + UUID.randomUUID().toString().replace("-", ""));
        JdbcFormMetadataReader reader = JdbcFormMetadataReaders.cached(
                JdbcSqlExecutor.create(ConnectionAccessTestSupport.jdbc(source), base), base,
                CacheRegionPolicy.metadataDefaults(), MetadataCacheInvalidator.none());
        try (Connection keeper = source.getConnection(); Statement statement = keeper.createStatement();
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source))
                     .dialect(custom).metadataReader(reader).build()) {
            statement.execute("create table items(id bigint)");
            assertEquals(1, clients.syncOperator().metadata().readTable("items").columns().size());
            DynamicForm expanded = DynamicForm.builder("items", "items")
                    .addField(DynamicField.of("id", "BIGINT"))
                    .addField(DynamicField.of("name", "VARCHAR(40)")).build();
            clients.syncOperator().ddl().createOrAlter(expanded);
            assertEquals(2, clients.syncOperator().metadata().readTable("items").columns().size());
            try (ResultSet columns = keeper.getMetaData().getColumns(null, null, "ITEMS", null)) {
                int count = 0;
                while (columns.next()) count++;
                assertEquals(2, count);
            }
        }
    }

    @TestFactory
    Stream<DynamicTest> metadataSupportIsNotAStartupRequirementForExplicitModelCrud() {
        return Stream.of(false, true).map(cached -> DynamicTest.dynamicTest("cached=" + cached,
                () -> verifyExplicitModelCrud(cached)));
    }

    private static void verifyExplicitModelCrud(boolean cached) throws Exception {
        RdbDialect base = RdbDialect.h2();
        RdbDialect custom = RdbDialect.of("application_dialect", base.schema(), base.pagination(),
                base.upsert(), base.json());
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:custom_dialect_" + UUID.randomUUID().toString().replace("-", ""));
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR(40)")).build();
        try (Connection keeper = source.getConnection(); Statement statement = keeper.createStatement()) {
            statement.execute("create table items(id bigint primary key,name varchar(40))");
            try (FlyingOrmClients clients = assertDoesNotThrow(() -> FlyingOrmClients
                    .builder(ConnectionAccessTestSupport.jdbc(source)).dialect(custom)
                    .cachePolicy(OrmCachePolicy.builder().metadata(cached
                            ? CacheRegionPolicy.metadataDefaults() : CacheRegionPolicy.disabled()).build()).build())) {
                assertEquals(1, clients.syncForms().insert(WriteSpec.insert(form, Map.of("id", 1L, "name", "Ada"))));
                assertEquals("Ada", clients.syncForms().select(QuerySpec.of(form,
                        ConditionGroup.and().where("id", "=", 1L).build())).getFirst().get("NAME"));
                assertEquals(1, clients.syncOperator().dml().update(form)
                        .set("name", "Grace").where("id", 1L).execute());
                assertEquals("Grace", clients.syncForms().select(QuerySpec.of(form,
                        ConditionGroup.and().where("id", "=", 1L).build())).getFirst().get("NAME"));
                assertEquals(1, clients.syncOperator().dml().delete(form).where("id", 1L).execute());
                assertTrue(clients.syncForms().select(QuerySpec.of(form, ConditionGroup.and().build())).isEmpty());
                assertThrows(UnsupportedOperationException.class,
                        () -> clients.syncOperator().metadata().readTable("items"));
                assertThrows(UnsupportedOperationException.class,
                        () -> clients.syncOperator().ddl().createOrAlter(form));
                JdbcFormMetadataReader reader = JdbcFormMetadataReaders.create(clients.syncExecutor(), custom);
                assertTrue(reader.snapshotCoverage().observedFacts().isEmpty());
                assertThrows(UnsupportedOperationException.class, () -> reader.readSnapshot("items"));
                assertDoesNotThrow(() -> clients.syncOperator().metadata().invalidateAll());
            }
        }
    }
}
