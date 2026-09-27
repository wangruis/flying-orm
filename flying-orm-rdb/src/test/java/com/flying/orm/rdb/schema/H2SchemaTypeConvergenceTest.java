package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class H2SchemaTypeConvergenceTest {

    @Test
    void normalizationPreservesStorageDifferences() {
        SchemaTypeComparison comparison = new SchemaTypeComparison(SchemaDialect.GeneratedValueStyle.H2);
        for (String[] types : new String[][]{
                {"NVARCHAR(12)", "VARCHAR(13)"}, {"VARBINARY(12)", "BINARY VARYING(13)"},
                {"FLOAT(24)", "FLOAT(25)"}, {"DEC(12,2)", "NUMERIC(12,3)"},
                {"TIME(3) WITHOUT TIME ZONE", "TIME(4)"},
                {"TIMESTAMP(3) WITHOUT TIME ZONE", "TIMESTAMP(3) WITH TIME ZONE"},
                {"INT[]", "INTEGER"}, {"VARCHAR[]", "CHARACTER VARYING(1000000000)[][]"}}) {
            assertFalse(comparison.same(types[0], types[1]), types[0] + " / " + types[1]);
        }
        assertTrue(comparison.same("INT[]", "INTEGER[]"));
        assertTrue(comparison.same("VARCHAR[]", "CHARACTER VARYING(1000000000)[]"));
    }

    @TestFactory
    Stream<DynamicTest> createdTypesConvergeWithTheirDictionaryRepresentation() {
        return Stream.of("INT", "INT2", "INT4", "INT8", "BOOL", "DOUBLE", "FLOAT4", "FLOAT8",
                "FLOAT", "FLOAT(24)", "FLOAT(53)", "DEC(12,2)", "NUMERIC(12)",
                "CHAR(12)", "CHAR", "NCHAR(12)", "NVARCHAR(12)", "VARCHAR2(12)",
                "NATIONAL CHARACTER VARYING(12)", "VARBINARY(12)", "VARBINARY", "BINARY", "MEDIUMINT",
                "NVARCHAR2(12)", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "NTEXT", "NVARCHAR",
                "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "IMAGE", "BYTEA", "RAW(12)",
                "LONG RAW", "LONGVARBINARY", "NUMERIC", "DECIMAL",
                "TIME(3) WITHOUT TIME ZONE", "TIMESTAMP(3) WITHOUT TIME ZONE").map(type ->
                DynamicTest.dynamicTest(type, () -> verifyConvergence(type)));
    }

    private static void verifyConvergence(String type) throws Exception {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:type_readback_" + UUID.randomUUID().toString().replace("-", ""));
        source.setUser("sa");
        RdbDialect dialect = RdbDialect.h2();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(RelationIdentity.table("events"))
                .addColumn(ColumnDefinition.builder("event_value", type).build()).build();
        try (Connection keeper = source.getConnection()) {
            SyncSqlExecutor jdbc = SyncSqlExecutor.jdbc(ConnectionAccessTestSupport.jdbc(source), dialect);
            RelationalSchemaSqlRenderer.create(dialect.schema()).render(SchemaOperation.of(
                    SchemaOperation.Kind.CREATE_TABLE, desired.identity(), "events", null, desired,
                    SchemaOperation.Compatibility.SAFE_INCREMENTAL)).forEach(jdbc::rowsUpdated);
            ReviewedSchemaPlan plan = JdbcSchemaClient.create(jdbc, dialect).reviewRelational(
                    DatabaseDescriptor.of("H2", "2.4.240", dialect), desired,
                    JdbcFormMetadataReaders.create(jdbc, dialect), SchemaCompatibilityMode.EXACT);
            assertTrue(plan.operations().isEmpty(), () -> type + ": " + plan.operations());
        }
    }
}
