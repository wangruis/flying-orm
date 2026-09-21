package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.type.DatabaseType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OracleDefaultNullMetadataTest {

    @Test
    void completeOracleSnapshotReadsTheDefaultNullProducedWhenRemovingADefault() {
        for (String expression : List.of("NULL", " null ", "(NULL)")) {
            var queries = OracleMetadataQueries.queries();
            var snapshot = FormMetadataRowConverter.toCompleteSchemaSnapshot(
                    RelationIdentity.of(null, "APP", "ACCOUNTS"),
                    List.of(Map.of("COLUMN_NAME", "CODE", "DATA_TYPE", "VARCHAR2",
                            "CHARACTER_MAXIMUM_LENGTH", 32, "NULLABLE", true,
                            "COLUMN_DEFAULT", expression)),
                    List.of(Map.of("TABLE_REPRESENTABLE", true)),
                    List.of(), List.of(), List.of(), List.of(), List.of(),
                    queries.typeMapper(), queries.snapshotTypeMapper(),
                    InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE);

            assertEquals(ColumnDefault.none(), snapshot.completeTable().orElseThrow()
                    .columns().getFirst().defaultValue());
        }
    }

    @Test
    void doesNotEraseSqlServerDefaultConstraintIdentityOrMysqlTextLiterals() {
        var snapshot = FormMetadataRowConverter.toCompleteSchemaSnapshot(
                RelationIdentity.of(null, "dbo", "accounts"),
                List.of(Map.of("COLUMN_NAME", "code", "DATA_TYPE", "NVARCHAR",
                        "CHARACTER_MAXIMUM_LENGTH", 32, "NULLABLE", true,
                        "COLUMN_DEFAULT", "(NULL)", "DEFAULT_CONSTRAINT_NAME", "DF_accounts_code")),
                List.of(Map.of("TABLE_REPRESENTABLE", true)),
                List.of(), List.of(), List.of(), List.of(), List.of(),
                type -> type, InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER);
        var column = snapshot.completeTable().orElseThrow().columns().getFirst();

        assertEquals(ColumnDefault.none(), column.defaultValue());
        assertEquals("DF_accounts_code", column.defaultConstraintName());
        assertEquals(ColumnDefault.literal("NULL"), RelationalMetadataValueParser.columnDefault(
                "NULL", DatabaseType.of("VARCHAR"),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
    }
}
