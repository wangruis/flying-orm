package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OracleNumericPrecisionMetadataTest {

    @Test
    void physicalCharacterProjectionPreservesDeclaredLengthUnits() throws Exception {
        for (InformationSchemaFormMetadataReader.Queries queries : profiles()) {
            String sql = queries.columnQuery().create(null, "items").sql();
            int start = sql.indexOf("end as DATA_TYPE,") + "end as DATA_TYPE,".length();
            String expression = sql.substring(start, sql.indexOf("as PHYSICAL_DATA_TYPE", start));
            try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:oracle_character_units");
                 PreparedStatement statement = connection.prepareStatement("select " + expression
                         + " from (values (?, ?, 10)) c(DATA_TYPE, CHAR_USED, CHAR_COL_DECL_LENGTH)")) {
                for (String type : List.of("CHAR", "VARCHAR2", "NVARCHAR2")) {
                    for (String unit : List.of("B", "C")) {
                        statement.setString(1, type);
                        statement.setString(2, unit);
                        try (ResultSet result = statement.executeQuery()) {
                            result.next();
                            assertEquals(type.equals("NVARCHAR2") ? type
                                    : type + "(10 " + (unit.equals("B") ? "BYTE" : "CHAR") + ")", result.getString(1));
                        }
                    }
                }
            }
        }
    }

    @Test
    void resolvesOracleStarPrecisionForBothMetadataProfilesAndModels() throws Exception {
        for (InformationSchemaFormMetadataReader.Queries queries : profiles()) {
            for (int scale : List.of(0, 2)) {
                Integer precision = projectPrecision(queries, "NUMBER", null, scale);
                assertEquals(38, precision);
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("COLUMN_NAME", "amount");
                row.put("DATA_TYPE", "NUMBER");
                row.put("PHYSICAL_DATA_TYPE", "NUMBER");
                row.put("NUMERIC_PRECISION", precision);
                row.put("NUMERIC_SCALE", scale);
                row.put("NULLABLE", true);

                DynamicForm form = FormMetadataRowConverter.toDynamicForm(
                        "amounts", "amounts", List.of(row), queries.typeMapper());
                assertEquals(38, form.field("amount").precision());
                assertEquals(scale, form.field("amount").scale());
                ColumnDefinition column = FormMetadataRowConverter.toCompleteSchemaSnapshot(
                        RelationIdentity.table("amounts"), List.of(row),
                        List.of(Map.of("TABLE_REPRESENTABLE", true)),
                        List.of(), List.of(), List.of(), List.of(), List.of(),
                        queries.typeMapper(), queries.snapshotTypeMapper(), queries.snapshotDialect())
                        .columns().value().getFirst();
                assertEquals(38, column.precision());
                assertEquals(scale, column.scale());
            }
        }
    }

    @Test
    void keepsUnconstrainedNumbersExplicitPrecisionAndOtherTypesUnchanged() throws Exception {
        for (InformationSchemaFormMetadataReader.Queries queries : profiles()) {
            assertNull(projectPrecision(queries, "NUMBER", null, null));
            assertEquals(8, projectPrecision(queries, "NUMBER", 8, 2));
            assertEquals(126, projectPrecision(queries, "FLOAT", 126, null));
            assertNull(projectPrecision(queries, "TIMESTAMP", null, 6));
        }
    }

    private static List<InformationSchemaFormMetadataReader.Queries> profiles() {
        return List.of(OracleMetadataQueries.queries(), OracleMetadataQueries.queries12c());
    }

    private static Integer projectPrecision(InformationSchemaFormMetadataReader.Queries queries,
                                            String type, Integer precision, Integer scale) throws Exception {
        String sql = queries.columnQuery().create(null, "amounts").sql();
        String precedingColumn = "as CHARACTER_MAXIMUM_LENGTH,";
        int start = sql.indexOf(precedingColumn) + precedingColumn.length();
        String expression = sql.substring(start, sql.indexOf("as NUMERIC_PRECISION", start));
        // Evaluate the actual portable CASE projection against Oracle dictionary-shaped values.
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:oracle_precision");
             PreparedStatement statement = connection.prepareStatement("select " + expression
                     + " from (values (?, cast(? as integer), cast(? as integer)))"
                     + " c(DATA_TYPE, DATA_PRECISION, DATA_SCALE)")) {
            statement.setString(1, type);
            statement.setObject(2, precision, Types.INTEGER);
            statement.setObject(3, scale, Types.INTEGER);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getObject(1, Integer.class);
            }
        }
    }
}
