package com.flying.orm.rdb.dialect;

import com.flying.orm.rdb.internal.dialect.StagedUpsertDialect;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScopedUpsertDialectTest {

    @Test
    void guardsMatchedUpdatesWithoutMovingUpdateOnlyParameters() {
        List<String> actual = dialects().stream().map(dialect -> scoped(dialect,
                List.of("id", "name"), List.of("name", "updated"), List.of("id", "name", "updated"),
                List.of("cast(? as bigint)", "lower(?)", "upper(?)"),
                qualifier(dialect) + ".tenant_id = ?")).toList();
        String assertion = "cast(case when (target.tenant_id = ?) then '1' "
                + "else cast(concat('flying-orm scope violation', target.id) as varchar(4000)) end as integer) = 1";

        assertEquals(List.of(
                "insert into items as target (id, name) values (cast(? as bigint), lower(?)) "
                        + "on conflict (id) do update set name = excluded.name, updated = upper(?) where " + assertion,
                "insert into items (id, name) values (cast(? as bigint), lower(?)) on duplicate key update "
                        + "id = if(id <=> values(id) and (items.tenant_id = ?), id, "
                        + "(select null union all select null)), name = values(name), updated = upper(?)",
                "merge into items target using (select cast(? as bigint) as id, lower(?) as name, "
                        + "upper(?) as updated from dual) source on (target.id = source.id) "
                        + "when matched then update set target.name = source.name, target.updated = source.updated "
                        + "where " + assertion + " "
                        + "when not matched then insert (id, name) values (source.id, source.name)",
                "merge into items with (holdlock) as target using (values (cast(? as bigint), lower(?), upper(?))) "
                        + "as source (id, name, updated) on target.id = source.id when matched and " + assertion
                        + " then update set target.name = source.name, target.updated = source.updated "
                        + "when not matched then insert (id, name) values (source.id, source.name);",
                "merge into items target using (values (cast(? as bigint), lower(?), upper(?))) "
                        + "source (id, name, updated) on target.id = source.id when matched and " + assertion
                        + " then update set target.name = source.name, target.updated = source.updated "
                        + "when not matched then insert (id, name) values (source.id, source.name)"
        ), actual);
    }

    @Test
    void exposesScopeParameterPositionAndExistingTargetQualifier() {
        assertEquals(List.of(3, 2, 3, 3, 3), dialects().stream().map(dialect ->
                dialect.scopeParameterIndex(2, 3)).toList());
        assertEquals(List.of("target", "items", "target", "target", "target"),
                dialects().stream().map(ScopedUpsertDialectTest::qualifier).toList());
    }

    @Test
    void preservesExistingSqlWhenNoColumnCanBeUpdated() {
        for (StagedUpsertDialect dialect : dialects()) {
            String ordinary = dialect.renderStaged("items", List.of("id"), List.of("id"), List.of(),
                    List.of("id"), List.of("?"));
            assertEquals(ordinary, scoped(dialect, List.of("id"), List.of(), List.of("id"),
                    List.of("?"), qualifier(dialect) + ".tenant_id = ?"));
        }
    }

    @Test
    void customStagedDialectDoesNotSilentlyIgnoreScope() {
        StagedUpsertDialect custom = new StagedUpsertDialect() {
            @Override
            public String render(String table, List<String> columns, List<String> conflicts,
                                 List<String> updates, List<String> expressions) {
                return "custom";
            }

            @Override
            public String renderStaged(String table, List<String> inserts, List<String> conflicts,
                                       List<String> updates, List<String> parameters, List<String> expressions) {
                return "custom";
            }
        };
        assertThrows(UnsupportedOperationException.class, () -> scoped(custom, List.of("id", "name"),
                List.of("name"), List.of("id", "name"), List.of("?", "?"), "target.tenant_id = ?"));
        assertEquals(3, custom.scopeParameterIndex(2, 3));
        assertEquals("target", qualifier(custom));
    }

    @Test
    void h2EvaluatesErrorBranchOnlyWhenTheScopeIsNotTrue() throws Exception {
        try (Connection connection = connection();
             var query = connection.prepareStatement("select cast(case when ? = 1 then '1' "
                     + "else 'flying-orm scope violation' end as integer)")) {
            query.setInt(1, 1);
            try (var result = query.executeQuery()) {
                assertTrue(result.next());
                assertEquals(1, result.getInt(1));
            }
            query.setInt(1, 0);
            assertEquals("22018", assertThrows(SQLException.class, query::executeQuery).getSQLState());
            query.setNull(1, java.sql.Types.INTEGER);
            assertEquals("22018", assertThrows(SQLException.class, query::executeQuery).getSQLState());
        }
    }

    @Test
    void h2ChecksTheOldRowAndRejectsFalseOrUnknownWithoutChangingIt() throws Exception {
        StagedUpsertDialect dialect = (StagedUpsertDialect) UpsertDialect.h2();
        String sql = scoped(dialect, List.of("id", "tenant_id", "name"), List.of("tenant_id", "name"),
                List.of("id", "tenant_id", "name"), List.of("?", "?", "?"), "target.tenant_id = ?");
        try (Connection connection = connection(); var statement = connection.createStatement()) {
            statement.execute("create table items(id bigint primary key, tenant_id bigint, name varchar(40))");
            statement.execute("insert into items values (1, 7, 'allowed'), (2, 8, 'foreign'), (3, null, 'unknown')");
            try (var write = connection.prepareStatement(sql)) {
                write.setLong(1, 1);
                write.setLong(2, 9);
                write.setString(3, "updated");
                write.setLong(4, 7);
                assertEquals(1, write.executeUpdate());
                for (long id : List.of(2L, 3L)) {
                    write.setLong(1, id);
                    write.setLong(2, 7);
                    assertEquals("22018", assertThrows(SQLException.class, write::executeUpdate).getSQLState());
                }
                write.setLong(1, 4);
                write.setLong(2, 9);
                write.setString(3, "inserted");
                assertEquals(1, write.executeUpdate());
            }
            try (var rows = statement.executeQuery("select tenant_id, name from items order by id")) {
                assertTrue(rows.next());
                assertEquals(9L, rows.getLong(1));
                assertEquals("updated", rows.getString(2));
                assertTrue(rows.next());
                assertEquals(8L, rows.getLong(1));
                assertEquals("foreign", rows.getString(2));
                assertTrue(rows.next());
                assertNull(rows.getObject(1));
                assertEquals("unknown", rows.getString(2));
                assertTrue(rows.next());
                assertEquals(9L, rows.getLong(1));
                assertEquals("inserted", rows.getString(2));
            }
        }
    }

    private static Connection connection() throws SQLException {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:scoped_upsert_" + UUID.randomUUID());
        return source.getConnection();
    }

    private static String scoped(StagedUpsertDialect dialect, List<String> inserts, List<String> updates,
                                 List<String> parameters, List<String> expressions, String predicate) {
        return dialect.renderScoped("items", inserts, List.of("id"), updates, parameters, expressions, predicate);
    }

    private static String qualifier(StagedUpsertDialect dialect) {
        return dialect.scopeTargetQualifier("items");
    }

    private static List<StagedUpsertDialect> dialects() {
        return List.of((StagedUpsertDialect) UpsertDialect.postgresql(), (StagedUpsertDialect) UpsertDialect.mysql(),
                (StagedUpsertDialect) UpsertDialect.oracle(), (StagedUpsertDialect) UpsertDialect.sqlServer(),
                (StagedUpsertDialect) UpsertDialect.h2());
    }
}
