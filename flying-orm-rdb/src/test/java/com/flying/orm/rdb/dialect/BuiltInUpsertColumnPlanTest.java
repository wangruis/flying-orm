package com.flying.orm.rdb.dialect;

import com.flying.orm.rdb.internal.dialect.StagedUpsertDialect;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BuiltInUpsertColumnPlanTest {

    @Test
    void preservesIndependentStageOrderAndExpressionsAcrossAllBuiltInDialects() {
        List<String> actual = dialects().stream().map(dialect -> staged(dialect).renderStaged(
                "items", List.of("shared", "id", "created"), List.of("id"),
                List.of("updated", "shared"), List.of("id", "updated", "shared", "created"),
                List.of("cast(? as bigint)", "upper(?)", "lower(?)", "trim(?)"))).toList();

        assertEquals(List.of(
                "insert into items (shared, id, created) values (lower(?), cast(? as bigint), trim(?)) "
                        + "on conflict (id) do update set updated = upper(?), shared = excluded.shared",
                "insert into items (shared, id, created) values (lower(?), cast(? as bigint), trim(?)) "
                        + "on duplicate key update id = if(id <=> values(id), id, "
                        + "(select null union all select null)), updated = upper(?), shared = values(shared)",
                "merge into items target using (select cast(? as bigint) as id, upper(?) as updated, "
                        + "lower(?) as shared, trim(?) as created from dual) source on (target.id = source.id) "
                        + "when matched then update set target.updated = source.updated, target.shared = source.shared "
                        + "when not matched then insert (shared, id, created) values (source.shared, source.id, source.created)",
                "merge into items with (holdlock) as target using (values (cast(? as bigint), upper(?), "
                        + "lower(?), trim(?))) as source (id, updated, shared, created) on target.id = source.id "
                        + "when matched then update set target.updated = source.updated, target.shared = source.shared "
                        + "when not matched then insert (shared, id, created) values (source.shared, source.id, source.created);",
                "merge into items target using (values (cast(? as bigint), upper(?), lower(?), trim(?))) "
                        + "source (id, updated, shared, created) on target.id = source.id "
                        + "when matched then update set target.updated = source.updated, target.shared = source.shared "
                        + "when not matched then insert (shared, id, created) values (source.shared, source.id, source.created)"
        ), actual);
    }

    @Test
    void preservesFirstExpressionForDuplicateParameterColumnsInPublicDialectSpi() {
        List<String> actual = dialects().stream().map(dialect -> dialect.render(
                "items", List.of("id", "name", "name"), List.of("id"), List.of("name"),
                List.of("?", "lower(?)", "upper(?)"))).toList();

        assertEquals(List.of(
                "insert into items (id, name, name) values (?, lower(?), lower(?)) on conflict (id) "
                        + "do update set name = excluded.name",
                "insert into items (id, name, name) values (?, lower(?), lower(?)) on duplicate key update "
                        + "id = if(id <=> values(id), id, (select null union all select null)), name = values(name)",
                "merge into items target using (select ? as id, lower(?) as name, upper(?) as name from dual) "
                        + "source on (target.id = source.id) when matched then update set target.name = source.name "
                        + "when not matched then insert (id, name, name) values (source.id, source.name, source.name)",
                "merge into items with (holdlock) as target using (values (?, lower(?), upper(?))) "
                        + "as source (id, name, name) on target.id = source.id "
                        + "when matched then update set target.name = source.name "
                        + "when not matched then insert (id, name, name) values (source.id, source.name, source.name);",
                "merge into items target using (values (?, lower(?), upper(?))) source (id, name, name) "
                        + "on target.id = source.id when matched then update set target.name = source.name "
                        + "when not matched then insert (id, name, name) values (source.id, source.name, source.name)"
        ), actual);
    }

    @Test
    void preservesEmptyUpdateAndDefaultMarkersAcrossAllBuiltInDialects() {
        List<String> actual = dialects().stream().map(dialect -> dialect.render(
                "items", List.of("id"), List.of("id"), List.of())).toList();

        assertEquals(List.of(
                "insert into items (id) values (?) on conflict (id) do nothing",
                "insert into items (id) values (?) on duplicate key update "
                        + "id = if(id <=> values(id), id, (select null union all select null))",
                "merge into items target using (select ? as id from dual) source on (target.id = source.id) "
                        + "when not matched then insert (id) values (source.id)",
                "merge into items with (holdlock) as target using (values (?)) as source (id) "
                        + "on target.id = source.id when not matched then insert (id) values (source.id);",
                "merge into items target using (values (?)) source (id) on target.id = source.id "
                        + "when not matched then insert (id) values (source.id)"
        ), actual);
    }

    @Test
    void rejectsMissingInsertAndUpdateColumnsAcrossAllBuiltInDialects() {
        for (UpsertDialect dialect : dialects()) {
            IllegalArgumentException missingInsert = assertThrows(IllegalArgumentException.class,
                    () -> staged(dialect).renderStaged("items", List.of("id", "created"), List.of("id"),
                            List.of(), List.of("id"), List.of("?")));
            IllegalArgumentException missingUpdate = assertThrows(IllegalArgumentException.class,
                    () -> staged(dialect).renderStaged("items", List.of("id"), List.of("id"),
                            List.of("updated"), List.of("id"), List.of("?")));

            assertEquals("upsert parameter columns must cover insert and update columns", missingInsert.getMessage());
            assertEquals(missingInsert.getMessage(), missingUpdate.getMessage());
        }
    }

    @Test
    void rejectsMismatchedExpressionCountsAcrossAllBuiltInDialects() {
        for (UpsertDialect dialect : dialects()) {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> dialect.render("items", List.of("id", "name"), List.of("id"), List.of("name"),
                            List.of("?")));

            assertEquals("upsert value expression count must match parameter column count", failure.getMessage());
        }
    }

    @Test
    void rejectsNullExpressionsEvenForAnUnusedDuplicateColumnAcrossAllBuiltInDialects() {
        for (UpsertDialect dialect : dialects()) {
            assertThrows(NullPointerException.class,
                    () -> dialect.render("items", List.of("id", "name", "name"), List.of("id"), List.of("name"),
                            Arrays.asList("?", "lower(?)", null)));
            assertThrows(NullPointerException.class,
                    () -> dialect.render("items", List.of("id", "name", "name"), List.of("id"), List.of("name"),
                            Arrays.asList("?", null, "upper(?)")));
        }
    }

    private static StagedUpsertDialect staged(UpsertDialect dialect) {
        return (StagedUpsertDialect) dialect;
    }

    private static List<UpsertDialect> dialects() {
        return List.of(UpsertDialect.postgresql(), UpsertDialect.mysql(), UpsertDialect.oracle(),
                       UpsertDialect.sqlServer(), UpsertDialect.h2());
    }
}
