package com.flying.orm.core.join;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.PageSort;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinAndPageContractTest {

    @Test
    void buildsOnlyTheDeclaredLightweightJoinTypesWithStableSources() {
        DynamicForm users = form("users", "user_id");
        DynamicForm departments = form("departments", "department_id");
        DynamicForm regions = form("regions", "region_id");
        DynamicForm companies = form("companies", "company_id");

        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource user = builder.root();
        JoinSource department = builder.join(JoinType.LEFT, departments, user, "user_id", "department_id");
        JoinSource region = builder.join(JoinType.RIGHT, regions, department, "department_id", "region_id");
        JoinSource company = builder.join(JoinType.INNER, companies, region, "region_id", "company_id");
        JoinQuerySpec query = builder.select(user, "user_id")
                                     .select(department, "department_id")
                                     .select(region, "region_id")
                                     .select(company, "company_id")
                                     .build();

        assertEquals(List.of(JoinType.LEFT, JoinType.RIGHT, JoinType.INNER),
                query.joins().stream().map(JoinClause::type).toList());
        assertEquals(List.of(0, 1, 2, 3), query.sources().stream().map(JoinSource::ordinal).toList());
        assertThrows(UnsupportedOperationException.class, () -> query.sources().add(user));
    }

    @Test
    void keepsOffsetAndCursorPaginationBoundsExplicit() {
        PageQuery page = PageQuery.of(Integer.MAX_VALUE, PageQuery.MAX_SIZE, PageSort.asc("id"));
        PageResult<String> result = PageResult.of(List.of("last"), Long.MAX_VALUE, page);

        assertEquals((long) (Integer.MAX_VALUE - 1) * PageQuery.MAX_SIZE, page.offset());
        assertTrue(result.totalPages() > 0);
        assertTrue(result.hasNext());

        CursorPageQuery first = CursorPageQuery.first(
                PageQuery.MAX_SIZE - 1, CursorSort.asc("id"));
        assertTrue(first.firstPage());
        assertThrows(IllegalArgumentException.class,
                () -> CursorPageQuery.first(PageQuery.MAX_SIZE, CursorSort.asc("id")));
    }

    @Test
    void allowsSameTableNameFromDifferentSegmentedSchemas() {
        DynamicForm sales = DynamicForm.relationalBuilder(
                        "sales", RelationIdentity.of(null, "sales", "orders"))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        DynamicForm archive = DynamicForm.relationalBuilder(
                        "archive", RelationIdentity.of(null, "archive", "orders"))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(sales);

        JoinSource archiveSource = builder.join(
                JoinType.INNER, archive, builder.root(), "id", "id");

        assertEquals(1, archiveSource.ordinal());
    }

    private static DynamicForm form(String table, String id) {
        return DynamicForm.builder(table, table)
                          .addField(DynamicField.primaryKey(id, "BIGINT"))
                          .build();
    }
}
