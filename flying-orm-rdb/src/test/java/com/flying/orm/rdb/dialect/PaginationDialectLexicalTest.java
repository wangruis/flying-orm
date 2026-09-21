package com.flying.orm.rdb.dialect;

import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.AbstractList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaginationDialectLexicalTest {

    @Test
    void builtInSqlOnlyRenderingNeverReadsOrCopiesParameters() {
        List<Object> unreadable = new AbstractList<>() {
            @Override
            public Object get(int index) { throw new AssertionError("unexpected parameter read"); }
            @Override
            public int size() { throw new AssertionError("unexpected parameter traversal"); }
        };
        for (PaginationDialect dialect : List.of(PaginationDialect.limitOffset(),
                PaginationDialect.offsetFetch(), PaginationDialect.sqlServerOffsetFetch())) {
            String sql = "select * from t where payload = ? order by id";
            PageQuery page = PageQuery.of(2, 3);
            assertEquals(dialect.paginate(sql, List.of(new byte[]{1}), page).sql(),
                         PaginationDialect.paginateSql(dialect, sql, unreadable, page));
        }
    }

    @Test
    void sqlOnlyRenderingPreservesTheCustomPaginationCallback() {
        List<Object> parameters = List.of(new byte[]{1});
        PageQuery page = PageQuery.of(2, 3);
        AtomicInteger calls = new AtomicInteger();
        PaginationDialect custom = new PaginationDialect() {
            @Override
            public SqlRequest paginate(String sql, List<Object> values, PageQuery suppliedPage) {
                assertEquals("select * from t", sql);
                assertSame(parameters, values);
                assertSame(page, suppliedPage);
                calls.incrementAndGet();
                return new SqlRequest(sql + " limit ?", List.of(3));
            }
            @Override
            public List<Object> paginationParameters(List<Object> values, PageQuery suppliedPage) {
                throw new AssertionError("unexpected parameter callback");
            }
        };
        assertEquals("select * from t limit ?",
                     PaginationDialect.paginateSql(custom, "select * from t", parameters, page));
        assertEquals(1, calls.get());
    }

    @Test
    void recognizesTopLevelOrderByAcrossCommentsAndQuotedIdentifiers() {
        assertEquals(
                "select [order] from t order /* stable */ by [id] offset ? rows fetch next ? rows only",
                PaginationDialect.sqlServerOffsetFetch()
                                 .paginate("select [order] from t order /* stable */ by [id]",
                                         List.of(), PageQuery.of(1, 10))
                                 .sql());
    }

    @Test
    void ignoresOrderByInsideCommentsQuotesAndSubqueries() {
        PaginationDialect dialect = PaginationDialect.sqlServerOffsetFetch();

        assertThrows(IllegalArgumentException.class,
                () -> dialect.paginate("select 'order by', [order by] from t /* order by */",
                        List.of(), PageQuery.of(1, 10)));
        assertThrows(IllegalArgumentException.class,
                () -> dialect.paginate("select * from (select * from t order by id) nested",
                        List.of(), PageQuery.of(1, 10)));
    }
}
