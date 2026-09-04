package com.flying.orm.rdb.dialect;

import com.flying.orm.core.page.PageQuery;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaginationDialectLexicalTest {

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
