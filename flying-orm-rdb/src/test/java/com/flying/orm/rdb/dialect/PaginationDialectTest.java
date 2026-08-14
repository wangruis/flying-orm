package com.flying.orm.rdb.dialect;

import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 验证分页方言只负责在基础 SQL 之上追加分页语法和参数顺序。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
class PaginationDialectTest {

    /**
     * 验证 H2、MySQL 和 PostgreSQL 可复用 limit/offset 分页方言。
     */
    @Test
    void rendersLimitOffsetPagination() {
        SqlRequest request = PaginationDialect.limitOffset()
                                              .paginate("select id from Users where status = ? order by id asc",
                                                        List.of("enabled"),
                                                        PageQuery.of(3, 20));

        assertEquals("select id from Users where status = ? order by id asc limit ? offset ?", request.sql());
        assertEquals(List.of("enabled", 20, 40L), request.parameters());
    }

    /**
     * 验证 offset/fetch 分页方言可作为 Oracle 和 SQL Server 后续适配基础。
     */
    @Test
    void rendersOffsetFetchPagination() {
        SqlRequest request = PaginationDialect.offsetFetch()
                                              .paginate("select id from Users where status = ? order by id asc",
                                                        List.of("enabled"),
                                                        PageQuery.of(2, 10));

        assertEquals("select id from Users where status = ? order by id asc offset ? rows fetch next ? rows only",
                     request.sql());
        assertEquals(List.of("enabled", 10L, 10), request.parameters());
    }
}
