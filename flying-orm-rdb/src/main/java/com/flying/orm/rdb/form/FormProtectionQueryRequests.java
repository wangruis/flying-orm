package com.flying.orm.rdb.form;

import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.List;

/**
 * 受保护 contains 查询的候选行请求只委托给已装配的纯 SQL planner。
 *
 * @author wangr
 * @version v3.2
 */
final class FormProtectionQueryRequests {

    private FormProtectionQueryRequests() {
    }

    static SqlRequest containsRows(FormProtectionSqlSupport protection,
                                   ProtectedFieldRuntime.PreparedContainsQuery query,
                                   List<PageSort> sorts,
                                   int candidateLimit) {
        return protection.contains.rows(query, sorts, candidateLimit);
    }

    static SqlRequest containsRows(FormProtectionSqlSupport protection,
                                   ProtectedFieldRuntime.PreparedContainsQuery query,
                                   CursorPageQuery page,
                                   int candidateLimit) {
        return protection.contains.rows(query, page, candidateLimit);
    }

    static SqlRequest containsRows(FormProtectionSqlSupport protection,
                                   ProtectedFieldRuntime.PreparedContainsQuery query,
                                   CursorPageNormalizer.NormalizedCursorPage page,
                                   int candidateLimit) {
        return protection.contains.rows(query, page, candidateLimit);
    }
}
