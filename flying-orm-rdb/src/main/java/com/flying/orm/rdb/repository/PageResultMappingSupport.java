package com.flying.orm.rdb.repository;

import com.flying.orm.core.page.PageResult;

import java.util.List;

/**
 * 把分页查询的返回头（total/page/size）保留下来，只替换 rows，统一改写方式。
 */
final class PageResultMappingSupport {

    private PageResultMappingSupport() {
    }

    static <T> PageResult<T> withRows(PageResult<T> source, List<T> rows) {
        return new PageResult<>(rows,
                               source.total(),
                               source.page(),
                               source.size());
    }
}
