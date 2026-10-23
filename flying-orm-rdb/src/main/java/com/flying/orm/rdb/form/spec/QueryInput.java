package com.flying.orm.rdb.form.spec;

import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;

import java.util.List;

/**
 * 完整的外部查询输入，不包含 SQL、执行上下文或 Scope。
 * 空选列表示默认投影，空分页表示不分页；条件、选列、排除及排序整体替换上次输入，服务端 where/Scope 保留。
 *
 * @author wangr
 * @version v4.1.2
 */
public record QueryInput(StructuredConditionInput filter, List<String> select, List<String> exclude,
                         List<PageSort> sorts, PageQuery page) {
    public QueryInput {
        select = select == null ? List.of() : List.copyOf(select);
        exclude = exclude == null ? List.of() : List.copyOf(exclude);
        sorts = sorts == null ? List.of() : List.copyOf(sorts);
    }
}
