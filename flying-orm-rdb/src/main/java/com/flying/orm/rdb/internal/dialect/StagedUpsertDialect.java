package com.flying.orm.rdb.internal.dialect;

import com.flying.orm.rdb.dialect.UpsertDialect;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.List;

/**
 * 内置方言接收 Repository UPSERT 独立 INSERT/UPDATE 阶段布局的内部扩展点。
 *
 * @author wangr
 * @version v3.1
 */
@InternalApi
public interface StagedUpsertDialect extends UpsertDialect {

    String renderStaged(String table,
                        List<String> insertColumns,
                        List<String> conflictColumns,
                        List<String> updateColumns,
                        List<String> parameterColumns,
                        List<String> valueExpressions);
}
