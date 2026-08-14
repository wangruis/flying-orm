package com.flying.orm.testkit.dialect;

import java.util.List;

/**
 * 真实库元数据读取后，我们只拿这些关键结构回来对账。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public record ReactiveDialectMetadataResult(String tableName,
                                            List<String> primaryKeys,
                                            List<String> uniqueIndexColumns,
                                            List<String> foreignKeyColumns,
                                            String referencedTableName) {
}
