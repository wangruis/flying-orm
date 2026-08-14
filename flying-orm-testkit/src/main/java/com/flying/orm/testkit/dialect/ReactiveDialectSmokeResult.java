package com.flying.orm.testkit.dialect;

import com.flying.orm.core.page.PageResult;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.List;

/**
 * 真实库 smoke 跑完后，上层拿这几个结果判断链路有没有走通。
 *
 * @param insertedRows  插入影响行数
 * @param upsertedRows  upsert 影响行数
 * @param pageResult    分页查询结果
 * @param deletedRows   删除影响行数
 * @param remainingRows 删除后剩余行
 * @author wangr
 * @date 2026-07-26
 * @version v1.0
 */
public record ReactiveDialectSmokeResult(long insertedRows,
                                         long upsertedRows,
                                         PageResult<DynamicRow> pageResult,
                                         long deletedRows,
                                         List<DynamicRow> remainingRows) {
}
