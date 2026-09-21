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

    /** 内置实现把目标行谓词放进冲突更新；未声明支持的扩展方言必须明确拒绝。 */
    default String renderScoped(String table,
                                List<String> insertColumns,
                                List<String> conflictColumns,
                                List<String> updateColumns,
                                List<String> parameterColumns,
                                List<String> valueExpressions,
                                String targetPredicate) {
        throw new UnsupportedOperationException("upsert dialect does not support row-level scope");
    }

    /** Scope 参数在已有阶段参数中的插入位置。 */
    default int scopeParameterIndex(int insertCount, int parameterCount) {
        return parameterCount;
    }

    /** 目标行字段与关系条件使用的 SQL 限定符。 */
    default String scopeTargetQualifier(String table) {
        return "target";
    }
}
