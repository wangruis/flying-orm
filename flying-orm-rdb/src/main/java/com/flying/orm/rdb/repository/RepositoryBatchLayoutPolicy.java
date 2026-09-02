package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;

import java.util.List;
import java.util.Objects;

/**
 * Repository 实体批量写入的列布局前置检查。
 *
 * <p>当前批量执行请求只有一条 SQL 和一套参数列顺序。{@code NOT_NULL}、{@code NOT_EMPTY}
 * 会根据每个实体的运行时值决定字段是否出现，因而可能让同一输入流出现多套 SQL。把不同布局按全局
 * 分组需要缓存整批数据；按遇到顺序切成多条请求又无法保持内部 ATOMIC 的单事务语义，所以这里必须在
 * 订阅和执行 SQL 前拒绝这类实体定义。</p>
 *
 * <p>{@code DEFAULT}/{@code ALWAYS} 始终保留字段，{@code NEVER} 始终省略字段，列集合稳定，
 * 不受这条规则影响。JDBC 和 R2DBC 的 Repository 协调器共用本类，避免两个执行内核出现不同判断。</p>
 */
final class RepositoryBatchLayoutPolicy {

    private RepositoryBatchLayoutPolicy() {
    }

    static void requireStableInsertLayout(EntityMetadata<?> metadata) {
        requireStable(metadata, "insert", false);
    }

    static void requireStableUpsertLayout(EntityMetadata<?> metadata) {
        requireStable(metadata, "upsert", true);
    }

    private static void requireStable(EntityMetadata<?> metadata, String operation, boolean upsert) {
        EntityMetadata<?> safeMetadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
        List<String> variableColumns = safeMetadata.fields().stream()
                .filter(field -> hasVariableLayout(field, upsert))
                .map(EntityFieldMetadata::columnName)
                .toList();
        if (!variableColumns.isEmpty()) {
            throw new IllegalStateException("repository batch " + operation
                                                    + " requires a stable column layout; fields " + variableColumns
                                                    + " use NOT_NULL or NOT_EMPTY write strategies. "
                                                    + "Use DEFAULT, ALWAYS, or NEVER for batch writes.");
        }
    }

    private static boolean hasVariableLayout(EntityFieldMetadata field, boolean upsert) {
        if (!upsert) {
            // readForInsert 会固定跳过数据库生成列，不能把它误判为运行时变化。
            return !field.generation().generated()
                    && field.insertable()
                    && isConditional(field.insertStrategy());
        }
        // INSERT 和冲突 UPDATE 各自固定一套 SQL 列布局；一个阶段的稳定策略不能替另一个阶段
        // 掩盖条件策略，否则首行仍会让后续行绑定到错误的阶段布局。
        return (field.insertable() && isConditional(field.insertStrategy()))
                || (field.updatable() && isConditional(field.updateStrategy()));
    }

    private static boolean isConditional(FieldStrategy strategy) {
        return strategy == FieldStrategy.NOT_NULL || strategy == FieldStrategy.NOT_EMPTY;
    }
}
