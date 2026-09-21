package com.flying.orm.rdb.form.spec;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.internal.InternalApi;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 流式批量写入的不可变规格。
 *
 * <p>规格只保存冷 Publisher 引用，不收集全量数据。insert/upsert 行为使用字段 Map，update 行为使用
 * {@link BatchOptimisticUpdate}；三个命名工厂把行类型与操作固定在一起，执行时仍逐行校验并使用有界输入缓冲。</p>
 *
 * <p>upsert 合并客户端默认 Scope 与本规格 Scope，在同一条 SQL 内检查已有冲突目标行。
 * 内置方言仅在目标范围条件为 TRUE 时更新；FALSE 或 NULL 会产生数据库失败，不能改写范围外记录。
 * 新行沿用 insert 的租户值与字段权限规则，没有更新列的幂等插入保留 no-op。
 * 不支持目标范围的扩展方言在交付执行请求前明确拒绝，不静默丢弃 Scope。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class BatchSpec {

    private final DynamicForm form;
    private final Publisher<?> rows;
    private final BatchOperation operation;
    private final DataScope scope;
    private final BatchWriteOptions options;
    private final BatchGeneratedKeys generatedKeys;

    private BatchSpec(DynamicForm form,
                      Publisher<?> rows,
                      BatchOperation operation,
                      DataScope scope,
                      BatchWriteOptions options,
                      BatchGeneratedKeys generatedKeys) {
        this.form = Objects.requireNonNull(form, "batch form must not be null");
        this.rows = Objects.requireNonNull(rows, "batch rows publisher must not be null");
        this.operation = Objects.requireNonNull(operation, "batch operation must not be null");
        this.scope = Objects.requireNonNull(scope, "batch data scope must not be null");
        this.options = options;
        this.generatedKeys = Objects.requireNonNull(generatedKeys, "batch generated keys must not be null");
        if (generatedKeys.required() && operation != BatchOperation.INSERT) {
            throw new IllegalArgumentException("database-generated keys are only supported for batch insert");
        }
    }

    /** @param form 动态表单；@param rows Map 行流；@return insert 批量规格。 */
    public static BatchSpec insert(DynamicForm form, Publisher<Map<String, Object>> rows) {
        return new BatchSpec(form, rows, BatchOperation.INSERT, DataScope.none(), null,
                             BatchGeneratedKeys.none());
    }

    /** @param form 动态表单；@param rows Map 行流；@return upsert 批量规格。 */
    public static BatchSpec upsert(DynamicForm form, Publisher<Map<String, Object>> rows) {
        return new BatchSpec(form, rows, BatchOperation.UPSERT, DataScope.none(), null,
                             BatchGeneratedKeys.none());
    }

    /** @param form 动态表单；@param rows 乐观更新行流；@return update 批量规格。 */
    public static BatchSpec update(DynamicForm form, Publisher<BatchOptimisticUpdate> rows) {
        return new BatchSpec(form, rows, BatchOperation.UPDATE, DataScope.none(), null,
                             BatchGeneratedKeys.none());
    }

    /** @return 动态表单。 */
    public DynamicForm form() {
        return form;
    }

    /** @return 未收集的冷行 Publisher；元素类型由 {@link #operation()} 的命名工厂保证。 */
    public Publisher<?> rows() {
        return rows;
    }

    /** @return 批量操作。 */
    public BatchOperation operation() {
        return operation;
    }

    /** @return 本次数据范围；insert 用于租户和字段保护，update 限制 WHERE，upsert 约束冲突更新的目标行。 */
    public DataScope scope() {
        return scope;
    }

    /** @return 显式批量边界；为空时使用客户端默认批量边界。 */
    public Optional<BatchWriteOptions> options() {
        return Optional.ofNullable(options);
    }

    /**
     * Repository 和执行层之间的生成键协作。普通 Map 批量始终返回 none，业务调用方不需要配置它。
     */
    @InternalApi
    public BatchGeneratedKeys generatedKeys() {
        return generatedKeys;
    }


    /**
     * @param scope 数据范围
     * @return 仅替换数据范围的新规格
     */
    public BatchSpec withScope(DataScope scope) {
        return new BatchSpec(form, rows, operation, scope, options, generatedKeys);
    }

    /** @param options 批量缓冲、输入和内存边界；@return 仅替换批量边界的新规格。 */
    public BatchSpec withOptions(BatchWriteOptions options) {
        return new BatchSpec(form, rows, operation, scope,
                             Objects.requireNonNull(options, "batch write options must not be null"),
                             generatedKeys);
    }

    /**
     * 由实体 Repository 声明数据库生成键回填。只允许 INSERT，避免把没有明确冲突主键的 AUTO upsert
     * 包装成看似可用的操作。
     */
    @InternalApi
    public BatchSpec withGeneratedKeys(BatchGeneratedKeys generatedKeys) {
        return new BatchSpec(form, rows, operation, scope, options,
                             Objects.requireNonNull(generatedKeys, "batch generated keys must not be null"));
    }

}
