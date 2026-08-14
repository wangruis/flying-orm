package com.flying.orm.rdb.form.spec;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
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
 * {@link BatchOptimisticUpdate}；三个命名工厂把行类型与操作固定在一起，执行时仍逐行校验并按有界策略分片。</p>
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
    private final BatchWriteCompletion completion;

    private BatchSpec(DynamicForm form,
                      Publisher<?> rows,
                      BatchOperation operation,
                      DataScope scope,
                      BatchWriteOptions options,
                      BatchGeneratedKeys generatedKeys,
                      BatchWriteCompletion completion) {
        this.form = Objects.requireNonNull(form, "batch form must not be null");
        this.rows = Objects.requireNonNull(rows, "batch rows publisher must not be null");
        this.operation = Objects.requireNonNull(operation, "batch operation must not be null");
        this.scope = Objects.requireNonNull(scope, "batch data scope must not be null");
        this.options = options;
        this.generatedKeys = Objects.requireNonNull(generatedKeys, "batch generated keys must not be null");
        this.completion = Objects.requireNonNull(completion, "batch write completion must not be null");
        if (generatedKeys.required() && operation != BatchOperation.INSERT) {
            throw new IllegalArgumentException("database-generated keys are only supported for batch insert");
        }
    }

    /** @param form 动态表单；@param rows Map 行流；@return insert 批量规格。 */
    public static BatchSpec insert(DynamicForm form, Publisher<Map<String, Object>> rows) {
        return new BatchSpec(form, rows, BatchOperation.INSERT, DataScope.none(), null,
                             BatchGeneratedKeys.none(), BatchWriteCompletion.noop());
    }

    /** @param form 动态表单；@param rows Map 行流；@return upsert 批量规格。 */
    public static BatchSpec upsert(DynamicForm form, Publisher<Map<String, Object>> rows) {
        return new BatchSpec(form, rows, BatchOperation.UPSERT, DataScope.none(), null,
                             BatchGeneratedKeys.none(), BatchWriteCompletion.noop());
    }

    /** @param form 动态表单；@param rows 乐观更新行流；@return update 批量规格。 */
    public static BatchSpec update(DynamicForm form, Publisher<BatchOptimisticUpdate> rows) {
        return new BatchSpec(form, rows, BatchOperation.UPDATE, DataScope.none(), null,
                             BatchGeneratedKeys.none(), BatchWriteCompletion.noop());
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

    /** @return 本次批量写入的数据范围；insert/upsert 用于租户和字段保护，update 还会限制 WHERE。 */
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

    /** @return 外部事务最终结束后的响应式协作。 */
    public BatchWriteCompletion completion() {
        return completion;
    }

    /**
     * @param scope 数据范围
     * @return 仅替换数据范围的新规格
     */
    public BatchSpec withScope(DataScope scope) {
        return new BatchSpec(form, rows, operation, scope, options, generatedKeys, completion);
    }

    /** @param options 批量分片、并发、内存和恢复边界；@return 仅替换批量边界的新规格。 */
    public BatchSpec withOptions(BatchWriteOptions options) {
        return new BatchSpec(form, rows, operation, scope,
                             Objects.requireNonNull(options, "batch write options must not be null"),
                             generatedKeys, completion);
    }

    /**
     * 由实体 Repository 声明数据库生成键回填。只允许 INSERT，避免把没有明确冲突主键的 AUTO upsert
     * 包装成看似可用的操作。
     */
    @InternalApi
    public BatchSpec withGeneratedKeys(BatchGeneratedKeys generatedKeys) {
        return new BatchSpec(form, rows, operation, scope, options,
                             Objects.requireNonNull(generatedKeys, "batch generated keys must not be null"),
                             completion);
    }

    /** @param completion 外部事务结束后的协作；@return 仅替换完成协作的新规格。 */
    public BatchSpec withCompletion(BatchWriteCompletion completion) {
        return new BatchSpec(form, rows, operation, scope, options,
                             generatedKeys,
                             Objects.requireNonNull(completion, "batch write completion must not be null"));
    }
}
