package com.flying.orm.rdb.form.spec;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.form.BatchUpdate;
import com.flying.orm.rdb.internal.InternalApi;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * 流式批量写入的不可变规格。
 *
 * <p>规格只保存冷 Publisher 引用，不收集全量数据。insert/upsert 行为使用字段 Map，update 行为使用
 * {@link BatchUpdate}；三个命名工厂把行类型与操作固定在一起，执行时仍逐行校验并使用有界输入缓冲。</p>
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
    private final boolean ignoreConflicts;
    private final Set<String> ignoredUpdates;
    private final List<String> conflictFields;
    private final ConditionGroup updateCondition;

    private BatchSpec(DynamicForm form,
                      Publisher<?> rows,
                      BatchOperation operation,
                      DataScope scope,
                      BatchWriteOptions options,
                      BatchGeneratedKeys generatedKeys,
                      boolean ignoreConflicts, Set<String> ignoredUpdates, List<String> conflictFields,
                      ConditionGroup updateCondition) {
        this.form = Objects.requireNonNull(form, "batch form must not be null");
        this.rows = Objects.requireNonNull(rows, "batch rows publisher must not be null");
        this.operation = Objects.requireNonNull(operation, "batch operation must not be null");
        this.scope = Objects.requireNonNull(scope, "batch data scope must not be null");
        this.options = options;
        this.generatedKeys = Objects.requireNonNull(generatedKeys, "batch generated keys must not be null");
        this.ignoreConflicts = ignoreConflicts;
        this.ignoredUpdates = ignoredUpdates;
        this.conflictFields = conflictFields;
        this.updateCondition = updateCondition;
        if (generatedKeys.required() && (operation == BatchOperation.UPDATE || ignoreConflicts || !conflictFields.isEmpty())) {
            throw new IllegalArgumentException("database-generated keys require insert or primary-key upsert");
        }
    }

    /** @param form 动态表单；@param rows Map 行流；@return insert 批量规格。 */
    public static BatchSpec insert(DynamicForm form, Publisher<Map<String, Object>> rows) {
        return new BatchSpec(form, rows, BatchOperation.INSERT, DataScope.none(), null,
                             BatchGeneratedKeys.none(), false, Set.of(), List.of(), ConditionGroup.and().build());
    }

    /** @param form 动态表单；@param rows Map 行流；@return upsert 批量规格。 */
    public static BatchSpec upsert(DynamicForm form, Publisher<Map<String, Object>> rows) {
        return new BatchSpec(form, rows, BatchOperation.UPSERT, DataScope.none(), null,
                             BatchGeneratedKeys.none(), false, Set.of(), List.of(), ConditionGroup.and().build());
    }

    /** @param form 动态表单；@param rows 普通或乐观锁更新行流；@return update 批量规格。 */
    public static BatchSpec update(DynamicForm form, Publisher<BatchUpdate> rows) {
        return new BatchSpec(form, rows, BatchOperation.UPDATE, DataScope.none(), null,
                             BatchGeneratedKeys.none(), false, Set.of(), List.of(), ConditionGroup.and().build());
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

    /**
     * 冲突时保留已有行，只处理 INSERT 字段、权限和租户值；不执行冲突 UPDATE 或其目标范围条件。
     * 内置 PostgreSQL、Oracle、SQL Server、H2 支持；未实现的方言明确拒绝，不忽略其他数据库错误。
     * @return 启用冲突忽略的新 upsert 规格
     */
    public BatchSpec ignoreConflicts() {
        if (operation != BatchOperation.UPSERT) {
            throw new IllegalStateException("conflict ignore is only supported for batch upsert");
        }
        return ignoreConflicts ? this : new BatchSpec(
                form, rows, operation, scope, options, generatedKeys, true, ignoredUpdates, conflictFields, updateCondition);
    }

    /**
     * 指定数据库已有普通唯一约束对应的冲突字段，冲突后不更新这些字段；不探测或创建约束。
     * 不支持表达式或过滤唯一索引；空值匹配遵循内置方言的普通唯一约束语义。
     * 使用表单字段名，每次调用替换前一次选择；不传字段恢复默认主键。所有冲突字段须参与 INSERT。
     * 随机加密字段不能作为冲突键；CONTAINS 侧索引要求冲突键包含全部主键，以保持归属一致。
     * @param fields 有序冲突字段
     * @return 使用指定冲突键的新 upsert 规格
     */
    public BatchSpec onConflict(String... fields) {
        if (operation != BatchOperation.UPSERT) {
            throw new IllegalStateException("conflict fields are only supported for batch upsert");
        }
        Set<String> names = new LinkedHashSet<>();
        for (String name : fields) {
            DynamicField field = form.field(name);
            if (form.protections().encrypted(field.name()).isPresent()) {
                throw new IllegalArgumentException("encrypted field cannot be an upsert conflict key");
            }
            names.add(field.normalizedName());
        }
        if (!names.isEmpty() && form.protections().encryptedFields().values().stream()
                .anyMatch(field -> field.searchModes().contains(EncryptedSearchMode.CONTAINS))
                && form.fields().stream().filter(DynamicField::primaryKey)
                        .anyMatch(field -> !names.contains(field.normalizedName()))) {
            throw new IllegalArgumentException("CONTAINS upsert conflict fields must include all primary keys");
        }
        List<String> keys = List.copyOf(names);
        return keys.equals(conflictFields) ? this : new BatchSpec(
                form, rows, operation, scope, options, generatedKeys, ignoreConflicts, ignoredUpdates, keys, updateCondition);
    }

    /** @return 有序、规范化的显式冲突字段；空列表表示使用表单主键。 */
    public List<String> conflictFields() {
        return conflictFields;
    }

    /**
     * 本次 upsert 冲突时不更新指定字段，INSERT 仍遵循原字段策略；多次调用累加。
     * 使用表单字段名。加密字段同时保留其检索列；CONTAINS 仍要求两阶段字段一致。
     * @param fields 本次保留已有值的字段
     * @return 不修改原规格的新 upsert 规格
     */
    public BatchSpec ignoreUpdates(String... fields) {
        if (operation != BatchOperation.UPSERT) {
            throw new IllegalStateException("update exclusions are only supported for batch upsert");
        }
        Set<String> names = new LinkedHashSet<>(ignoredUpdates);
        for (String field : fields) names.add(form.field(field).normalizedName());
        return names.equals(ignoredUpdates) ? this : new BatchSpec(
                form, rows, operation, scope, options, generatedKeys, ignoreConflicts, Set.copyOf(names), conflictFields, updateCondition);
    }

    /** @return 本次冲突更新排除的不可变字段规范名集合。 */
    public Set<String> ignoredUpdates() {
        return ignoredUpdates;
    }

    /** @return 是否在冲突时保留已有行。 */
    public boolean ignoresConflicts() {
        return ignoreConflicts;
    }

    /**
     * 冲突目标行满足条件才更新；FALSE/NULL 跳过更新，无冲突仍插入。每次调用替换原条件。
     * 使用表单字段名与既有条件 codec；空条件表示不限制。Scope 仍独立校验，不被本条件放宽。
     * ignoreConflicts 或没有更新列时不执行该条件；无法等价实现的方言明确拒绝。
     */
    public BatchSpec withUpdateCondition(ConditionGroup condition) {
        if (operation != BatchOperation.UPSERT) {
            throw new IllegalStateException("update condition is only supported for batch upsert");
        }
        return new BatchSpec(form, rows, operation, scope, options, generatedKeys, ignoreConflicts,
                ignoredUpdates, conflictFields, Objects.requireNonNull(condition, "upsert update condition must not be null"));
    }

    /** @return 冲突目标行的业务条件；不包含 Scope。 */
    public ConditionGroup updateCondition() {
        return updateCondition;
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
        return new BatchSpec(form, rows, operation, scope, options, generatedKeys, ignoreConflicts, ignoredUpdates, conflictFields, updateCondition);
    }

    /** @param options 批量缓冲、输入和内存边界；@return 仅替换批量边界的新规格。 */
    public BatchSpec withOptions(BatchWriteOptions options) {
        return new BatchSpec(form, rows, operation, scope,
                             Objects.requireNonNull(options, "batch write options must not be null"),
                             generatedKeys, ignoreConflicts, ignoredUpdates, conflictFields, updateCondition);
    }

    /**
     * 由实体 Repository 声明数据库生成键回填；主键 upsert 仅对缺失生成主键的 INSERT 行取键。
     */
    @InternalApi
    public BatchSpec withGeneratedKeys(BatchGeneratedKeys generatedKeys) {
        return new BatchSpec(form, rows, operation, scope, options,
                             Objects.requireNonNull(generatedKeys, "batch generated keys must not be null"),
                             ignoreConflicts, ignoredUpdates, conflictFields, updateCondition);
    }

}
