package com.flying.orm.rdb.form.spec;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.internal.MutableValueSnapshots;
import com.flying.orm.rdb.lock.OptimisticLockOptions;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 单条写入的不可变规格，统一承载表单、字段值、条件、数据范围、乐观锁和可选执行保护。
 *
 * <p>字段值按调用方声明顺序复制并允许 {@code null}，之后修改原 Map 不会改变已创建规格。update/delete
 * 的非空条件、租户、DataScope、逻辑删除和乐观锁仍由 FormClient 的安全管线统一校验。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class WriteSpec {

    private final DynamicForm form;
    private final WriteOperation operation;
    private final Map<String, Object> values;
    private final ConditionGroup where;
    private final DataScope scope;
    private final OptimisticLockOptions lock;
    private final SqlExecutionOptions executionOptions;

    private WriteSpec(DynamicForm form,
                      WriteOperation operation,
                      Map<String, Object> values,
                      ConditionGroup where,
                      DataScope scope,
                      OptimisticLockOptions lock,
                      SqlExecutionOptions executionOptions) {
        this.form = Objects.requireNonNull(form, "write form must not be null");
        this.operation = Objects.requireNonNull(operation, "write operation must not be null");
        this.values = snapshotValues(Objects.requireNonNull(values, "write values must not be null"));
        this.where = Objects.requireNonNull(where, "write where must not be null");
        this.scope = Objects.requireNonNull(scope, "write data scope must not be null");
        this.lock = lock;
        this.executionOptions = executionOptions;
    }

    /**
     * 创建插入规格；插入不使用条件。
     *
     * @param form 动态表单
     * @param values 待插入字段值
     * @return 插入规格
     */
    public static WriteSpec insert(DynamicForm form, Map<String, Object> values) {
        return new WriteSpec(form, WriteOperation.INSERT, values,
                             ConditionGroup.and().build(), DataScope.none(), null, null);
    }

    /**
     * 创建更新规格；执行时仍会强制非空 where。
     *
     * @param form 动态表单
     * @param values 待更新字段值
     * @param where 更新条件
     * @return 更新规格
     */
    public static WriteSpec update(DynamicForm form, Map<String, Object> values, ConditionGroup where) {
        return new WriteSpec(form, WriteOperation.UPDATE, values, where, DataScope.none(), null, null);
    }

    /**
     * 创建删除规格；逻辑删除和物理删除由执行方法决定。
     *
     * @param form 动态表单
     * @param where 删除条件
     * @return 删除规格
     */
    public static WriteSpec delete(DynamicForm form, ConditionGroup where) {
        return new WriteSpec(form, WriteOperation.DELETE, Map.of(), where, DataScope.none(), null, null);
    }

    /** @return 动态表单。 */
    public DynamicForm form() {
        return form;
    }

    /** @return 规格创建时声明的操作类型。 */
    public WriteOperation operation() {
        return operation;
    }

    /** @return 保留声明顺序且允许 null 值的只读字段 Map；数组值以独立数组图副本返回。 */
    public Map<String, Object> values() {
        return snapshotValues(values);
    }

    /** @return 参数化写入条件。 */
    public ConditionGroup where() {
        return where;
    }

    /** @return 本次写入附加的数据范围。 */
    public DataScope scope() {
        return scope;
    }

    /** @return 可选乐观锁；为空表示普通写入。 */
    public Optional<OptimisticLockOptions> lock() {
        return Optional.ofNullable(lock);
    }

    /** @return 可选执行保护；为空时使用客户端默认保护。 */
    public Optional<SqlExecutionOptions> executionOptions() {
        return Optional.ofNullable(executionOptions);
    }

    /**
     * @param scope 数据范围
     * @return 仅替换数据范围的新规格
     */
    public WriteSpec withScope(DataScope scope) {
        return new WriteSpec(form, operation, values, where, scope, lock, executionOptions);
    }

    /**
     * @param lock 乐观锁参数
     * @return 仅替换乐观锁的新规格
     */
    public WriteSpec withLock(OptimisticLockOptions lock) {
        return new WriteSpec(form, operation, values, where, scope,
                             Objects.requireNonNull(lock, "write optimistic lock must not be null"), executionOptions);
    }

    /**
     * @param options 执行保护
     * @return 仅替换执行保护的新规格
     */
    public WriteSpec withExecutionOptions(SqlExecutionOptions options) {
        return new WriteSpec(form, operation, values, where, scope, lock,
                             Objects.requireNonNull(options, "write execution options must not be null"));
    }

    /** 冻结 Map 结构和直接数组值的完整数组图，非数组值继续按高层写入对象的既有交接语义保留。 */
    private static Map<String, Object> snapshotValues(Map<String, Object> source) {
        Map<String, Object> snapshot = new LinkedHashMap<>(source.size());
        source.forEach((name, value) -> snapshot.put(name, MutableValueSnapshots.arrayGraph(value)));
        return Collections.unmodifiableMap(snapshot);
    }
}
