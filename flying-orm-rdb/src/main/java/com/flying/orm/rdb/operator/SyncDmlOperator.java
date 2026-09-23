package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 动态数据的同步链式入口。
 *
 * <p>它只保存 {@link SyncFormClient}、{@link SyncSqlExecutor} 和共享渲染配置，最终直接执行 JDBC。
 * 每个 query、update 或 delete 调用都会创建自己的轻量命令对象。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class SyncDmlOperator {

    private final SyncFormClient formClient;
    private final SyncSqlExecutor executor;
    private final SqlRenderer renderer;
    private final DataScope defaultDataScope;

    /** 原生 JDBC 构造器，统一由同步 DatabaseOperator 装配。 */
    SyncDmlOperator(SyncFormClient formClient,
                    SyncSqlExecutor executor,
                    SqlRenderer renderer,
                    DataScope defaultDataScope) {
        this.formClient = Objects.requireNonNull(formClient, "sync form client must not be null");
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        this.defaultDataScope = Objects.requireNonNull(defaultDataScope, "default data scope must not be null");
    }

    /** 创建单次同步查询构建器。 */
    public SyncQueryOperator query() {
        return new SyncQueryOperator(formClient, executor, renderer, defaultDataScope);
    }

    /** 创建绑定动态表单的新查询；元数据、字段保护与 Scope 由原有表单内核处理。 */
    public SyncQueryOperator query(DynamicForm form) { return query().from(form); }

    /** 插入一行，复用表单的类型转换、租户和加密规则。 */
    public long insert(DynamicForm form, Map<String, Object> values) {
        return formClient.insert(WriteSpec.insert(form, values));
    }

    /** 更新动态表单；保留元数据和非空条件保护。 */
    public SyncDmlUpdateOperator update(DynamicForm form) {
        return new SyncDmlUpdateOperator(formClient, DmlWriteCommand.update(renderer, form));
    }

    /** 删除动态表单；声明逻辑删除时默认软删除。 */
    public SyncDmlDeleteOperator delete(DynamicForm form) {
        return new SyncDmlDeleteOperator(formClient, DmlWriteCommand.delete(renderer, form));
    }

    /** 使用有界批量内核插入；返回实际执行证据。 */
    public BatchExecutionEvidence insertBatch(
            DynamicForm form, List<Map<String, Object>> rows) {
        return formClient.writeBatch(BatchSpec.insert(form, Flux.fromIterable(Objects.requireNonNull(rows, "batch rows must not be null"))));
    }

    /** 按主键批量插入或更新，沿用 Scope 约束冲突目标的语义。 */
    public BatchExecutionEvidence upsertBatch(
            DynamicForm form, List<Map<String, Object>> rows) {
        return formClient.writeBatch(BatchSpec.upsert(form, Flux.fromIterable(Objects.requireNonNull(rows, "batch rows must not be null"))));
    }

    /** 每行携带条件及预期版本的批量更新，保留并发冲突证据。 */
    public BatchExecutionEvidence updateBatch(
            DynamicForm form, List<BatchOptimisticUpdate> rows) {
        return formClient.writeBatch(BatchSpec.update(form, Flux.fromIterable(Objects.requireNonNull(rows, "batch rows must not be null"))));
    }

    /** 创建以 DynamicForm 为根源的原生 JDBC 轻量多表查询。 */
    public SyncJoinQueryOperator joinQuery(DynamicForm rootForm) {
        return new SyncJoinQueryOperator(formClient, renderer, rootForm);
    }

    /** 创建以实体类型为根源的原生 JDBC Lambda 轻量多表查询。 */
    public <T> SyncEntityJoinQueryOperator<T> joinQuery(Class<T> rootType) {
        return new SyncEntityJoinQueryOperator<>(formClient, renderer, rootType);
    }

    /** 创建单次同步更新构建器。 */
    public SyncDmlUpdateOperator update(String table) {
        return new SyncDmlUpdateOperator(formClient, renderer, table);
    }

    /** 创建单次同步删除构建器。 */
    public SyncDmlDeleteOperator delete(String table) {
        return new SyncDmlDeleteOperator(formClient, renderer, table);
    }
}
