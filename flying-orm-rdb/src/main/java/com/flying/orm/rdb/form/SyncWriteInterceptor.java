package com.flying.orm.rdb.form;

import com.flying.orm.core.metadata.Feature;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.WriteSpec;

import java.util.function.Function;

/**
 * 表单级同步写入扩展，注册到 DynamicForm 的 FeatureRegistry。
 * 可在 next 前后处理规格和结果；next 直接执行原生同步操作，不阻塞响应式链。
 * 结果只表示 SQL 执行结果，不表示事务提交；不提供重试、补偿或异常恢复。
 * 批量只拦截整次操作，原始 SQL 执行器不经过此入口。
 * 使用默认 ID 注册，每张表一个扩展；同 ID 的本层声明覆盖父层声明。
 * 扩展需可并发复用，执行后回调失败不表示 SQL 未执行。
 *
 * @author wangr
 * @version v4.1.2
 */
public interface SyncWriteInterceptor extends Feature {

    String ID = "sync-write-interceptor";

    @Override
    default String id() { return ID; }

    /** 单条写入；R 为 Long 或返回生成键时的 SqlWriteResult。 */
    default <R> R write(WriteSpec spec, Function<WriteSpec, R> next) {
        return next.apply(spec);
    }

    /** 流式批量写入；不改变逐行完成回调及执行证据。 */
    default BatchExecutionEvidence batch(BatchSpec spec, Function<BatchSpec, BatchExecutionEvidence> next) {
        return next.apply(spec);
    }
}
