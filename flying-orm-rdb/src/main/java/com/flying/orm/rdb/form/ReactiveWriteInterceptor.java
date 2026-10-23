package com.flying.orm.rdb.form;

import com.flying.orm.core.metadata.Feature;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * 表单级响应式写入扩展，注册到 DynamicForm 的 FeatureRegistry。
 * 每次订阅调用一次；可在 next 前后组合异步工作，next 接收本次实际执行的规格。
 * 结果表示 SQL 执行结果，不表示事务提交；异常和取消沿原订阅链传播。
 * 批量只拦截整次操作，不收集行流；原始 SQL 执行器不经过此入口。
 * 使用默认 ID 注册，每张表一个扩展；同 ID 的本层声明覆盖父层声明。
 * 扩展需可并发复用，执行后回调失败不表示 SQL 未执行，ORM 不自动重试。
 *
 * @author wangr
 * @version v4.1.2
 */
public interface ReactiveWriteInterceptor extends Feature {

    String ID = "reactive-write-interceptor";

    @Override
    default String id() { return ID; }

    /** 单条写入；R 为 Long 或返回生成键时的 SqlWriteResult。 */
    default <R> Mono<R> write(WriteSpec spec, Function<WriteSpec, Mono<R>> next) {
        return next.apply(spec);
    }

    /** 流式批量写入；不改变逐行完成回调及执行证据。 */
    default Mono<BatchExecutionEvidence> batch(
            BatchSpec spec, Function<BatchSpec, Mono<BatchExecutionEvidence>> next) {
        return next.apply(spec);
    }
}
