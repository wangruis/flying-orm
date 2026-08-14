package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.internal.ReflectionFailureSupport;

/**
 * 观测扩展边界共享的 JVM 致命错误恢复工具。
 *
 * <p>日志、指标和脱敏规则的普通运行时故障仍由观测层隔离；若普通异常包装了 VME，则按对象身份在
 * cause/suppressed 身份图中恢复原错误，避免观测旁路静默吞掉 JVM 终止信号。</p>
 *
 * @author wangr
 * @date 2026-08-11
 * @version v1.0
 */
final class ObservationFailureSupport {

    private ObservationFailureSupport() {
    }

    /** 若异常图包含 VME，则传播原对象；普通异常保持由调用方按原策略处理。 */
    static void rethrowVirtualMachineError(Throwable failure) {
        ReflectionFailureSupport.rethrowVirtualMachineError(failure);
    }
}
