package com.flying.orm.core.condition;

/**
 * 单次结构化条件处理的资源账本。
 *
 * <p>实例绝不跨请求复用：编译和预校验各自创建一份，避免并发请求互相消耗节点额度。
 * 路径生成也集中在这里，保证预校验和正式编译报告同一个前端位置。</p>
 */
final class ConditionCompilationBudget {

    static final String ROOT_PATH = "conditions";

    private int nodes;

    void checkNode(int depth, StructuredConditionPolicy policy, String path) {
        if (depth > policy.maxDepth()) {
            throw StructuredConditionException.of(StructuredConditionErrorCode.DEPTH_EXCEEDED,
                                                  path,
                                                  "structured condition depth exceeds limit at " + path);
        }
        nodes++;
        if (nodes > policy.maxNodes()) {
            throw StructuredConditionException.of(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED,
                                                  path,
                                                  "structured condition node count exceeds limit at " + path);
        }
    }

    static String childConditionPath(String path, int index) {
        if (ROOT_PATH.equals(path)) {
            return ROOT_PATH + "[" + index + "]";
        }
        return path + "." + ROOT_PATH + "[" + index + "]";
    }

    static String propertyPath(String path, String property) {
        return path + "." + property;
    }

    static String valuePath(String path, int index) {
        return index < 0 ? path : path + "[" + index + "]";
    }
}
