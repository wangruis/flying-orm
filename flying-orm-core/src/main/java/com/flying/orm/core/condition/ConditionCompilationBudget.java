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

    void checkNode(Path path, StructuredConditionPolicy policy) {
        if (path.depth > policy.maxDepth()) {
            String location = path.text();
            throw StructuredConditionException.of(StructuredConditionErrorCode.DEPTH_EXCEEDED,
                                                  location,
                                                  "structured condition depth exceeds limit at " + location);
        }
        if (nodes >= policy.maxNodes()) {
            String location = path.text();
            throw StructuredConditionException.of(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED,
                                                  location,
                                                  "structured condition node count exceeds limit at " + location);
        }
        nodes++;
    }

    /** 当前 term 已在 checkNode 中计过一次，这里只补 descriptor 声明的额外成本。 */
    void checkTerm(String operator, StructuredConditionPolicy policy, Path path) {
        int additional = policy.termComplexityCost(operator) - 1;
        if (additional > policy.maxNodes() - nodes) {
            String location = path.text();
            throw StructuredConditionException.of(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED,
                                                  location,
                                                  "structured condition node count exceeds limit at " + location);
        }
        nodes += additional;
    }

    /** 活动路径只保存父节点和当前片段，完整路径仅在报告错误时生成。 */
    static final class Path {
        static final Path ROOT = root(ROOT_PATH);

        private final Path parent;
        private final String segment;
        private final int depth;

        private Path(Path parent, String segment, int depth) {
            this.parent = parent;
            this.segment = segment;
            this.depth = depth;
        }

        static Path root(String text) {
            return new Path(null, text, 1);
        }

        Path child(int childIndex) {
            return new Path(this, (parent == null ? "" : "." + ROOT_PATH) + "[" + childIndex + "]",
                    depth + 1);
        }

        Path property(String property) {
            return new Path(this, "." + property, depth);
        }

        Path index(int valueIndex) {
            return valueIndex < 0 ? this : new Path(this, "[" + valueIndex + "]", depth);
        }

        String text() {
            if (parent == null) {
                return segment;
            }
            java.util.ArrayDeque<Path> segments = new java.util.ArrayDeque<>();
            for (Path current = this; current != null; current = current.parent) {
                segments.addLast(current);
            }
            StringBuilder path = new StringBuilder();
            while (!segments.isEmpty()) {
                path.append(segments.removeLast().segment);
            }
            return path.toString();
        }
    }
}
