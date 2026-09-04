package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Schema 依赖图的强连通分量结果。
 *
 * <p>同一个分量中的表构成外键环，调用方可以据此执行两阶段建表；分量之间按依赖优先拓扑排序，
 * 分量内部按完整关系身份排序，所以输入声明顺序不会改变规划结果。</p>
 *
 * @author wangr
 * @date 2026-09-03
 * @version v3.2
 */
public final class SchemaStronglyConnectedComponents {

    private final List<List<RelationalTableDefinition>> components;
    private final List<RelationalTableDefinition> dependencyOrder;

    private SchemaStronglyConnectedComponents(SchemaDependencyGraph graph) {
        List<List<RelationIdentity>> discovered = new Tarjan(graph).components();
        components = orderComponents(graph, discovered);
        dependencyOrder = components.stream().flatMap(List::stream).toList();
    }

    /** 计算给定不可变图的稳定强连通分量。 */
    public static SchemaStronglyConnectedComponents of(SchemaDependencyGraph graph) {
        return new SchemaStronglyConnectedComponents(Objects.requireNonNull(
                graph, "schema dependency graph must not be null"));
    }

    /**
     * @return 依赖优先的不可变分量；每个内层列表代表一个可能包含外键环的强连通分量
     */
    public List<List<RelationalTableDefinition>> components() {
        return components;
    }

    /** @return 按分量依赖顺序展开、环内身份稳定的不可变表列表 */
    public List<RelationalTableDefinition> dependencyOrder() {
        return dependencyOrder;
    }

    private static List<List<RelationalTableDefinition>> orderComponents(
            SchemaDependencyGraph graph,
            List<List<RelationIdentity>> discovered) {
        Map<RelationIdentity, Integer> componentByIdentity = new HashMap<>();
        for (int component = 0; component < discovered.size(); component++) {
            for (RelationIdentity identity : discovered.get(component)) {
                componentByIdentity.put(identity, component);
            }
        }

        List<Set<Integer>> dependencies = new ArrayList<>(discovered.size());
        List<Set<Integer>> dependents = new ArrayList<>(discovered.size());
        for (int component = 0; component < discovered.size(); component++) {
            dependencies.add(new HashSet<>());
            dependents.add(new HashSet<>());
        }
        for (int component = 0; component < discovered.size(); component++) {
            for (RelationIdentity identity : discovered.get(component)) {
                for (RelationIdentity dependency : graph.dependencyIdentities(identity)) {
                    int dependencyComponent = componentByIdentity.get(dependency);
                    if (dependencyComponent != component && dependencies.get(component).add(dependencyComponent)) {
                        dependents.get(dependencyComponent).add(component);
                    }
                }
            }
        }

        int[] remainingDependencies = new int[discovered.size()];
        PriorityQueue<Integer> ready = new PriorityQueue<>((left, right) ->
                SchemaDependencyGraph.identityOrder().compare(
                        discovered.get(left).getFirst(), discovered.get(right).getFirst()));
        for (int component = 0; component < discovered.size(); component++) {
            remainingDependencies[component] = dependencies.get(component).size();
            if (remainingDependencies[component] == 0) {
                ready.add(component);
            }
        }

        List<List<RelationalTableDefinition>> ordered = new ArrayList<>(discovered.size());
        while (!ready.isEmpty()) {
            int component = ready.remove();
            ordered.add(discovered.get(component).stream().map(graph::table).toList());
            for (int dependent : dependents.get(component)) {
                if (--remainingDependencies[dependent] == 0) {
                    ready.add(dependent);
                }
            }
        }
        if (ordered.size() != discovered.size()) {
            throw new IllegalStateException("strongly connected component graph must be acyclic");
        }
        return List.copyOf(ordered);
    }

    /** Tarjan 只负责收缩环；最终分量顺序由上面的确定性拓扑排序统一决定。 */
    private static final class Tarjan {

        private final SchemaDependencyGraph graph;
        private final Map<RelationIdentity, Integer> indexes = new HashMap<>();
        private final Map<RelationIdentity, Integer> lowLinks = new HashMap<>();
        private final Deque<RelationIdentity> stack = new ArrayDeque<>();
        private final Set<RelationIdentity> onStack = new HashSet<>();
        private final List<List<RelationIdentity>> components = new ArrayList<>();
        private int nextIndex;

        private Tarjan(SchemaDependencyGraph graph) {
            this.graph = graph;
        }

        private List<List<RelationIdentity>> components() {
            for (RelationIdentity identity : graph.identities()) {
                if (!indexes.containsKey(identity)) {
                    visit(identity);
                }
            }
            return components;
        }

        private void visit(RelationIdentity identity) {
            int index = nextIndex++;
            indexes.put(identity, index);
            lowLinks.put(identity, index);
            stack.push(identity);
            onStack.add(identity);

            for (RelationIdentity dependency : graph.dependencyIdentities(identity)) {
                if (!indexes.containsKey(dependency)) {
                    visit(dependency);
                    lowLinks.put(identity, Math.min(lowLinks.get(identity), lowLinks.get(dependency)));
                } else if (onStack.contains(dependency)) {
                    lowLinks.put(identity, Math.min(lowLinks.get(identity), indexes.get(dependency)));
                }
            }

            if (lowLinks.get(identity).equals(indexes.get(identity))) {
                List<RelationIdentity> component = new ArrayList<>();
                RelationIdentity member;
                do {
                    member = stack.pop();
                    onStack.remove(member);
                    component.add(member);
                } while (!member.equals(identity));
                component.sort(SchemaDependencyGraph.identityOrder());
                components.add(List.copyOf(component));
            }
        }
    }
}
