package com.flying.orm.core.condition;

/**
 * 条件树节点，表示可以被 SQL 规划器继续解析的结构化 Java 条件。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public sealed interface ConditionNode permits ConditionGroup, TermCondition {
}
