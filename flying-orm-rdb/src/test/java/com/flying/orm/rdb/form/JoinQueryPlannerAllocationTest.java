package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class JoinQueryPlannerAllocationTest {

    private static final int WARMUP_ITERATIONS = 500;
    private static final int MEASURED_ITERATIONS = 100;

    private static volatile Object sink;

    @Test
    void reportsSixteenSourcePagePlanningAllocation() {
        java.lang.management.ThreadMXBean managementBean = ManagementFactory.getThreadMXBean();
        assumeTrue(managementBean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) managementBean;
        assumeTrue(allocationBean.isThreadAllocatedMemorySupported());
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        JoinQueryPlanner planner = planner();
        JoinQuerySpec spec = sixteenSourceSpec();
        PageQuery page = PageQuery.of(2, 25);
        for (int index = 0; index < WARMUP_ITERATIONS; index++) {
            sink = planner.page(spec, page, null);
        }

        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        for (int index = 0; index < MEASURED_ITERATIONS; index++) {
            sink = planner.page(spec, page, null);
        }
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;
        long perInvocation = allocated / MEASURED_ITERATIONS;

        System.out.println("JOIN_PAGE_ALLOCATED_BYTES_PER_INVOCATION=" + perInvocation);
        assertTrue(perInvocation > 0L, "join page allocation measurement must be positive");
    }

    private static JoinQueryPlanner planner() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        return new JoinQueryPlanner(
                renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.safeDefaults());
    }

    private static JoinQuerySpec sixteenSourceSpec() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form(0));
        JoinSource root = builder.root();
        JoinSource previous = root;
        for (int ordinal = 1; ordinal < 16; ordinal++) {
            previous = builder.join(JoinType.INNER, form(ordinal), previous, "id", "parent_id");
        }
        return builder.select(root, "id")
                      .orderBy(root, "id", PageSort.Direction.ASC)
                      .build();
    }

    private static DynamicForm form(int ordinal) {
        DynamicForm.Builder builder = DynamicForm.builder(
                "join-source-" + ordinal, "join_source_" + ordinal)
                                                 .addField(DynamicField.primaryKey("id", "BIGINT"));
        if (ordinal > 0) {
            builder.addField(DynamicField.of("parent_id", "BIGINT"));
        }
        return builder.build();
    }
}
