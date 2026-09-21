package com.flying.orm.core.architecture;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ConditionCompilerSurfaceConvergenceTest {

    @Test
    void conditionCompilerOwnsAstCompilationWithoutASecondCompilerType() {
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("com.flying.orm.core.condition.StructuredConditionAstCompiler"));
    }
}
