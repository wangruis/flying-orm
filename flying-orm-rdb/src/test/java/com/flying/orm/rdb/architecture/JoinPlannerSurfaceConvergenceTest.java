package com.flying.orm.rdb.architecture;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class JoinPlannerSurfaceConvergenceTest {

    @Test
    void joinPlannerOwnsItsSingleUseResultFormAssembly() {
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("com.flying.orm.rdb.form.JoinResultForms"));
    }
}
