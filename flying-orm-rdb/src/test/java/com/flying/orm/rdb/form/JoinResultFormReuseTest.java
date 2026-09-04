package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JoinResultFormReuseTest {

    @Test
    void selectAndPageKeepPhysicalTypesAndAliasesForMixedProtectedProjections() {
        DynamicForm form = DynamicForm.builder("join_accounts", "join_accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR").withLength(100))
                .addField(DynamicField.of("note", "VARCHAR").withLength(40))
                .encrypted("secret", EncryptedFieldDefinition.builder().build())
                .build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource root = builder.root();
        JoinQuerySpec spec = builder.selectAs(root, "id", "account_id")
                .selectAs(root, "secret", "protected_value")
                .selectAs(root, "note", "account_note")
                .orderBy(root, "id", PageSort.Direction.ASC).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                    .withProtectedFields(runtime);
            JoinQueryPlanner planner = new JoinQueryPlanner(renderer,
                    new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                    SqlExecutionOptions.safeDefaults());

            DynamicForm selected = planner.plan(spec, null).resultForm();
            DynamicForm paged = planner.page(spec, PageQuery.of(1, 10), null).resultForm();

            assertEquals(List.of("account_id", "protected_value", "account_note"),
                    selected.fields().stream().map(DynamicField::name).toList());
            assertEquals(ProtectedFormLayout.physical(form).field("secret").databaseType(),
                    selected.field("protected_value").databaseType());
            assertEquals(40, selected.field("account_note").length());
            assertEquals(selected.structureFingerprint(), paged.structureFingerprint());
        }
    }
}
