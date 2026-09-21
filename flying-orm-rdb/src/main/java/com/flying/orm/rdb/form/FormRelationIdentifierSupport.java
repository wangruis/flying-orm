package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/** 统一渲染旧字符串表名、分段关系身份及其派生表，避免通用 SQL 支撑类继续膨胀。 */
final class FormRelationIdentifierSupport {

    private FormRelationIdentifierSupport() {
    }

    static String identifier(
            DynamicForm form,
            UnaryOperator<String> identifierRenderer,
            Function<RelationIdentity, String> relationRenderer) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return safeForm.relationIdentity().isEmpty()
                ? renderIdentifier(identifierRenderer, safeForm.table())
                : renderRelation(relationRenderer, safeForm.relationIdentity().orElseThrow());
    }

    static String derivedIdentifier(
            DynamicForm owner,
            String localTable,
            UnaryOperator<String> identifierRenderer,
            Function<RelationIdentity, String> relationRenderer) {
        DynamicForm safeOwner = Objects.requireNonNull(owner, "dynamic form must not be null");
        return safeOwner.relationIdentity().isEmpty()
                ? renderIdentifier(identifierRenderer, localTable)
                : renderRelation(relationRenderer,
                                 safeOwner.relationIdentity().orElseThrow().withTable(localTable));
    }

    static String render(UnaryOperator<String> renderer, RelationIdentity identity) {
        RelationIdentity safeIdentity = Objects.requireNonNull(
                identity, "relation identity must not be null");
        StringJoiner qualified = new StringJoiner(".");
        safeIdentity.catalog().map(renderer).ifPresent(qualified::add);
        safeIdentity.schema().map(renderer).ifPresent(qualified::add);
        qualified.add(renderer.apply(safeIdentity.table()));
        return qualified.toString();
    }

    private static String renderIdentifier(UnaryOperator<String> renderer, String identifier) {
        return Objects.requireNonNull(
                renderer.apply(identifier), "rendered identifier must not be null");
    }

    private static String renderRelation(
            Function<RelationIdentity, String> renderer, RelationIdentity identity) {
        return Objects.requireNonNull(
                renderer.apply(identity), "rendered relation identifier must not be null");
    }
}
