package com.flying.orm.core.scope;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinSource;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QualifiedJoinFieldUsePolicyTest {

    @Test
    void keepsQualifiedRequirementsAndDecisionsImmutableAndSourceSpecific() {
        Sources sources = sources();
        JoinFieldRef accountName = new JoinFieldRef(sources.accounts(), " NAME ");
        JoinFieldRef auditName = new JoinFieldRef(sources.audits(), "name");
        FieldUseRequirements.Builder builder = FieldUseRequirements.builder()
                .requireJoin(accountName, FieldUse.PROJECT)
                .requireJoin(auditName, FieldUse.PROJECT);
        FieldUseRequirements requirements = builder.build();
        builder.requireJoin(new JoinFieldRef(sources.accounts(), "id"), FieldUse.JOIN);
        FieldUsePolicy.Builder policyBuilder = FieldUsePolicy.builder()
                .joinVisibility(accountName, FieldVisibility.FULL)
                .joinVisibility(auditName, FieldVisibility.MASKED);
        FieldUsePolicy policy = policyBuilder.build();
        policyBuilder.joinVisibility(auditName, FieldVisibility.FULL);

        FieldUseSnapshot snapshot = policy.approveJoin(
                requirements,
                Map.of(sources.accounts(), FieldScope.readable("name"),
                       sources.audits(), FieldScope.readable("name")));

        assertEquals(2, requirements.joinRequirements().size());
        assertEquals("name", requirements.joinRequirements().getFirst().field().field());
        assertEquals(2, snapshot.joinDecisions().size());
        assertTrue(snapshot.allowed());
        assertEquals(FieldVisibility.FULL, snapshot.joinVisibility(accountName));
        assertEquals(FieldVisibility.MASKED, snapshot.joinVisibility(auditName));
        assertThrows(UnsupportedOperationException.class, snapshot.joinDecisions()::clear);
    }

    @Test
    void intersectsEachQualifiedDecisionWithItsOwnSourceScope() {
        Sources sources = sources();
        JoinFieldRef accountName = new JoinFieldRef(sources.accounts(), "name");
        JoinFieldRef auditName = new JoinFieldRef(sources.audits(), "name");
        FieldUseRequirements requirements = FieldUseRequirements.builder()
                .requireJoin(accountName, FieldUse.PROJECT)
                .requireJoin(auditName, FieldUse.PROJECT)
                .build();
        FieldUsePolicy policy = FieldUsePolicy.builder()
                .joinVisibility(accountName, FieldVisibility.FULL)
                .joinVisibility(auditName, FieldVisibility.FULL)
                .build();

        FieldUseSnapshot snapshot = policy.approveJoin(
                requirements,
                Map.of(sources.accounts(), FieldScope.readable("name"),
                       sources.audits(), FieldScope.readable("id")));

        assertFalse(snapshot.allowed());
        assertTrue(snapshot.joinDecision(accountName, FieldUse.PROJECT, FieldUseOrigin.CALLER)
                           .orElseThrow().allowed());
        assertTrue(snapshot.joinDecision(auditName, FieldUse.PROJECT, FieldUseOrigin.CALLER)
                           .orElseThrow().denied());
        assertEquals(FieldVisibility.HIDDEN, snapshot.joinVisibility(auditName));
        assertEquals(1, snapshot.deniedJoinDecisions().size());
    }

    @Test
    void bareGrantsDoNotAuthorizeMultiSourceRequirementsButBareApiIsUnchanged() {
        Sources sources = sources();
        JoinFieldRef accountName = new JoinFieldRef(sources.accounts(), "name");
        JoinFieldRef auditName = new JoinFieldRef(sources.audits(), "name");
        FieldUsePolicy bare = FieldUsePolicy.builder()
                .visibility("name", FieldVisibility.FULL)
                .build();
        FieldUseRequirements joinRequirements = FieldUseRequirements.builder()
                .requireJoin(accountName, FieldUse.PROJECT)
                .requireJoin(auditName, FieldUse.PROJECT)
                .build();

        FieldUseSnapshot joinSnapshot = bare.approveJoin(
                joinRequirements,
                Map.of(sources.accounts(), FieldScope.unrestricted(),
                       sources.audits(), FieldScope.unrestricted()));
        FieldUseSnapshot bareSnapshot = bare.approve(
                FieldUseRequirements.builder().require("name", FieldUse.PROJECT).build(),
                FieldScope.unrestricted());

        assertFalse(joinSnapshot.allowed());
        assertEquals(2, joinSnapshot.deniedJoinDecisions().size());
        assertTrue(bareSnapshot.allowed());
        assertEquals(FieldVisibility.FULL, bareSnapshot.visibility("name"));
    }

    @Test
    void unrestrictedQualifiedApprovalRetainsTheSharedFastPath() {
        Sources sources = sources();
        FieldUseRequirements requirements = FieldUseRequirements.builder()
                .requireJoin(new JoinFieldRef(sources.accounts(), "name"), FieldUse.PROJECT)
                .requireJoin(new JoinFieldRef(sources.audits(), "name"), FieldUse.PROJECT)
                .build();

        FieldUseSnapshot snapshot = FieldUsePolicy.unrestricted().approveJoin(
                requirements,
                Map.of(sources.accounts(), FieldScope.unrestricted(),
                       sources.audits(), FieldScope.unrestricted()));

        assertSame(FieldUseSnapshot.unrestricted(), snapshot);
    }

    private static Sources sources() {
        DynamicForm accounts = form("accounts", "accounts");
        DynamicForm audits = form("audits", "audits");
        return new Sources(new JoinSource(0, accounts), new JoinSource(1, audits));
    }

    private static DynamicForm form(String id, String table) {
        return DynamicForm.builder(id, table)
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .build();
    }

    private record Sources(JoinSource accounts, JoinSource audits) {
    }
}
