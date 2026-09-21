package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.LinkedList;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class SqlRequestOwnershipTest {

    @Test
    void internalExecutionCanReuseOwnedValuesWithoutTriggeringAnotherSnapshot() {
        CountingDate source = new CountingDate(123L);
        SqlRequest request = new SqlRequest("select ?", List.of(source));
        source.resetCopies();

        List<Object> publicView = request.parameters();
        List<Object> executionValues = OwnedBindableValues.ownedValues(publicView);

        assertEquals(0, source.copies());
        assertEquals(new Date(123L), executionValues.getFirst());
        assertEquals(0, source.copies());
        assertNotSame(executionValues.getFirst(), publicView.getFirst());
        assertEquals(1, source.copies());
    }

    @Test
    void requestReusesMutableValuesAlreadyOwnedByAFragment() {
        CountingDate source = new CountingDate(123L);
        SqlFragment fragment = new SqlFragment("seen_at = ?", List.of(source));
        source.resetCopies();

        new SqlRequest(
                SqlStatementPlan.canonical(
                        fragment.sql(), SqlBindMarkerStyle.CANONICAL, 1),
                fragment.parameters());

        assertEquals(0, source.copies());
    }

    @Test
    void traversesSequentialParameterListsWithoutIndexedReads() {
        CountingLinkedList<Object> parameters = new CountingLinkedList<>();
        for (int index = 0; index < 512; index++) {
            parameters.add(index);
        }

        SqlRequest request = new SqlRequest("select " + "?, ".repeat(511) + "?", parameters);

        assertEquals(parameters, request.parameters());
        assertEquals(0, parameters.indexedGets);
    }

    @Test
    void snapshotsSequentialMutableParametersLinearlyAndKeepsSharedValues() {
        byte[] bytes = {1, 2, 3};
        CountingLinkedList<Object> parameters = new CountingLinkedList<>();
        parameters.add(bytes);
        for (int index = 1; index < 511; index++) {
            parameters.add(index);
        }
        parameters.add(bytes);

        SqlRequest request = new SqlRequest("select " + "?, ".repeat(511) + "?", parameters);
        bytes[0] = 9;
        parameters.clear();

        assertEquals(0, parameters.indexedGets);
        List<Object> published = request.parameters();
        assertEquals(512, published.size());
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) published.getFirst());
        assertSame(published.getFirst(), published.getLast());
        ((byte[]) published.getFirst())[0] = 7;
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) request.parameters().getFirst());
    }

    @Test
    void reusesTheRendererOwnedParameterListAtTheFinalRequestBoundary() {
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        SqlFragment fragment = renderer.renderWhere(
                ConditionGroup.and().where("status", "=", "active").build());

        SqlRequest request = new SqlRequest(
                SqlStatementPlan.canonical(
                        fragment.sql(), SqlBindMarkerStyle.CANONICAL, fragment.parameters().size()),
                fragment.parameters());

        assertSame(fragment.parameters(), request.parameters());
    }

    @Test
    void snapshotsMutableValuesReceivedThroughThePublicConstructor() {
        byte[] callerValue = {1, 2, 3};
        List<Object> callerParameters = new java.util.ArrayList<>();
        callerParameters.add(callerValue);

        SqlRequest request = new SqlRequest("select ?", callerParameters);
        callerValue[0] = 9;
        callerParameters.clear();

        assertNotSame(callerValue, request.parameters().getFirst());
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) request.parameters().getFirst());
    }

    private static final class CountingLinkedList<E> extends LinkedList<E> {
        private int indexedGets;

        @Override
        public E get(int index) {
            indexedGets++;
            return super.get(index);
        }
    }

    private static final class CountingDate extends Date {
        private final AtomicInteger copies;

        private CountingDate(long time) {
            this(time, new AtomicInteger());
        }

        private CountingDate(long time, AtomicInteger copies) {
            super(time);
            this.copies = copies;
        }

        @Override
        public Object clone() {
            copies.incrementAndGet();
            return new CountingDate(getTime(), copies);
        }

        private void resetCopies() {
            copies.set(0);
        }

        private int copies() {
            return copies.get();
        }
    }

    @Test
    void snapshotsMutableValuesReceivedThroughThePublicFragmentFactory() {
        byte[] callerValue = {1, 2, 3};

        SqlFragment fragment = SqlFragment.of("payload = ?", callerValue);
        SqlRequest request = new SqlRequest(
                SqlStatementPlan.canonical(fragment.sql(), SqlBindMarkerStyle.CANONICAL, 1),
                fragment.parameters());
        callerValue[0] = 9;

        assertNotSame(callerValue, request.parameters().getFirst());
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) request.parameters().getFirst());
    }

    @Test
    void snapshotsMutableValuesWhenAnOwnedRendererFragmentCrossesIntoARequest() {
        SqlFragment fragment = SqlRenderer.builder().addDefaultTerms().build().renderWhere(
                ConditionGroup.and().where("payload", "=", new byte[]{1, 2, 3}).build());

        SqlRequest request = new SqlRequest(
                SqlStatementPlan.canonical(fragment.sql(), SqlBindMarkerStyle.CANONICAL, 1),
                fragment.parameters());
        ((byte[]) fragment.parameters().getFirst())[0] = 9;

        assertNotSame(fragment.parameters().getFirst(), request.parameters().getFirst());
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) request.parameters().getFirst());
    }
}
