package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.internal.plan.SqlStatementCompiler;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcBindMarkersTest {

    @Test
    void adaptsOnlyPostgresqlMarkersInExecutableCode() {
        R2dbcBindMarkers markers = markers("PostgreSQL");

        assertEquals(
                "select $1, $$?$$, $tag$?$tag$, '?' -- ?\nwhere id = $2",
                markers.adapt("select ?, $$?$$, $tag$?$tag$, '?' -- ?\nwhere id = ?",
                        2, SqlBindMarkerStyle.CANONICAL));
    }

    @Test
    void appliesMysqlCommentRulesWhenCountingQuestionMarks() {
        R2dbcBindMarkers markers = markers("MySQL");

        assertEquals(
                "select ?--not-comment\n, ? -- comment ?",
                markers.adapt("select ?--not-comment\n, ? -- comment ?\n",
                        2, SqlBindMarkerStyle.CANONICAL));
        assertThrows(IllegalArgumentException.class,
                () -> markers.adapt("select ? /*! hidden ? */", 1, SqlBindMarkerStyle.CANONICAL));
    }

    @Test
    void acceptsMissingOptionalDriverProductNameWithoutChangingQuestionMarkers() {
        assertEquals("select ?", markers(null).adapt("select ?", 1, SqlBindMarkerStyle.CANONICAL));
    }

    @Test
    void adaptsAnOrmCompiledPlan() {
        R2dbcBindMarkers markers = markers("PostgreSQL");
        SqlRequest request = new SqlRequest(
                SqlStatementCompiler.compile(
                        "select ?", 1, SqlBindMarkerStyle.CANONICAL,
                        "POSTGRESQL"),
                java.util.List.of(7));

        assertEquals("select $1", markers.adapt(request));
    }

    @Test
    void ignoresTransportSqlFromAnExternallyPreparedPlan() {
        R2dbcBindMarkers markers = markers("PostgreSQL");
        SqlRequest request = new SqlRequest(
                SqlStatementPlan.prepared(
                        "select ?", SqlBindMarkerStyle.CANONICAL, 1,
                        "POSTGRESQL", "select $1; delete from users"),
                java.util.List.of(7));

        assertEquals("select $1", markers.adapt(request));
    }

    @Test
    void validatesCanonicalSqlFromAnExternallyPreparedPlan() {
        R2dbcBindMarkers markers = markers("PostgreSQL");
        SqlRequest request = new SqlRequest(
                SqlStatementPlan.prepared(
                        "select ?; delete from users", SqlBindMarkerStyle.CANONICAL, 1,
                        "POSTGRESQL", "select $1"),
                java.util.List.of(7));

        assertThrows(IllegalArgumentException.class, () -> markers.adapt(request));
    }

    @Test
    void publicCanonicalPlanCannotForgeCompiledTemplateTrust() {
        R2dbcBindMarkers markers = markers("PostgreSQL");
        SqlRequest request = new SqlRequest(
                SqlStatementPlan.canonical(
                        "select ?; delete from users", SqlBindMarkerStyle.CANONICAL, 1),
                java.util.List.of(7));

        assertThrows(IllegalArgumentException.class, () -> markers.adapt(request));
    }

    private static R2dbcBindMarkers markers(String name) {
        ConnectionFactory factory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.empty();
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> name;
            }
        };
        return R2dbcBindMarkers.from(factory);
    }
}
