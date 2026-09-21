package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.SqlStatementCompiler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcBindMarkersTest {

    @Test
    void adaptsOnlyPostgresqlMarkersInExecutableCode() {
        R2dbcBindMarkers markers = R2dbcBindMarkers.from(RdbDialect.postgresql());

        assertEquals(
                "select $1, $$?$$, $tag$?$tag$, '?' -- ?\nwhere id = $2",
                markers.adapt("select ?, $$?$$, $tag$?$tag$, '?' -- ?\nwhere id = ?",
                        2, SqlBindMarkerStyle.CANONICAL));
    }

    @Test
    void appliesMysqlCommentRulesWhenCountingQuestionMarks() {
        R2dbcBindMarkers markers = R2dbcBindMarkers.from(RdbDialect.mysql());

        assertEquals(
                "select ?--not-comment\n, ? -- comment ?",
                markers.adapt("select ?--not-comment\n, ? -- comment ?\n",
                        2, SqlBindMarkerStyle.CANONICAL));
        assertThrows(IllegalArgumentException.class,
                () -> markers.adapt("select ? /*! hidden ? */", 1, SqlBindMarkerStyle.CANONICAL));
    }

    @Test
    void rejectsMissingExplicitDialect() {
        assertThrows(NullPointerException.class, () -> R2dbcBindMarkers.from(null));
    }

    @Test
    void adaptsAnOrmCompiledPlan() {
        R2dbcBindMarkers markers = R2dbcBindMarkers.from(RdbDialect.postgresql());
        SqlRequest request = new SqlRequest(
                SqlStatementCompiler.compile(
                        "select ?", 1, SqlBindMarkerStyle.CANONICAL,
                        "POSTGRESQL"),
                java.util.List.of(7));

        assertEquals("select $1", markers.adapt(request));
    }

    @Test
    void ignoresTransportSqlFromAnExternallyPreparedPlan() {
        R2dbcBindMarkers markers = R2dbcBindMarkers.from(RdbDialect.postgresql());
        SqlRequest request = new SqlRequest(
                SqlStatementPlan.prepared(
                        "select ?", SqlBindMarkerStyle.CANONICAL, 1,
                        "POSTGRESQL", "select $1; delete from users"),
                java.util.List.of(7));

        assertEquals("select $1", markers.adapt(request));
    }

    @Test
    void validatesCanonicalSqlFromAnExternallyPreparedPlan() {
        R2dbcBindMarkers markers = R2dbcBindMarkers.from(RdbDialect.postgresql());
        SqlRequest request = new SqlRequest(
                SqlStatementPlan.prepared(
                        "select ?; delete from users", SqlBindMarkerStyle.CANONICAL, 1,
                        "POSTGRESQL", "select $1"),
                java.util.List.of(7));

        assertThrows(IllegalArgumentException.class, () -> markers.adapt(request));
    }

    @Test
    void publicCanonicalPlanCannotForgeCompiledTemplateTrust() {
        R2dbcBindMarkers markers = R2dbcBindMarkers.from(RdbDialect.postgresql());
        SqlRequest request = new SqlRequest(
                SqlStatementPlan.canonical(
                        "select ?; delete from users", SqlBindMarkerStyle.CANONICAL, 1),
                java.util.List.of(7));

        assertThrows(IllegalArgumentException.class, () -> markers.adapt(request));
    }

}
