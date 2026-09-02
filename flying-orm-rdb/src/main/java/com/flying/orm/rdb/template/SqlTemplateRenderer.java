package com.flying.orm.rdb.template;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.internal.value.BindableValueSnapshots;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.rdb.internal.plan.SqlStatementCompiler;
import com.flying.orm.rdb.internal.template.SqlLexicalScanner;
import com.flying.orm.rdb.internal.template.SqlStatements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** 一次 SQL 模板渲染的输入、扫描状态和输出。 */
final class SqlTemplateRenderer {

    private final CompiledTemplate compiled;

    private final Map<String, Object> values;

    private final Map<String, String> identifiers;

    private final Backend backend;

    private final StringBuilder sql;

    private final List<Object> parameters = new ArrayList<>();

    private SqlTemplateRenderer(CompiledTemplate compiled,
                                Map<String, ?> values,
                                Map<String, String> identifiers,
                                Backend backend,
                                boolean owned) {
        this.compiled = Objects.requireNonNull(compiled, "compiled SQL template must not be null");
        this.values = owned ? ownedValues(values) : snapshotValues(values);
        this.identifiers = owned
                ? Objects.requireNonNull(identifiers, "SQL template identifiers must not be null")
                : Map.copyOf(Objects.requireNonNull(
                        identifiers, "SQL template identifiers must not be null"));
        this.backend = Objects.requireNonNull(backend, "SQL template backend must not be null");
        this.sql = compiled.statement() == null
                ? new StringBuilder(compiled.template().sql().length()) : null;
    }

    static SqlRequest render(SqlTemplate template,
                             Map<String, ?> values,
                             Map<String, String> identifiers,
                             Backend backend) {
        return new SqlTemplateRenderer(compile(template, backend), values, identifiers, backend, false).render();
    }

    /** 渲染已经在调用状态/提供器边界取得所有权的参数，不再重复复制可变值。 */
    static SqlRequest renderOwned(SqlTemplate template,
                                  Map<String, ?> values,
                                  Map<String, String> identifiers,
                                  Backend backend) {
        return new SqlTemplateRenderer(compile(template, backend), values, identifiers, backend, true).render();
    }

    static SqlRequest renderCompiled(CompiledTemplate compiled,
                                     Map<String, ?> values,
                                     Map<String, String> identifiers,
                                     boolean owned) {
        return new SqlTemplateRenderer(
                compiled, values, identifiers, compiled.backend(), owned).render();
    }

    private SqlRequest render() {
        requireMatchingIdentifierSlots();
        if (!compiled.valueSlots().equals(values.keySet())) {
            throw new IllegalArgumentException("SQL template values do not match placeholders");
        }
        if (compiled.statement() != null) {
            for (Segment segment : compiled.segments()) {
                if (segment instanceof Value value) {
                    parameters.add(bindValue(value.name()));
                }
            }
            return new SqlRequest(compiled.statement(), parameters);
        }
        for (Segment segment : compiled.segments()) {
            if (segment instanceof Literal literal) {
                sql.append(literal.text());
            } else if (segment instanceof Identifier identifier) {
                appendIdentifier(identifier.name());
            } else if (segment instanceof Value value) {
                appendValue(value.name());
            } else if (segment instanceof QuestionMark) {
                appendQuestionMark(sql, backend);
            }
        }
        SqlStatementPlan statement = SqlStatementCompiler.compile(
                sql.toString(), parameters.size(), SqlBindMarkerStyle.NATIVE,
                backend.dialect().name());
        return new SqlRequest(statement, parameters);
    }

    private void requireMatchingIdentifierSlots() {
        if (!identifiers.keySet().equals(compiled.identifierSlots())) {
            throw new IllegalArgumentException("SQL template identifier slots do not match registered slots");
        }
    }

    private void appendIdentifier(String name) {
        sql.append(backend.dialect().schema().identifier(identifiers.get(name)));
    }

    private void appendValue(String name) {
        appendBindMarker();
        parameters.add(bindValue(name));
    }

    private Object bindValue(String name) {
        Object value = values.get(name);
        if (value instanceof SqlNullParameter) {
            return value;
        }
        ValueCodecRegistry codecs = backend.valueCodecs();
        if (codecs != ValueCodecRegistry.standard()) {
            // Extension codecs may consume mutable inputs; each slot must keep the saved value intact.
            value = value instanceof ByteBuffer buffer && buffer.isReadOnly()
                    ? buffer.duplicate().order(buffer.order())
                    : BindableValueSnapshots.immutableValue(value);
        }
        return codecs.write(value);
    }

    private void appendBindMarker() {
        if (backend.jdbcBindMarkers()) {
            sql.append('?');
            return;
        }
        String dialectName = backend.dialect().name();
        if ("postgresql".equals(dialectName)) {
            sql.append('$').append(parameters.size() + 1);
        } else if ("sqlserver".equals(dialectName) || "sql-server".equals(dialectName)) {
            sql.append("@P").append(parameters.size());
        } else {
            sql.append('?');
        }
    }

    private static int valuePlaceholderEnd(String source, int index) {
        if (source.charAt(index) != ':'
                || index > 0 && source.charAt(index - 1) == ':'
                || index + 1 >= source.length()
                || source.charAt(index + 1) == ':'
                || !Character.isJavaIdentifierStart(source.charAt(index + 1))) {
            return -1;
        }
        int end = index + 2;
        while (end < source.length() && Character.isJavaIdentifierPart(source.charAt(end))) {
            end++;
        }
        return end;
    }

    static CompiledTemplate compile(SqlTemplate template, Backend backend) {
        return compile(template, backend, false);
    }

    static CompiledTemplate compileRegistered(SqlTemplate template, Backend backend) {
        return compile(template, backend, true);
    }

    private static CompiledTemplate compile(SqlTemplate template,
                                             Backend backend,
                                             boolean registered) {
        SqlTemplate safeTemplate = Objects.requireNonNull(template, "SQL template must not be null");
        Backend safeBackend = Objects.requireNonNull(backend, "SQL template backend must not be null");
        String source = SqlStatements.requireSingle(safeTemplate.sql(), safeBackend.dialect());
        List<Segment> segments = new ArrayList<>();
        Set<String> valueSlots = new LinkedHashSet<>();
        Set<String> identifierSlots = new LinkedHashSet<>();
        StringBuilder literal = new StringBuilder(source.length());
        for (int index = 0; index < source.length();) {
            long protectedSegment = SqlLexicalScanner.protectedSegmentAt(
                    source, index, safeBackend.lexicalRules(), true);
            if (protectedSegment >= 0L) {
                int end = SqlLexicalScanner.segmentEnd(protectedSegment);
                if (SqlLexicalScanner.segmentKind(protectedSegment)
                        == SqlLexicalScanner.SegmentKind.TEMPLATE_SLOT) {
                    flushLiteral(segments, literal);
                    String name = SqlTemplate.requireName(
                            source.substring(index + 2, end - 1), "identifier slot");
                    if (!safeTemplate.identifierSlots().contains(name)) {
                        throw new IllegalArgumentException("SQL template identifier slot is not registered");
                    }
                    identifierSlots.add(name);
                    segments.add(new Identifier(name));
                } else {
                    literal.append(source, index, end);
                }
                index = end;
                continue;
            }
            int valueEnd = valuePlaceholderEnd(source, index);
            if (valueEnd >= 0) {
                flushLiteral(segments, literal);
                String name = source.substring(index + 1, valueEnd);
                valueSlots.add(name);
                segments.add(new Value(name));
                index = valueEnd;
                continue;
            }
            char current = source.charAt(index++);
            if (current == '?') {
                flushLiteral(segments, literal);
                segments.add(new QuestionMark());
            } else {
                literal.append(current);
            }
        }
        flushLiteral(segments, literal);
        if (!identifierSlots.equals(safeTemplate.identifierSlots())) {
            throw new IllegalArgumentException("registered identifier slots are not all used by SQL template");
        }
        List<Segment> compiledSegments = List.copyOf(segments);
        SqlStatementPlan statement = registered && identifierSlots.isEmpty()
                ? compileStaticStatement(compiledSegments, safeBackend)
                : null;
        return new CompiledTemplate(safeTemplate,
                                    safeBackend,
                                    compiledSegments,
                                    Collections.unmodifiableSet(valueSlots),
                                    safeTemplate.identifierSlots(),
                                    statement);
    }

    /** Reuses the validated slot plan when only the driver bind-marker backend changes. */
    static CompiledTemplate retarget(CompiledTemplate compiled, Backend backend) {
        CompiledTemplate safeCompiled = Objects.requireNonNull(
                compiled, "compiled SQL template must not be null");
        Backend safeBackend = Objects.requireNonNull(backend, "SQL template backend must not be null");
        SqlStatementPlan statement = safeCompiled.identifierSlots().isEmpty()
                ? compileStaticStatement(safeCompiled.segments(), safeBackend)
                : null;
        return new CompiledTemplate(safeCompiled.template(),
                                    safeBackend,
                                    safeCompiled.segments(),
                                    safeCompiled.valueSlots(),
                                    safeCompiled.identifierSlots(),
                                    statement);
    }

    private static SqlStatementPlan compileStaticStatement(List<Segment> segments, Backend backend) {
        StringBuilder compiledSql = new StringBuilder();
        int parameterCount = 0;
        for (Segment segment : segments) {
            if (segment instanceof Literal literal) {
                compiledSql.append(literal.text());
            } else if (segment instanceof Value) {
                appendBindMarker(compiledSql, backend, parameterCount++);
            } else if (segment instanceof QuestionMark) {
                appendQuestionMark(compiledSql, backend);
            }
        }
        String sql = compiledSql.toString();
        return SqlStatementCompiler.compile(
                sql, parameterCount, SqlBindMarkerStyle.NATIVE, backend.dialect().name());
    }

    private static void appendQuestionMark(StringBuilder target, Backend backend) {
        if (backend.jdbcBindMarkers()
                && "postgresql".equalsIgnoreCase(backend.dialect().name())) {
            target.append("??");
        } else {
            target.append('?');
        }
    }

    private static void appendBindMarker(StringBuilder target, Backend backend, int parameterIndex) {
        if (backend.jdbcBindMarkers()) {
            target.append('?');
        } else if ("postgresql".equals(backend.dialect().name())) {
            target.append('$').append(parameterIndex + 1);
        } else if ("sqlserver".equals(backend.dialect().name())
                || "sql-server".equals(backend.dialect().name())) {
            target.append("@P").append(parameterIndex);
        } else {
            target.append('?');
        }
    }

    private static void flushLiteral(List<Segment> segments, StringBuilder literal) {
        if (!literal.isEmpty()) {
            segments.add(new Literal(literal.toString()));
            literal.setLength(0);
        }
    }

    private static Map<String, Object> snapshotValues(Map<String, ?> values) {
        Map<String, Object> snapshots = new LinkedHashMap<>();
        Objects.requireNonNull(values, "SQL template values must not be null")
               .forEach((name, value) -> snapshots.put(name, BindableValueSnapshots.immutableValue(value)));
        return snapshots;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> ownedValues(Map<String, ?> values) {
        return (Map<String, Object>) Objects.requireNonNull(
                values, "SQL template values must not be null");
    }

    record CompiledTemplate(SqlTemplate template,
                            Backend backend,
                            List<Segment> segments,
                            Set<String> valueSlots,
                            Set<String> identifierSlots,
                            SqlStatementPlan statement) {
    }

    private sealed interface Segment permits Literal, Value, Identifier, QuestionMark {
    }

    private record Literal(String text) implements Segment {
    }

    private record Value(String name) implements Segment {
    }

    private record Identifier(String name) implements Segment {
    }

    private record QuestionMark() implements Segment {
    }

    /** 同一模板引擎内可复用的方言、词法和值转换规则。 */
    record Backend(RdbDialect dialect,
                   ValueCodecRegistry valueCodecs,
                   SqlLexicalScanner.Rules lexicalRules,
                   boolean jdbcBindMarkers) {

        static Backend create(RdbDialect dialect,
                              ValueCodecRegistry valueCodecs,
                              boolean jdbcBindMarkers) {
            RdbDialect safeDialect = Objects.requireNonNull(dialect, "RDB dialect must not be null");
            return new Backend(safeDialect,
                               Objects.requireNonNull(valueCodecs, "value codec registry must not be null"),
                               SqlLexicalScanner.rulesFor(safeDialect.name()),
                               jdbcBindMarkers);
        }

        Backend {
            dialect = Objects.requireNonNull(dialect, "RDB dialect must not be null");
            valueCodecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
            lexicalRules = Objects.requireNonNull(lexicalRules, "SQL lexical rules must not be null");
        }
    }
}
