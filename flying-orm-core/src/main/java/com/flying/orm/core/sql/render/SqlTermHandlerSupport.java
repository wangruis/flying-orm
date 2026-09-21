package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.ConditionValueShape;
import java.lang.reflect.Array;
import java.util.Iterator;

/** Package-local implementations shared by the public standard term factories. */
final class SqlTermHandlerSupport {

    private static final int MAX_COLLECTION_SIZE = 1_000;

    private SqlTermHandlerSupport() {
    }

    static SqlTermHandler scalar(String id, String sqlOperator) {
        return structural(id, ConditionValueShape.SCALAR, (term, context, output) -> {
            Object value = term.value();
            output.appendSql(context.identifier(term.field()) + " " + sqlOperator + " ?");
            output.addParameter(parameter(context, value));
        });
    }

    /** Database-side case folding avoids Java locale differences. */
    static SqlTermHandler caseInsensitiveLike(String id, String sqlOperator) {
        return structural(id, ConditionValueShape.SCALAR, (term, context, output) -> {
            Object value = term.value();
            output.appendSql("lower(" + context.identifier(term.field()) + ") "
                                     + sqlOperator + " lower(?)");
            output.addParameter(parameter(context, value));
        });
    }

    static SqlTermHandler collection(String id, String sqlOperator) {
        return structural(id, ConditionValueShape.COLLECTION, (term, context, output) -> {
            Object value = term.value();
            output.appendSql(context.identifier(term.field()) + " " + sqlOperator + " (");
            int count = appendValues(value, context, output);
            if (count == 0) {
                throw new IllegalArgumentException("collection term value must not be empty");
            }
            output.appendSql(")");
        });
    }

    static SqlTermHandler range(String id, String sqlOperator) {
        return structural(id, ConditionValueShape.RANGE, (term, context, output) -> {
            Object value = term.value();
            Object first;
            Object second;
            if (value instanceof Iterable<?> iterable) {
                Iterator<?> iterator = iterable.iterator();
                if (!iterator.hasNext()) {
                    throw invalidRange();
                }
                first = iterator.next();
                if (!iterator.hasNext()) {
                    throw invalidRange();
                }
                second = iterator.next();
                if (iterator.hasNext()) {
                    throw invalidRange();
                }
            } else if (value != null && value.getClass().isArray()) {
                if (Array.getLength(value) != 2) {
                    throw invalidRange();
                }
                first = Array.get(value, 0);
                second = Array.get(value, 1);
            } else {
                throw new IllegalArgumentException("in term value must be iterable or array");
            }
            output.appendSql(context.identifier(term.field()) + " " + sqlOperator + " ? and ?");
            output.addParameter(parameter(context, first));
            output.addParameter(parameter(context, second));
        });
    }

    static SqlTermHandler structural(String id,
                                     ConditionValueShape shape,
                                     InternalSqlTermRenderer renderer) {
        return SimpleSqlTermHandler.internal(id, shape, renderer);
    }

    /** The public term accessor already returns an isolated value for the extension codec boundary. */
    static Object parameter(SqlRenderContext context, Object value) {
        return context.parameter(value);
    }

    private static int appendValues(Object value,
                                    SqlRenderContext context,
                                    SqlTermOutput output) {
        int count = 0;
        if (value instanceof Iterable<?> iterable) {
            for (Object item : iterable) {
                count = appendValue(item, count, context, output);
            }
            return count;
        }
        if (value != null && value.getClass().isArray()) {
            int length = Array.getLength(value);
            if (length > MAX_COLLECTION_SIZE) {
                throw collectionTooLarge();
            }
            for (int index = 0; index < length; index++) {
                count = appendValue(Array.get(value, index), count, context, output);
            }
            return count;
        }
        throw new IllegalArgumentException("in term value must be iterable or array");
    }

    private static int appendValue(Object value,
                                   int count,
                                   SqlRenderContext context,
                                   SqlTermOutput output) {
        if (count == MAX_COLLECTION_SIZE) {
            throw collectionTooLarge();
        }
        if (count > 0) {
            output.appendSql(", ");
        }
        output.appendSql("?");
        output.addParameter(parameter(context, value));
        return count + 1;
    }

    private static IllegalArgumentException collectionTooLarge() {
        return new IllegalArgumentException("multi-value term must not contain more than 1000 values");
    }

    private static IllegalArgumentException invalidRange() {
        return new IllegalArgumentException("range term value must contain exactly two values");
    }
}
