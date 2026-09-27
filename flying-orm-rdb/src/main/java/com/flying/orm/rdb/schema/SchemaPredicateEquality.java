package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckPredicate;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.flying.orm.rdb.schema.SchemaDefinitionEquality.canonicalName;
import static com.flying.orm.rdb.schema.SchemaDefinitionEquality.sameName;

/** CHECK 表达式与字面量的方言等价规则；只比较结构事实，不生成或执行 SQL。 */
final class SchemaPredicateEquality {

    private SchemaPredicateEquality() {
    }

    static boolean samePredicate(CheckPredicate left,
                                         CheckPredicate right,
                                         SchemaDialect schemaDialect) {
        if (left.equals(right)) {
            return true;
        }
        // Preserve the linear path for unchanged large IN lists, including numeric carrier differences.
        if (left instanceof CheckPredicate.In a && right instanceof CheckPredicate.In b
                && sameName(a.column(), b.column(), schemaDialect) && a.values().size() == b.values().size()) {
            int index = 0;
            while (index < a.values().size()
                    && sameLiteral(a.values().get(index), b.values().get(index), schemaDialect)) {
                index++;
            }
            if (index == a.values().size()) {
                return true;
            }
        }
        return sameNormalizedPredicate(normalizePredicate(left, false, schemaDialect),
                normalizePredicate(right, false, schemaDialect), schemaDialect);
    }

    /** Normalize only proven three-valued-logic identities; keep the public model and fingerprint intact. */
    private static CheckPredicate normalizePredicate(CheckPredicate predicate, boolean negated,
                                                     SchemaDialect dialect) {
        return switch (predicate) {
            case CheckPredicate.Negation not -> normalizePredicate(not.predicate(), !negated, dialect);
            case CheckPredicate.Comparison comparison -> negated
                    ? CheckPredicate.compare(comparison.column(), switch (comparison.operator()) {
                        case EQUAL -> CheckPredicate.ComparisonOperator.NOT_EQUAL;
                        case NOT_EQUAL -> CheckPredicate.ComparisonOperator.EQUAL;
                        case LESS_THAN -> CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL;
                        case LESS_THAN_OR_EQUAL -> CheckPredicate.ComparisonOperator.GREATER_THAN;
                        case GREATER_THAN -> CheckPredicate.ComparisonOperator.LESS_THAN_OR_EQUAL;
                        case GREATER_THAN_OR_EQUAL -> CheckPredicate.ComparisonOperator.LESS_THAN;
                    }, comparison.value()) : comparison;
            case CheckPredicate.NullCheck check -> negated
                    ? new CheckPredicate.NullCheck(check.column(), !check.negated()) : check;
            case CheckPredicate.Range range -> normalizePredicate(rangeComparisons(range), negated, dialect);
            case CheckPredicate.In in -> normalizeGroup(CheckPredicate.LogicalOperator.OR,
                    in.values().stream().<CheckPredicate>map(value -> CheckPredicate.compare(
                            in.column(), CheckPredicate.ComparisonOperator.EQUAL, value)).toList(), negated, dialect);
            case CheckPredicate.Logical logical -> normalizeGroup(
                    logical.operator(), logical.predicates(), negated, dialect);
        };
    }

    private static CheckPredicate normalizeGroup(CheckPredicate.LogicalOperator operator,
                                                  List<CheckPredicate> predicates, boolean negated,
                                                  SchemaDialect dialect) {
        CheckPredicate.LogicalOperator effective = !negated ? operator
                : operator == CheckPredicate.LogicalOperator.AND
                        ? CheckPredicate.LogicalOperator.OR : CheckPredicate.LogicalOperator.AND;
        List<CheckPredicate> children = new ArrayList<>();
        Map<Integer, List<CheckPredicate>> indexed = new HashMap<>();
        for (CheckPredicate predicate : predicates) {
            CheckPredicate normalized = normalizePredicate(predicate, negated, dialect);
            List<CheckPredicate> flattened = normalized instanceof CheckPredicate.Logical logical
                    && logical.operator() == effective ? logical.predicates() : List.of(normalized);
            for (CheckPredicate child : flattened) {
                List<CheckPredicate> bucket = indexed.computeIfAbsent(
                        normalizedPredicateHash(child, dialect), ignored -> new ArrayList<>());
                if (matchingPredicateIndex(bucket, child, dialect) < 0) {
                    bucket.add(child);
                    children.add(child);
                }
            }
        }
        return children.size() == 1 ? children.getFirst() : new CheckPredicate.Logical(effective, children);
    }

    private static boolean sameNormalizedPredicate(CheckPredicate left, CheckPredicate right,
                                                    SchemaDialect schemaDialect) {
        if (left instanceof CheckPredicate.Comparison a && right instanceof CheckPredicate.Comparison b) {
            return sameName(a.column(), b.column(), schemaDialect)
                    && a.operator() == b.operator() && sameLiteral(a.value(), b.value(), schemaDialect);
        }
        if (left instanceof CheckPredicate.NullCheck a && right instanceof CheckPredicate.NullCheck b) {
            return sameName(a.column(), b.column(), schemaDialect) && a.negated() == b.negated();
        }
        if (left instanceof CheckPredicate.Logical a && right instanceof CheckPredicate.Logical b) {
            if (a.operator() != b.operator() || a.predicates().size() != b.predicates().size()) {
                return false;
            }
            Map<Integer, List<CheckPredicate>> unmatched = new HashMap<>();
            for (CheckPredicate child : b.predicates()) {
                unmatched.computeIfAbsent(normalizedPredicateHash(child, schemaDialect),
                        ignored -> new ArrayList<>()).add(child);
            }
            for (CheckPredicate child : a.predicates()) {
                List<CheckPredicate> bucket = unmatched.get(normalizedPredicateHash(child, schemaDialect));
                int index = bucket == null ? -1 : matchingPredicateIndex(bucket, child, schemaDialect);
                if (index < 0) {
                    return false;
                }
                bucket.remove(index);
            }
            return true;
        }
        return false;
    }

    private static int matchingPredicateIndex(List<CheckPredicate> candidates, CheckPredicate predicate,
                                               SchemaDialect dialect) {
        for (int index = 0; index < candidates.size(); index++) {
            if (sameNormalizedPredicate(candidates.get(index), predicate, dialect)) {
                return index;
            }
        }
        return -1;
    }

    /** Hashes only select candidates; collisions still require semantic, one-to-one equality. */
    private static int normalizedPredicateHash(CheckPredicate predicate, SchemaDialect dialect) {
        if (predicate instanceof CheckPredicate.Comparison comparison) {
            return 31 * (31 * canonicalName(comparison.column(), dialect).hashCode()
                    + comparison.operator().hashCode()) + literalHash(comparison.value(), dialect);
        }
        if (predicate instanceof CheckPredicate.NullCheck check) {
            return 31 * canonicalName(check.column(), dialect).hashCode() + Boolean.hashCode(check.negated());
        }
        CheckPredicate.Logical logical = (CheckPredicate.Logical) predicate;
        int hash = logical.operator().hashCode();
        for (CheckPredicate child : logical.predicates()) {
            hash += normalizedPredicateHash(child, dialect);
        }
        return hash;
    }

    private static int literalHash(Object value, SchemaDialect dialect) {
        if (value instanceof Boolean flag && numericBooleanLiterals(dialect)) {
            value = flag ? 1 : 0;
        }
        if (value instanceof Number number) {
            return new BigDecimal(number.toString()).stripTrailingZeros().hashCode();
        }
        // Instant can equal OffsetDateTime in every dialect; offset-only differences remain
        // candidates and are rejected by sameLiteral where that dialect preserves the offset.
        return value instanceof OffsetDateTime time ? time.toInstant().hashCode() : value.hashCode();
    }

    private static CheckPredicate rangeComparisons(CheckPredicate.Range range) {
        return CheckPredicate.and(
                CheckPredicate.compare(range.column(), range.lowerInclusive()
                        ? CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL
                        : CheckPredicate.ComparisonOperator.GREATER_THAN, range.lower()),
                CheckPredicate.compare(range.column(), range.upperInclusive()
                        ? CheckPredicate.ComparisonOperator.LESS_THAN_OR_EQUAL
                        : CheckPredicate.ComparisonOperator.LESS_THAN, range.upper()));
    }

    static boolean sameLiteral(Object left, Object right, SchemaDialect dialect) {
        if (numericBooleanLiterals(dialect)) {
            // These dialects render/store boolean literals as the exact numeric values 0 and 1.
            // Do not use truthiness: e.g. 2 and 1.5 are different SQL literals from TRUE.
            if (left instanceof Boolean flag && right instanceof Number) {
                left = flag ? 1 : 0;
            } else if (right instanceof Boolean flag && left instanceof Number) {
                right = flag ? 1 : 0;
            }
        }
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            try {
                return new BigDecimal(leftNumber.toString()).compareTo(new BigDecimal(rightNumber.toString())) == 0;
            } catch (NumberFormatException ignored) {
                return false;
            }
        }
        if (left instanceof Instant leftInstant && right instanceof OffsetDateTime rightOffset) {
            return leftInstant.equals(rightOffset.toInstant());
        }
        if (left instanceof OffsetDateTime leftOffset && right instanceof Instant rightInstant) {
            return leftOffset.toInstant().equals(rightInstant);
        }
        // PostgreSQL stores the instant; metadata displays it in the session time zone.
        if (dialect != null && dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.POSTGRESQL
                && left instanceof OffsetDateTime leftOffset && right instanceof OffsetDateTime rightOffset) {
            return leftOffset.isEqual(rightOffset);
        }
        return Objects.equals(left, right);
    }

    private static boolean numericBooleanLiterals(SchemaDialect dialect) {
        return dialect != null && switch (dialect.generatedValueStyle()) {
            case MYSQL, ORACLE, SQL_SERVER -> true;
            default -> false;
        };
    }

}
