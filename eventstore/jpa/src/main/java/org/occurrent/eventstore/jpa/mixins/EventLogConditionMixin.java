package org.occurrent.eventstore.jpa.mixins;

import jakarta.persistence.criteria.Expression;
import java.util.List;

import lombok.val;
import org.occurrent.condition.Condition;
import org.springframework.data.jpa.domain.Specification;

/**
 * Maps instances of {@link Condition} to instances of {@link Specification}. This interface is
 * completely implemented but consumers can override it if they desire.
 *
 * @param <T> the concrete type that the cloud event is stored at. This is a hibernate managed
 *     entity.
 */
public interface EventLogConditionMixin<T> extends EventLogExpressionMixin<T> {
  private static Comparable convertToComparable(Object o) {
    if (o instanceof Comparable<?> c) {
      return c;
    }

    throw new IllegalArgumentException(
        "Object is not comparable. %s".formatted(o.getClass().getSimpleName()));
  }

  default <U> Specification<T> byAnyCondition(String fieldName, Condition<U> condition) {
    if (condition instanceof Condition.MultiOperandCondition<U> operation) {
      return byMultiCondition(fieldName, operation);
    }
    if (condition instanceof Condition.SingleOperandCondition<U> operation) {
      return bySingleCondition(fieldName, operation);
    }
    throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
  }

  default <U> Specification<T> byMultiCondition(
      String fieldName, Condition.MultiOperandCondition<U> condition) {
    Condition.MultiOperandConditionName operationName = condition.operationName();
    List<Condition<U>> operations = condition.operations();
    List<Specification<T>> specs =
        operations.stream().map(c -> byAnyCondition(fieldName, c)).toList();
    return switch (operationName) {
      case AND ->
          specs.stream()
              .reduce(Specification::and)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Expected multi operand condition to have at least 1 condition if the"
                              + " operand is AND"));
      case OR ->
          specs.stream()
              .reduce(Specification::or)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Expected multi operand condition to have at least 1 condition if the"
                              + " operand is OR"));
      case NOT ->
          Specification.not(
              specs.stream()
                  .findFirst()
                  .orElseThrow(
                      () ->
                          new IllegalArgumentException(
                              "Expected multi operand condition to have exactly 1 condition if the"
                                  + " operand is NOT")));
    };
  }

  abstract class Testy implements Comparable<Testy> {}

  default <U> Specification<T> bySingleCondition(
      String fieldName, Condition.SingleOperandCondition<U> fieldCondition) {
    Condition.SingleOperandConditionName singleOperandConditionName =
        fieldCondition.operandConditionName();
    return ((root, query, builder) -> {
      val value = fieldCondition.operand();
      Expression fieldExpression = expressFieldName(root, fieldName);
      return switch (singleOperandConditionName) {
          // Can use regular value
        case EQ -> builder.equal(fieldExpression, value);
        case NE -> builder.notEqual(fieldExpression, value);
          // Must convert value to Comparable<T>
        case LT -> builder.lessThan(fieldExpression, convertToComparable(fieldCondition.operand()));
        case GT -> builder.greaterThan(fieldExpression, convertToComparable(fieldCondition.operand()));
        case LTE -> builder.lessThanOrEqualTo(fieldExpression, convertToComparable(fieldCondition.operand()));
        case GTE -> builder.greaterThanOrEqualTo(fieldExpression, convertToComparable(fieldCondition.operand()));
      };
    });
  }
}
