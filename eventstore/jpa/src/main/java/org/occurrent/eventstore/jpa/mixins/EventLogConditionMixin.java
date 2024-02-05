package org.occurrent.eventstore.jpa.mixins;

import java.util.List;
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
  private static Number convertToNumber(Object o) {
    // TODO: more robust?
    return (int) o;
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

  default <U> Specification<T> bySingleCondition(
      String fieldName, Condition.SingleOperandCondition<U> fieldCondition) {
    Condition.SingleOperandConditionName singleOperandConditionName =
        fieldCondition.operandConditionName();
    U expectedVersion = fieldCondition.operand();

    return switch (singleOperandConditionName) {
      case EQ ->
          (root, query, builder) ->
              builder.equal(expressFieldName(root, fieldName), expectedVersion);
      case LT ->
          (root, query, builder) ->
              builder.lt(expressFieldName(root, fieldName), convertToNumber(expectedVersion));
      case GT ->
          (root, query, builder) ->
              builder.gt(expressFieldName(root, fieldName), convertToNumber(expectedVersion));
      case LTE ->
          (root, query, builder) ->
              builder.le(expressFieldName(root, fieldName), convertToNumber(expectedVersion));
      case GTE ->
          (root, query, builder) ->
              builder.ge(expressFieldName(root, fieldName), convertToNumber(expectedVersion));
      case NE ->
          (root, query, builder) ->
              builder.notEqual(expressFieldName(root, fieldName), convertToNumber(expectedVersion));
    };
  }
}
