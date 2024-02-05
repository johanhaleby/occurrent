package org.occurrent.eventstore.jpa.operations;

import java.util.List;
import org.occurrent.condition.Condition;
import org.springframework.data.jpa.domain.Specification;

public interface EventLogConditionOperations<T> {
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
    switch (operationName) {
      case AND:
        // TODO: list might be empty
        return specs.stream().reduce(Specification::and).get();
      case OR:
        // TODO: same as above
        return specs.stream().reduce(Specification::or).get();
      case NOT:
        // TODO: same as above
        return Specification.not(specs.stream().findFirst().get());
      default:
        throw new IllegalStateException("Unexpected value: " + operationName);
    }
  }

  default <U> Specification<T> bySingleCondition(
      String fieldName, Condition.SingleOperandCondition<U> fieldCondition) {
    Condition.SingleOperandConditionName singleOperandConditionName =
        fieldCondition.operandConditionName();
    U expectedVersion = fieldCondition.operand();

    return switch (singleOperandConditionName) {
        // TODO: IDK That this uses fieldname quite the same way mongo does. Might need a more
        // intricate design here...
        // Specifically, field names can differ between JVM name and the table name. Might run into
        // issues with that here
        // Maybe need to pass field name to some other component for mapping. not sure
      case EQ -> (root, query, builder) -> builder.equal(root.get(fieldName), expectedVersion);
      case LT ->
          (root, query, builder) ->
              builder.lt(root.get(fieldName), convertToNumber(expectedVersion));
      case GT ->
          (root, query, builder) ->
              builder.gt(root.get(fieldName), convertToNumber(expectedVersion));
      case LTE ->
          (root, query, builder) ->
              builder.le(root.get(fieldName), convertToNumber(expectedVersion));
      case GTE ->
          (root, query, builder) ->
              builder.ge(root.get(fieldName), convertToNumber(expectedVersion));
      case NE ->
          (root, query, builder) ->
              builder.notEqual(root.get(fieldName), convertToNumber(expectedVersion));
    };
  }
}
