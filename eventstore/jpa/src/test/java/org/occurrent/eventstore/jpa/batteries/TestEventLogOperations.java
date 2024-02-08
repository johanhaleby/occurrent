package org.occurrent.eventstore.jpa.batteries;

import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Root;
import java.util.List;
import org.occurrent.condition.Condition;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.jpa.EventLogOperationsDefaultImpl;
import org.springframework.data.jpa.domain.Specification;

public class TestEventLogOperations extends EventLogOperationsDefaultImpl<CloudEventDao> {
  private static <Y> Expression<Y> expressFieldName(
      Root<CloudEventDao> root, FieldNames fieldName) {
    return root.get(fieldName.daoValue());
  }

  @Override
  public <Y> Expression<Y> expressStreamId(Root<CloudEventDao> root) {
    return expressFieldName(root, FieldNames.STREAM_ID);
  }

  @Override
  public <Y> Expression<Y> expressCloudEventId(Root<CloudEventDao> root) {
    return expressFieldName(root, FieldNames.CLOUD_EVENT_ID);
  }

  @Override
  public <Y> Expression<Y> expressCloudEventSource(Root<CloudEventDao> root) {
    return expressFieldName(root, FieldNames.CLOUD_EVENT_SOURCE);
  }

  @Override
  public <Y> Expression<Y> expressFieldName(Root<CloudEventDao> root, String fieldName) {
    return FieldNames.fromStringSafe(fieldName)
        .map(x -> TestEventLogOperations.<Y>expressFieldName(root, x))
        .orElseThrow(
            () ->
                new IllegalArgumentException("Field name %s not represented in FIELD_NAMES enum"));
  }

  @Override
  public EventLogSort<CloudEventDao> defaultSort(SortBy.NaturalImpl sort) {
    return (root, query, criteriaBuilder) ->
        switch (sort.direction) {
          case ASCENDING ->
              List.of(criteriaBuilder.asc(expressFieldName(root, FieldNames.TIMESTAMP)));
          case DESCENDING ->
              List.of(criteriaBuilder.desc(expressFieldName(root, FieldNames.TIMESTAMP)));
        };
  }

  @Override
  public <U> Specification<CloudEventDao> bySingleCondition(
      String fieldName, Condition.SingleOperandCondition<U> fieldCondition) {
    if (null == fieldName) {
      return super.bySingleCondition(null, fieldCondition);
    }
    if (!fieldName.contains("data")) {
      return super.bySingleCondition(fieldName, fieldCondition);
    }

    var parts = fieldName.split("\\.");
    if (parts.length != 2) {
      throw new UnsupportedOperationException("Nope. Nopenopenope");
    }

    var jsonField = parts[1];

    U expectedVersion = fieldCondition.operand();

    // https://stackoverflow.com/a/48492202
    // https://medium.com/@bayern01kahn/spring-data-jpa-examples-querying-data-by-json-properties-in-postgressql-e88dd9eabee9
    //    throw new RuntimeException("Hype!");
    return (root, query, cb) -> {
      return cb.equal(
          cb.function(
              "jsonb_extract_path_text", String.class, root.get("data"), cb.literal(jsonField)),
          expectedVersion);
      //      return builder.equal(builder.function("jsonb_extract_path_text", String.class,
      // root.<String>get("data"), builder.literal(this.locale)), expectedVersion);
    };
  }
}
