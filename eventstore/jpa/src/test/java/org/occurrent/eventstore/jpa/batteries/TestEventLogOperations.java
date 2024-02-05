package org.occurrent.eventstore.jpa.batteries;

import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Root;
import java.util.List;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.jpa.operations.EventLogOperations;

public class TestEventLogOperations implements EventLogOperations<CloudEventDao> {
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
}
