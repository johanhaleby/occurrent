package org.occurrent.eventstore.jpa.operations;

import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Root;

public interface EventLogExpressionOperations<T> {
  <Y> Expression<Y> expressStreamId(Root<T> root);

  <Y> Expression<Y> expressCloudEventId(Root<T> root);

  <Y> Expression<Y> expressCloudEventSource(Root<T> root);

  <Y> Expression<Y> expressFieldName(Root<T> root, String fieldName);
}
