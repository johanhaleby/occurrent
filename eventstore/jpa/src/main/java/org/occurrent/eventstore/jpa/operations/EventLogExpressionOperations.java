package org.occurrent.eventstore.jpa.operations;

import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Root;

public interface EventLogExpressionOperations<T> {
  Expression<T> expressStreamId(Root<T> root);

  Expression<T> expressCloudEventId(Root<T> root);

  Expression<T> expressCloudEventSource(Root<T> root);

  Expression<T> expressFieldName(Root<T> root, String fieldName);
}
