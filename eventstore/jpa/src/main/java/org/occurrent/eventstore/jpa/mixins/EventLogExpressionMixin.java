package org.occurrent.eventstore.jpa.mixins;

import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Root;
import org.springframework.data.jpa.domain.Specification;

/**
 * This interface maps specific {@link io.cloudevents.CloudEvent} properties to {@link Expression}s.
 * These expressions will be used to compose {@link Specification} that will then be used to drive
 * queries.
 *
 * @param <T> the concrete type that the cloud event is stored at. This is a hibernate managed
 *     entity.
 */
public interface EventLogExpressionMixin<T> {
  <Y> Expression<Y> expressStreamId(Root<T> root);

  <Y> Expression<Y> expressCloudEventId(Root<T> root);

  <Y> Expression<Y> expressCloudEventSource(Root<T> root);

  <Y> Expression<Y> expressFieldName(Root<T> root, String fieldName);
}
