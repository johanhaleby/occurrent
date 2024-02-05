package org.occurrent.eventstore.jpa;

import org.occurrent.eventstore.jpa.mixins.EventLogFilterMixin;
import org.occurrent.eventstore.jpa.mixins.EventLogSortingMixin;
import org.springframework.data.jpa.domain.Specification;

/**
 * Combines all the common operations interfaces into a single type.
 *
 * @param <T> the concrete type that the cloud event is stored at. This is a hibernate managed
 *     entity.
 */
public interface EventLogOperations<T> extends EventLogFilterMixin<T>, EventLogSortingMixin<T> {
  default Specification<T> byStreamId(String streamId) {
    return (root, query, criteriaBuilder) -> criteriaBuilder.equal(expressStreamId(root), streamId);
  }

  default Specification<T> byCloudEventIdAndSource(
      String cloudEventId, java.net.URI cloudEventSource) {
    Specification<T> s1 =
        (root, query, criteriaBuilder) ->
            criteriaBuilder.equal(expressCloudEventId(root), cloudEventId);
    Specification<T> s2 =
        (root, query, criteriaBuilder) ->
            criteriaBuilder.equal(expressCloudEventSource(root), cloudEventSource);
    return s1.and(s2);
  }
}
