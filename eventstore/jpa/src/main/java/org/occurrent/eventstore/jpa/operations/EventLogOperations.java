package org.occurrent.eventstore.jpa.operations;

import org.springframework.data.jpa.domain.Specification;

public interface EventLogOperations<T>
    extends EventLogFilterOperations<T>, EventLogSortingOperations<T> {
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
