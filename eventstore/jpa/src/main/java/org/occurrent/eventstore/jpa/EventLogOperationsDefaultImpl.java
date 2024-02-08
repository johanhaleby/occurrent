package org.occurrent.eventstore.jpa;

import java.net.URI;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.jpa.mixins.AllMixins;
import org.occurrent.filter.Filter;
import org.springframework.data.jpa.domain.Specification;

public abstract class EventLogOperationsDefaultImpl<T>
    implements EventLogOperations<T>, AllMixins<T> {
  @Override
  public Specification<T> sorted(Specification<T> originalSpec, SortBy sort) {
    return (root, query, builder) -> {
      var orders = sorted(sort).apply(root, query, builder);
      query.orderBy(orders);
      return originalSpec.toPredicate(root, query, builder);
    };
  }

  @Override
  public Specification<T> byFilter(Filter filter) {
    return byFilter(null, filter);
  }

  @Override
  public Specification<T> byStreamId(String streamId) {
    return (root, query, criteriaBuilder) -> criteriaBuilder.equal(expressStreamId(root), streamId);
  }

  @Override
  public Specification<T> byCloudEventIdAndSource(String cloudEventId, URI cloudEventSource) {
    Specification<T> s1 =
        (root, query, criteriaBuilder) ->
            criteriaBuilder.equal(expressCloudEventId(root), cloudEventId);
    Specification<T> s2 =
        (root, query, criteriaBuilder) ->
            criteriaBuilder.equal(expressCloudEventSource(root), cloudEventSource);
    return s1.and(s2);
  }
}
