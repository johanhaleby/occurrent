package org.occurrent.eventstore.jpa;

import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.jpa.mixins.EventLogFilterMixin;
import org.occurrent.eventstore.jpa.mixins.EventLogSortingMixin;
import org.occurrent.filter.Filter;
import org.springframework.data.jpa.domain.Specification;

/**
 * Combines all the common operations interfaces into a single type.
 *
 * @param <T> the concrete type that the cloud event is stored at. This is a hibernate managed
 *     entity.
 */
public interface EventLogOperations<T>{
  Specification<T> sorted(Specification<T> originalSpec, SortBy sort);
  Specification<T> byFilter(Filter filter);
  Specification<T> byStreamId(String streamId);
  Specification<T> byCloudEventIdAndSource(
          String cloudEventId, java.net.URI cloudEventSource);

}
