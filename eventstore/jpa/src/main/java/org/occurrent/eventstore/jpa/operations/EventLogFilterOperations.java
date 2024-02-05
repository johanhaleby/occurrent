package org.occurrent.eventstore.jpa.operations;

import org.occurrent.filter.Filter;
import org.springframework.data.jpa.domain.Specification;

public interface EventLogFilterOperations<T> extends EventLogConditionOperations<T> {
  default Specification<T> byFilter(Filter filter) {
    return byFilter(null, filter);
  }

  default Specification<T> byFilter(String fieldNamePrefix, Filter filter) {
    if (filter instanceof Filter.All) {
      return all();
    }
    if (filter instanceof Filter.SingleConditionFilter scf) {
      return byFilter(fieldNamePrefix, scf);
    }
    if (filter instanceof Filter.CompositionFilter cf) {
      return byFilter(fieldNamePrefix, cf);
    }
    throw new IllegalStateException("Unexpected filter: " + filter.getClass().getName());
  }

  default Specification<T> all() {
    return ((root, query, criteriaBuilder) ->
        criteriaBuilder.isTrue(criteriaBuilder.literal(true)));
  }

  default Specification<T> byFilter(String fieldNamePrefix, Filter.SingleConditionFilter filter) {
    // TODO: do we need to worry about time representation here?
    return byAnyCondition(fieldNamePrefix, filter.condition());
  }

  default Specification<T> byFilter(String fieldNamePrefix, Filter.CompositionFilter filter) {
    var composed = filter.filters().stream().map(f -> byFilter(fieldNamePrefix, f));
    // TODO: empty stream?
    return switch (filter.operator()) {
      case AND -> composed.reduce(Specification::and).get();
      case OR -> composed.reduce(Specification::or).get();
    };
  }
}
