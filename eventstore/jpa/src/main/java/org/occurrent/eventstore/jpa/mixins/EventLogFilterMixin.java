package org.occurrent.eventstore.jpa.mixins;

import java.util.function.Supplier;
import org.occurrent.filter.Filter;
import org.springframework.data.jpa.domain.Specification;

/**
 * Maps instances of {@link Filter} to instances of {@link Specification}. This interface is
 * completely implemented but consumers can override it if they desire.
 *
 * @param <T> the concrete type that the cloud event is stored at. This is a hibernate managed
 *     entity.
 */
public interface EventLogFilterMixin<T> extends EventLogConditionMixin<T> {
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

  Supplier<IllegalArgumentException> emptyCompositionFilterException =
      () -> new IllegalArgumentException("Expected composition filter to have at least one filter");

  default Specification<T> byFilter(String fieldNamePrefix, Filter.CompositionFilter filter) {
    var composed = filter.filters().stream().map(f -> byFilter(fieldNamePrefix, f));
    return switch (filter.operator()) {
      case AND -> composed.reduce(Specification::and).orElseThrow(emptyCompositionFilterException);
      case OR -> composed.reduce(Specification::or).orElseThrow(emptyCompositionFilterException);
    };
  }
}
