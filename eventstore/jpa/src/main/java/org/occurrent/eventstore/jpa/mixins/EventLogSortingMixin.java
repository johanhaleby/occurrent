package org.occurrent.eventstore.jpa.mixins;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Order;
import jakarta.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;
import org.occurrent.eventstore.api.SortBy;
import org.springframework.data.jpa.domain.Specification;

/**
 * Maps instances of {@link SortBy} to instances of {@link Specification}. This interface is
 * completely implemented but consumers can override it if they desire.
 *
 * @param <T> the concrete type that the cloud event is stored at. This is a hibernate managed
 *     entity.
 */
public interface EventLogSortingMixin<T> extends EventLogExpressionMixin<T> {
  @FunctionalInterface
  interface EventLogSort<T> {
    static <U> BinaryOperator<EventLogSort<U>> reducer() {
      return (a, b) ->
          (root, query, criteriaBuilder) -> {
            var aList = a.apply(root, query, criteriaBuilder);
            var bList = b.apply(root, query, criteriaBuilder);
            var retVal = new ArrayList<Order>();
            retVal.addAll(aList);
            retVal.addAll(bList);
            return retVal;
          };
    }

    List<Order> apply(Root<T> root, CriteriaQuery<?> query, CriteriaBuilder cb);
  }

  EventLogSort<T> defaultSort(SortBy.NaturalImpl sort);

  default EventLogSort<T> sorted(SortBy.SingleFieldImpl sort) {
    return (root, query, criteriaBuilder) ->
        switch (sort.direction) {
          case ASCENDING -> List.of(criteriaBuilder.asc(expressFieldName(root, sort.fieldName)));
          case DESCENDING -> List.of(criteriaBuilder.desc(expressFieldName(root, sort.fieldName)));
        };
  }

  default EventLogSort<T> sorted(SortBy.MultipleSortStepsImpl sort) {
    return sort.steps.stream()
        .map(this::sorted)
        .reduce((a, b) -> EventLogSort.<T>reducer().apply(a, b))
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Internal error: Expecting "
                        + SortBy.MultipleSortStepsImpl.class.getSimpleName()
                        + " to have at least one step"));
  }

  default EventLogSort<T> sorted(SortBy sort) {
    if (sort instanceof SortBy.NaturalImpl n) {
      return defaultSort(n);
    }
    if (sort instanceof SortBy.SingleFieldImpl s) {
      return sorted(s);
    }
    if (sort instanceof SortBy.MultipleSortStepsImpl m) {
      return sorted(m);
    }
    throw new IllegalArgumentException(
        "Internal error: Unrecognized "
            + SortBy.class.getSimpleName()
            + " instance: "
            + sort.getClass().getSimpleName());
  }
}
