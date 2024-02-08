package org.occurrent.eventstore.jpa.batteries;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import org.springframework.data.jpa.domain.Specification;

public record CloudEventDaoDataSpecification() implements Specification<CloudEventDao> {
  @Override
  public Predicate toPredicate(
      Root<CloudEventDao> root, CriteriaQuery<?> query, CriteriaBuilder criteriaBuilder) {

    return null;
  }
}
