package org.occurrent.eventstore.jpa;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.NoRepositoryBean;

@NoRepositoryBean
public interface JPAEventLog<T extends CloudEventDaoTraits, TKey>
    extends JpaRepository<T, TKey>, JpaSpecificationExecutor<T> {}
