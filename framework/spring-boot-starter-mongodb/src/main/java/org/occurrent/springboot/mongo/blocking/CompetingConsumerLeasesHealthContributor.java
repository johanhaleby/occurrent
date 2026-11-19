/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.springboot.mongo.blocking;

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.mongodb.blocking.ccs.LeaseRefreshFailure;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * One health indicator per {@link SpringMongoLeaseCompetingConsumerStrategy} bean, named after the bean, built the
 * way ADR 171 builds the RabbitMQ one.
 * <p>
 * A strategy is down while {@link SpringMongoLeaseCompetingConsumerStrategy#leaseRefreshFailure()} has a failure,
 * which covers a database that answers reads and refuses writes. Spring Boot's own MongoDB indicator stays up then,
 * while no node can take a lease and every subscription waits.
 * <p>
 * The beans are looked up on every health call, and only those already created are listed, so a health call creates
 * no bean.
 */
final class CompetingConsumerLeasesHealthContributor implements CompositeHealthContributor {

    private final ConfigurableListableBeanFactory beanFactory;

    CompetingConsumerLeasesHealthContributor(ConfigurableListableBeanFactory beanFactory) {
        this.beanFactory = requireNonNull(beanFactory, "beanFactory cannot be null");
    }

    @Override
    public @Nullable HealthContributor getContributor(String name) {
        return indicators().get(name);
    }

    @Override
    public Stream<Entry> stream() {
        return indicators().entrySet().stream().map(entry -> new Entry(entry.getKey(), entry.getValue()));
    }

    // No eager init and containsSingleton, so neither the type check nor the lookup creates a bean
    private Map<String, HealthIndicator> indicators() {
        Map<String, HealthIndicator> indicators = new LinkedHashMap<>();
        for (String name : beanFactory.getBeanNamesForType(SpringMongoLeaseCompetingConsumerStrategy.class, false, false)) {
            if (beanFactory.containsSingleton(name)) {
                SpringMongoLeaseCompetingConsumerStrategy strategy = beanFactory.getBean(name, SpringMongoLeaseCompetingConsumerStrategy.class);
                indicators.put(name, () -> strategy.leaseRefreshFailure()
                        .map(CompetingConsumerLeasesHealthContributor::down)
                        .orElseGet(() -> Health.up().build()));
            }
        }
        return indicators;
    }

    private static Health down(LeaseRefreshFailure failure) {
        return Health.down()
                .withDetail("failure", failure.cause().getClass().getName() + ": " + failure.cause().getMessage())
                .withDetail("since", failure.since().toString())
                .build();
    }
}
