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
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerStatus;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * One health indicator per subscription id of the {@link CompetingConsumerSubscriptionModel}s, named after the
 * subscription id, built the way ADR 171 builds the RabbitMQ one.
 * <p>
 * A subscription is down while {@link CompetingConsumerStatus#isFailing()}, which is a subscription given up after the
 * wrapped model refused to start it, one held paused after such a refusal, and one whose last call to the wrapped model
 * or the lease strategy failed with no call succeeding since. Standby, a pause by the user and a stopped model are up
 * while no call fails. When two models use the same subscription id, the indicator is down while either reports a
 * failure, and shows the details of the first model that does.
 * <p>
 * The models are found on every health call, among the {@link SubscriptionModel} beans already created, by unwrapping
 * each {@link SubscriptionModelWrapper} until a {@code CompetingConsumerSubscriptionModel} turns up. A health call
 * creates no bean.
 */
final class CompetingConsumersHealthContributor implements CompositeHealthContributor {

    private final ConfigurableListableBeanFactory beanFactory;

    CompetingConsumersHealthContributor(ConfigurableListableBeanFactory beanFactory) {
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

    // Two models can use the same subscription id, so a component asks every model that knows its id
    private Map<String, HealthIndicator> indicators() {
        Map<String, List<CompetingConsumerSubscriptionModel>> modelsById = new LinkedHashMap<>();
        for (CompetingConsumerSubscriptionModel model : models()) {
            for (CompetingConsumerStatus status : model.competingConsumerStatuses()) {
                modelsById.computeIfAbsent(status.subscriptionId(), __ -> new ArrayList<>()).add(model);
            }
        }
        Map<String, HealthIndicator> indicators = new LinkedHashMap<>();
        modelsById.forEach((subscriptionId, models) -> indicators.put(subscriptionId, () -> health(subscriptionId, models)));
        return indicators;
    }

    // No eager init and containsSingleton, so neither the type check nor the lookup creates a bean. The same model
    // can sit under two beans, a user bean wrapping the starter's for instance, so each is listed once, in the order
    // of the bean names.
    private List<CompetingConsumerSubscriptionModel> models() {
        Set<CompetingConsumerSubscriptionModel> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        List<CompetingConsumerSubscriptionModel> models = new ArrayList<>();
        for (String name : beanFactory.getBeanNamesForType(SubscriptionModel.class, false, false)) {
            if (beanFactory.containsSingleton(name)) {
                CompetingConsumerSubscriptionModel model = competingConsumerModelIn(beanFactory.getBean(name, SubscriptionModel.class));
                if (model != null && seen.add(model)) {
                    models.add(model);
                }
            }
        }
        return models;
    }

    // Reports the first failing status of the models, so a model that fails is never hidden behind one that doesn't
    private static Health health(String subscriptionId, List<CompetingConsumerSubscriptionModel> models) {
        List<CompetingConsumerStatus> statuses = models.stream()
                .map(model -> model.competingConsumerStatus(subscriptionId))
                .flatMap(Optional::stream)
                .toList();
        return statuses.stream().filter(CompetingConsumerStatus::isFailing).findFirst()
                .or(() -> statuses.stream().findFirst())
                .map(CompetingConsumersHealthContributor::health)
                .orElseGet(() -> Health.unknown().build());
    }

    private static @Nullable CompetingConsumerSubscriptionModel competingConsumerModelIn(SubscriptionModel subscriptionModel) {
        SubscriptionModel model = subscriptionModel;
        while (true) {
            if (model instanceof CompetingConsumerSubscriptionModel competingConsumerSubscriptionModel) {
                return competingConsumerSubscriptionModel;
            } else if (model instanceof SubscriptionModelWrapper wrapper) {
                model = wrapper.getWrappedSubscriptionModel();
            } else {
                return null;
            }
        }
    }

    private static Health health(CompetingConsumerStatus status) {
        Health.Builder health = (status.isFailing() ? Health.down() : Health.up())
                .withDetail("state", status.state().name())
                .withDetail("subscriberId", status.subscriberId());
        CompetingConsumerStatus.Failure failure = status.failure();
        if (failure != null) {
            health.withDetail("failure", failure.cause().getClass().getName() + ": " + failure.cause().getMessage())
                    .withDetail("since", failure.since().toString());
        }
        return health.build();
    }
}
