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

package org.occurrent.springboot.blocking;

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.internal.SingleConsumerMessages;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Records which {@code @Projection} or {@code @Saga} is registered on each live {@link PushSubscriptionModel}, and
 * publishes the {@link CatchupThenPushSubscriptionModel} wrapping each feed as a single shared bean under
 * {@link #BEAN_NAME}, so a CloudEvent-level broker source wired in a separate starter module that never depends on
 * this one (per ADR 133 decision 1) can find the wrapper for its own {@code model} argument. A wrapper's per-id bean
 * name ({@link CatchupThenPushSubscriptionModelPublisher#beanName(String)}) cannot give it that: ADR 102 allows two
 * wrapper instances to share a subscription id, and a source asking whether its feed is ready needs the wrapper
 * built on the exact {@link PushSubscriptionModel} instance it was built with.
 * <p>
 * A {@link PushSubscriptionModel} delivers to one subscription only, so a second {@code @Projection} or
 * {@code @Saga} on the same feed instance fails context refresh with an {@link IllegalArgumentException} naming both,
 * whether or not either one catches up first and whether subscriptions start on their own or manually. Without that,
 * manual start would leave the published entry pointing at whichever wrapper registered last, and a broker source
 * asking it would stay paused forever. Registering the same wrapper on the same feed again is not a second
 * registration.
 * <p>
 * Feeds are compared by identity, never with {@code equals}, both here and in the published map, since
 * {@link PushSubscriptionModel} is not final and a subclass may override {@code equals}.
 * <p>
 * Looked up by a fixed bean name rather than by type, mirroring
 * {@link CatchupThenPushSubscriptionModelPublisher#beanName(String)}'s own convention, since the starter modules
 * that read this bean have no compile-time dependency on this module to share a type or a constant through.
 */
final class CatchupThenPushWrapperRegistry {

    /**
     * The bean name a CloudEvent-level broker starter looks the wrapper map up by. Documented here as the one place
     * this convention is defined. Each starter's own {@code CatchupThenPushReadiness} duplicates the literal, since
     * neither depends on this module.
     */
    static final String BEAN_NAME = "occurrentCatchupThenPushSubscriptionModelsByLiveFeed";

    private static final String REGISTRATIONS_BEAN_NAME = "occurrentPushFeedRegistrations";

    private final Map<PushSubscriptionModel, Registration> registrationsByLiveFeed = new IdentityHashMap<>();
    // Read by the broker starters from their own threads, so every access goes through the synchronized wrapper.
    private final Map<PushSubscriptionModel, CatchupThenPushSubscriptionModel> wrappersByLiveFeed = Collections.synchronizedMap(new IdentityHashMap<>());

    private CatchupThenPushWrapperRegistry() {
    }

    /**
     * Records that the {@code annotationName} ({@code "@Projection"} or {@code "@Saga"}) registration {@code id} is
     * registered on {@code liveFeed}, and that {@code wrapper}, when not {@code null}, wraps it. A registration with no catch-up in front of the feed
     * passes {@code null}. Creates and publishes the shared beans the first time this is called for
     * {@code applicationContext}.
     * <p>
     * A no-op when {@code applicationContext} is not a {@link ConfigurableApplicationContext}, matching
     * {@link CatchupThenPushSubscriptionModelPublisher#publish}'s own fallback for the same exotic-harness case. A
     * second registration on the same feed is then refused when it subscribes instead, and a broker source's
     * readiness lookup finds no wrapper and answers ready.
     *
     * @throws IllegalArgumentException if a different registration is already recorded on {@code liveFeed}
     */
    static void register(ApplicationContext applicationContext, String annotationName, String id, PushSubscriptionModel liveFeed, @Nullable CatchupThenPushSubscriptionModel wrapper) {
        if (!(applicationContext instanceof ConfigurableApplicationContext configurableContext)) {
            return;
        }
        ConfigurableListableBeanFactory beanFactory = configurableContext.getBeanFactory();
        registryFrom(beanFactory).record(beanFactory, new Registration(annotationName, id, wrapper), liveFeed);
    }

    private synchronized void record(ConfigurableListableBeanFactory beanFactory, Registration registration, PushSubscriptionModel liveFeed) {
        Registration existing = registrationsByLiveFeed.get(liveFeed);
        if (existing != null) {
            if (existing.annotationName().equals(registration.annotationName()) && existing.id().equals(registration.id()) && existing.wrapper() == registration.wrapper()) {
                return;
            }
            String feed = feedBeanName(beanFactory, liveFeed).map(name -> "push feed bean '" + name + "'").orElse("its push feed");
            throw new IllegalArgumentException("Cannot register %s '%s' on %s. %s".formatted(registration.annotationName(), registration.id(), feed,
                    SingleConsumerMessages.singleConsumerOnly(PushSubscriptionModel.class.getSimpleName(), existing.annotationName(), existing.id(), registration.id())));
        }
        registrationsByLiveFeed.put(liveFeed, registration);
        if (registration.wrapper() != null) {
            wrappersByLiveFeed.put(liveFeed, registration.wrapper());
        }
    }

    // Only asked on the failure path, so walking every candidate singleton costs nothing on a normal startup.
    private static Optional<String> feedBeanName(ConfigurableListableBeanFactory beanFactory, PushSubscriptionModel liveFeed) {
        return Arrays.stream(beanFactory.getBeanNamesForType(PushSubscriptionModel.class, true, false))
                .filter(name -> beanFactory.containsSingleton(name) && beanFactory.getSingleton(name) == liveFeed)
                .findFirst();
    }

    private static CatchupThenPushWrapperRegistry registryFrom(ConfigurableListableBeanFactory beanFactory) {
        // Spring's singleton registry does not serialize a check-then-register pair, so two registrars calling this
        // at once could otherwise each create a registry and fail on the second registerSingleton.
        synchronized (beanFactory) {
            if (!beanFactory.containsSingleton(REGISTRATIONS_BEAN_NAME)) {
                CatchupThenPushWrapperRegistry registry = new CatchupThenPushWrapperRegistry();
                beanFactory.registerSingleton(REGISTRATIONS_BEAN_NAME, registry);
                beanFactory.registerSingleton(BEAN_NAME, registry.wrappersByLiveFeed);
            }
            return (CatchupThenPushWrapperRegistry) beanFactory.getSingleton(REGISTRATIONS_BEAN_NAME);
        }
    }

    private record Registration(String annotationName, String id, @Nullable CatchupThenPushSubscriptionModel wrapper) {
    }
}
