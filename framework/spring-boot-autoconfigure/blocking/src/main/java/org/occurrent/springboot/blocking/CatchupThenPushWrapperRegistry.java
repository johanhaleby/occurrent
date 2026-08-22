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

import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps a live {@link PushSubscriptionModel} instance to the {@link CatchupThenPushSubscriptionModel} wrapping it,
 * published as a single shared bean under {@link #BEAN_NAME} rather than one per registration, so a CloudEvent-level
 * broker bridge wired in a separate starter module that never depends on this one (per ADR 133 decision 1) can
 * correlate a bridge's own {@code model} argument to its wrapper by identity. That correlation is what a wrapper's
 * per-id bean name ({@link CatchupThenPushSubscriptionModelPublisher#beanName(String)}) alone cannot give it: ADR
 * 102 allows two wrapper instances to share a subscription id, and a bridge asking "is my model's own wrapper
 * ready" needs the one wrapping the exact {@link PushSubscriptionModel} it was built with, not whichever bean the
 * id happens to name first.
 * <p>
 * {@link PushSubscriptionModel} carries no {@code equals}/{@code hashCode} override, so a plain
 * {@link ConcurrentHashMap} keyed on it is already identity-keyed, exactly the correlation a bridge needs.
 * <p>
 * Looked up by a fixed bean name rather than by type, mirroring
 * {@link CatchupThenPushSubscriptionModelPublisher#beanName(String)}'s own convention, since the starter modules
 * that read this bean have no compile-time dependency on this module to share a type or a constant through.
 */
final class CatchupThenPushWrapperRegistry {

    /**
     * The bean name a CloudEvent-level broker starter looks this map up by. Documented here as the one place this
     * convention is defined; each starter's own {@code CatchupThenPushReadiness} duplicates the literal, since
     * neither depends on this module.
     */
    static final String BEAN_NAME = "occurrentCatchupThenPushSubscriptionModelsByLiveFeed";

    private CatchupThenPushWrapperRegistry() {
    }

    /**
     * Records that {@code wrapper} wraps {@code liveFeed}, creating and publishing the shared map bean the first
     * time this is called for {@code applicationContext}. A no-op, rather than a failure, when
     * {@code applicationContext} is not a {@link ConfigurableApplicationContext}, matching
     * {@link CatchupThenPushSubscriptionModelPublisher#publish}'s own fallback for the same exotic-harness case:
     * the catch-up itself keeps running fine either way, only a healthy sibling bridge's readiness lookup loses the
     * benefit of this correlation and falls back to always-ready.
     */
    static void register(ApplicationContext applicationContext, PushSubscriptionModel liveFeed, CatchupThenPushSubscriptionModel wrapper) {
        if (!(applicationContext instanceof ConfigurableApplicationContext configurableContext)) {
            return;
        }
        registryFrom(configurableContext.getBeanFactory()).put(liveFeed, wrapper);
    }

    @SuppressWarnings("unchecked")
    private static Map<PushSubscriptionModel, CatchupThenPushSubscriptionModel> registryFrom(ConfigurableListableBeanFactory beanFactory) {
        if (!beanFactory.containsSingleton(BEAN_NAME)) {
            // A benign race between two concurrent first calls can attempt to register the map twice; Spring's
            // singleton registry itself is what actually serializes registerSingleton, so the loser's own
            // ConcurrentHashMap is simply discarded, unused, rather than causing a duplicate-bean failure the way
            // CatchupThenPushSubscriptionModelPublisher's per-id bean names deliberately do.
            synchronized (beanFactory) {
                if (!beanFactory.containsSingleton(BEAN_NAME)) {
                    beanFactory.registerSingleton(BEAN_NAME, new ConcurrentHashMap<PushSubscriptionModel, CatchupThenPushSubscriptionModel>());
                }
            }
        }
        return (Map<PushSubscriptionModel, CatchupThenPushSubscriptionModel>) beanFactory.getSingleton(BEAN_NAME);
    }
}
