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

package org.occurrent.springboot.broker.rabbitmq.blocking;

import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.springframework.context.ApplicationContext;

import java.util.Collection;

/**
 * The zero-config {@code readinessSource} {@link DefaultRabbitMqCloudEventBridgeFactory} pre-seeds every bridge
 * with: {@code true} for a subscription id no {@link CatchupThenPushSubscriptionModel} bean claims, and that bean's
 * own {@link CatchupThenPushSubscriptionModel#isReadyForLiveDelivery(String)} for one that does.
 * <p>
 * Looked up by type and ownership, whether {@link CatchupThenPushSubscriptionModel#subscriptionIds()} contains the
 * id, rather than by the {@code "catchupThenPushSubscriptionModel-<id>"} bean-name convention the framework's
 * separate autoconfigure module owns, so this starter stays decoupled from that module, per ADR 133 decision 1.
 * <p>
 * {@code readinessSource} is a pacing hint only, never a correctness dependency: {@code RoutingOutcome.DEFERRED}
 * is what a bridge falls back to for an event that arrives before catch-up is actually done, whatever this method
 * answered. A wrapper bean not yet published this early in startup, or no catch-up wrapper involved at all, both
 * default to {@code true} here and stay correct either way, just possibly noisier until the answer catches up.
 */
final class CatchupThenPushReadiness {

    private CatchupThenPushReadiness() {
    }

    static boolean isReady(ApplicationContext applicationContext, String subscriptionId) {
        Collection<CatchupThenPushSubscriptionModel> wrappers = applicationContext.getBeansOfType(CatchupThenPushSubscriptionModel.class).values();
        for (CatchupThenPushSubscriptionModel wrapper : wrappers) {
            if (wrapper.subscriptionIds().contains(subscriptionId)) {
                return wrapper.isReadyForLiveDelivery(subscriptionId);
            }
        }
        return true;
    }
}
