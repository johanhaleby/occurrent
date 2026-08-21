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

package org.occurrent.springboot.broker.kafka.blocking;

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.context.ApplicationContext;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * The zero-config {@code readinessSource} {@link DefaultKafkaCloudEventBridgeFactory} pre-seeds every bridge with:
 * {@code true} for a {@link PushSubscriptionModel} no {@link CatchupThenPushSubscriptionModel} wraps, and that
 * wrapper's own {@link CatchupThenPushSubscriptionModel#isReadyForLiveDelivery(String)} for one that does.
 * <p>
 * {@link #memoized(ApplicationContext, PushSubscriptionModel)} is what {@link DefaultKafkaCloudEventBridgeFactory}
 * actually uses, and correlates by identity: it looks the wrapper up in the shared
 * {@code occurrentCatchupThenPushSubscriptionModelsByLiveFeed} bean a framework {@code @Projection(source = PUSH)}
 * or {@code @Saga(source = PUSH)} registration publishes (see that module's {@code CatchupThenPushWrapperRegistry}),
 * keyed on the exact {@link PushSubscriptionModel} instance the bridge was built with, never on subscription id
 * alone. ADR 102 allows two independent {@code CatchupThenPushSubscriptionModel} instances to subscribe under the
 * same id, so an id-only lookup across every such bean in the context, {@link #isReady(ApplicationContext, String)}
 * below, can answer for a bridge with an unrelated model's wrapper, permanently starving a healthy bridge if that
 * unrelated wrapper's own catch-up has failed. When no identity match exists, this falls back to that same id-based
 * scan, but only when it is itself unambiguous, exactly one wrapper bean claims the id, the shape a wrapper built
 * and registered by hand outside the framework registrar still takes. Two or more claimants is the very ambiguity
 * this correlation exists to resolve, so guessing among them is refused the same way no claimant at all is: both
 * default to ready.
 * <p>
 * Looked up by a fixed bean name rather than a shared type, since this starter has no compile-time dependency on the
 * framework autoconfigure module that publishes it, per ADR 133 decision 1's deliberate decoupling. Resolved lazily,
 * on first use, and memoized once found, never eagerly at bridge-build time, so a wrapper the framework module
 * publishes after this bridge is already built (bean initialization order is not guaranteed) is still picked up the
 * first time a live event actually asks.
 * <p>
 * {@code readinessSource} is a pacing hint only, never a correctness dependency: {@code RoutingOutcome.DEFERRED}
 * is what a bridge falls back to for an event that arrives before catch-up is actually done, whatever this method
 * answered. A wrapper bean not yet published this early in startup, or no catch-up wrapper involved at all, both
 * default to {@code true} here and stay correct either way, just possibly noisier until the answer catches up.
 */
final class CatchupThenPushReadiness {

    private static final String WRAPPERS_BY_LIVE_FEED_BEAN_NAME = "occurrentCatchupThenPushSubscriptionModelsByLiveFeed";

    private CatchupThenPushReadiness() {
    }

    /**
     * The id-only lookup: {@code true} for a subscription id no {@link CatchupThenPushSubscriptionModel} bean in
     * the whole context claims, and the first such bean's own answer for one that does, in whatever order
     * {@link ApplicationContext#getBeansOfType(Class)} returns them. Ambiguous whenever two wrappers share an id
     * (ADR 102 permits exactly that), see the class javadoc. Kept for a caller with no {@link PushSubscriptionModel}
     * reference in hand. {@link DefaultKafkaCloudEventBridgeFactory} itself no longer uses this, it asks
     * {@link #memoized(ApplicationContext, PushSubscriptionModel)} instead, so it never crosses model identity.
     */
    static boolean isReady(ApplicationContext applicationContext, String subscriptionId) {
        Collection<CatchupThenPushSubscriptionModel> wrappers = applicationContext.getBeansOfType(CatchupThenPushSubscriptionModel.class).values();
        for (CatchupThenPushSubscriptionModel wrapper : wrappers) {
            if (wrapper.subscriptionIds().contains(subscriptionId)) {
                return wrapper.isReadyForLiveDelivery(subscriptionId);
            }
        }
        return true;
    }

    /**
     * A {@code readinessSource} predicate correlated to {@code liveFeed} by identity, lazily resolved and memoized
     * once found. See the class javadoc.
     */
    static Predicate<String> memoized(ApplicationContext applicationContext, PushSubscriptionModel liveFeed) {
        AtomicReference<@Nullable CatchupThenPushSubscriptionModel> resolved = new AtomicReference<>();
        return subscriptionId -> {
            CatchupThenPushSubscriptionModel wrapper = resolved.get();
            if (wrapper == null) {
                wrapper = wrapperFor(applicationContext, liveFeed, subscriptionId);
                if (wrapper != null) {
                    resolved.set(wrapper);
                }
            }
            return wrapper == null || wrapper.isReadyForLiveDelivery(subscriptionId);
        };
    }

    // Identity match first, from the shared registry: the correct, unambiguous answer whenever the framework
    // module published this exact liveFeed's own wrapper. Falling back to the id-based scan below, but only when
    // it is itself unambiguous (exactly one CatchupThenPushSubscriptionModel bean in the whole context claims this
    // id), covers a wrapper built and registered by hand, outside the framework registrar, the same shape
    // CatchupThenPushReadinessTest and the auto-configuration integration test both exercise. Two or more beans
    // claiming the same id is precisely the ambiguity this identity correlation exists to resolve. Guessing among
    // them would reintroduce it, so that case, and no claimant at all, both default to ready instead.
    @SuppressWarnings("unchecked")
    private static @Nullable CatchupThenPushSubscriptionModel wrapperFor(ApplicationContext applicationContext, PushSubscriptionModel liveFeed, String subscriptionId) {
        if (applicationContext.containsBean(WRAPPERS_BY_LIVE_FEED_BEAN_NAME)) {
            Map<PushSubscriptionModel, CatchupThenPushSubscriptionModel> wrappersByLiveFeed =
                    (Map<PushSubscriptionModel, CatchupThenPushSubscriptionModel>) applicationContext.getBean(WRAPPERS_BY_LIVE_FEED_BEAN_NAME, Map.class);
            CatchupThenPushSubscriptionModel byIdentity = wrappersByLiveFeed.get(liveFeed);
            if (byIdentity != null) {
                return byIdentity;
            }
        }
        CatchupThenPushSubscriptionModel unambiguousClaimant = null;
        for (CatchupThenPushSubscriptionModel wrapper : applicationContext.getBeansOfType(CatchupThenPushSubscriptionModel.class).values()) {
            if (wrapper.subscriptionIds().contains(subscriptionId)) {
                if (unambiguousClaimant != null) {
                    return null;
                }
                unambiguousClaimant = wrapper;
            }
        }
        return unambiguousClaimant;
    }
}
