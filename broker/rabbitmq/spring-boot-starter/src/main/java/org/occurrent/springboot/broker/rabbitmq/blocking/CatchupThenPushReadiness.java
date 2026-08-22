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

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.context.ApplicationContext;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * The zero-config {@code readinessSource} {@link DefaultRabbitMqCloudEventBridgeFactory} pre-seeds every bridge
 * with: {@code true} for a {@link PushSubscriptionModel} no {@link CatchupThenPushSubscriptionModel} wraps, and that
 * wrapper's own {@link CatchupThenPushSubscriptionModel#isReadyForLiveDelivery(String)} for one that does.
 * <p>
 * {@link #memoized(ApplicationContext, PushSubscriptionModel)} is what {@link DefaultRabbitMqCloudEventBridgeFactory}
 * actually uses, and correlates by identity: it looks the wrapper up in the shared
 * {@code occurrentCatchupThenPushSubscriptionModelsByLiveFeed} bean a framework {@code @Projection(source = PUSH)}
 * or {@code @Saga(source = PUSH)} registration publishes (see that module's {@code CatchupThenPushWrapperRegistry}),
 * keyed on the exact {@link PushSubscriptionModel} instance the bridge was built with, never on subscription id
 * alone. ADR 102 allows two independent {@code CatchupThenPushSubscriptionModel} instances to subscribe under the
 * same id, so an id-only lookup across every such bean in the context, {@link #isReady(ApplicationContext, String)}
 * below, can answer for a bridge with an unrelated model's wrapper, permanently starving a healthy bridge if that
 * unrelated wrapper's own catch-up has failed. That id-based scan is used as a fallback only when the registry bean
 * does not exist in the context at all, a hand-built wrapper with no framework registrar in play. Once the registry
 * bean exists, its own answer for this exact {@link PushSubscriptionModel} is authoritative, "not (yet) in it" and
 * "definitely not wrapped" both answered the same way (ready), rather than falling through to a scan that could
 * still land on an unrelated wrapper's answer despite an authoritative source already being available.
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
     * reference in hand. {@link DefaultRabbitMqCloudEventBridgeFactory} itself no longer uses this, it asks
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
     * once found. See the class javadoc, and {@link #wrapperFor(ApplicationContext, PushSubscriptionModel, String,
     * AtomicReference)} for what is and is not cached, and why.
     */
    static Predicate<String> memoized(ApplicationContext applicationContext, PushSubscriptionModel liveFeed) {
        AtomicReference<@Nullable CatchupThenPushSubscriptionModel> identityMatch = new AtomicReference<>();
        return subscriptionId -> {
            CatchupThenPushSubscriptionModel wrapper = wrapperFor(applicationContext, liveFeed, subscriptionId, identityMatch);
            return wrapper == null || wrapper.isReadyForLiveDelivery(subscriptionId);
        };
    }

    // Only a positive identity match, from the shared registry, is ever memoized into identityMatch: once the
    // framework registrar has published this exact liveFeed's own wrapper, that mapping is never later withdrawn
    // or replaced, so caching it is always safe and skips the registry lookup on every later poll. A caller with
    // no bean in hand yet (the registry bean does not exist at all) or no entry for liveFeed specifically (not
    // published yet, since bean initialization order is not guaranteed, or genuinely never wrapped) is never
    // cached, deliberately: a "not found" answer taken while the registry has not caught up yet, and cached
    // forever, would starve a bridge of ever seeing the real wrapper once it does appear. Re-resolved fresh, from
    // the map, on every call instead, cheap enough (one HashMap lookup) that this costs nothing meaningful even
    // polled forever. Once the registry bean exists at all, its answer for liveFeed is authoritative and final for
    // that poll. The id-scan fallback below never runs, since guessing through it while an authoritative source
    // says "not wrapped" risks matching a different, unrelated wrapper that merely shares this subscription id
    // (ADR 102 permits exactly that), the same ambiguity the identity registry exists to resolve in the first
    // place. The id-scan only ever runs when the registry bean is absent outright, a hand-built wrapper with no
    // framework registrar in play at all, the shape CatchupThenPushReadinessTest and the auto-configuration
    // integration test both exercise. Its own answer is never memoized either, since it is only ever a guess.
    @SuppressWarnings("unchecked")
    private static @Nullable CatchupThenPushSubscriptionModel wrapperFor(ApplicationContext applicationContext, PushSubscriptionModel liveFeed,
                                                                          String subscriptionId, AtomicReference<@Nullable CatchupThenPushSubscriptionModel> identityMatch) {
        CatchupThenPushSubscriptionModel cached = identityMatch.get();
        if (cached != null) {
            return cached;
        }
        if (applicationContext.containsBean(WRAPPERS_BY_LIVE_FEED_BEAN_NAME)) {
            Map<PushSubscriptionModel, CatchupThenPushSubscriptionModel> wrappersByLiveFeed =
                    (Map<PushSubscriptionModel, CatchupThenPushSubscriptionModel>) applicationContext.getBean(WRAPPERS_BY_LIVE_FEED_BEAN_NAME, Map.class);
            CatchupThenPushSubscriptionModel byIdentity = wrappersByLiveFeed.get(liveFeed);
            if (byIdentity != null) {
                identityMatch.set(byIdentity);
            }
            return byIdentity;
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
