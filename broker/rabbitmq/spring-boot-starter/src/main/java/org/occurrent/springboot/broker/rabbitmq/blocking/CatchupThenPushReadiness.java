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

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * The zero-config {@code readinessSource} {@link DefaultRabbitMqCloudEventBridgeFactory} pre-seeds every bridge
 * with: {@code true} for a {@link PushSubscriptionModel} no {@link CatchupThenPushSubscriptionModel} wraps, and that
 * wrapper's own {@link CatchupThenPushSubscriptionModel#isReadyForLiveDelivery(String)} for one that does.
 * <p>
 * {@link #memoized(ApplicationContext, PushSubscriptionModel)} correlates by identity, never by subscription id.
 * It looks the wrapper up in the shared {@code occurrentCatchupThenPushSubscriptionModelsByLiveFeed} bean a
 * framework {@code @Projection(source = PUSH)} or {@code @Saga(source = PUSH)} registration publishes (see that
 * module's {@code CatchupThenPushWrapperRegistry}), keyed on the exact {@link PushSubscriptionModel} instance the
 * bridge was built with. ADR 102 allows two independent {@code CatchupThenPushSubscriptionModel} instances to
 * subscribe under the same id, so an id-only lookup across every such bean in the context would answer for a bridge
 * with an unrelated model's wrapper, permanently starving a healthy bridge if that unrelated wrapper's own catch-up
 * has failed or is merely slow. Nothing here falls back to that kind of lookup. A {@link PushSubscriptionModel} the
 * registry bean does not (yet) know about, or no registry bean published at all, both answer {@code true}, the same
 * "ready" answer a model no wrapper ever touches gets. See {@code RoutingOutcome.DEFERRED} below for why a
 * false-positive "ready" here costs pacing, never correctness.
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
     * A {@code readinessSource} predicate correlated to {@code liveFeed} by identity, lazily resolved and memoized
     * once found. See the class javadoc, and {@link #wrapperFor(ApplicationContext, PushSubscriptionModel,
     * AtomicReference)} for what is and is not cached, and why.
     */
    static Predicate<String> memoized(ApplicationContext applicationContext, PushSubscriptionModel liveFeed) {
        AtomicReference<@Nullable CatchupThenPushSubscriptionModel> identityMatch = new AtomicReference<>();
        return subscriptionId -> {
            CatchupThenPushSubscriptionModel wrapper = wrapperFor(applicationContext, liveFeed, identityMatch);
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
    // polled forever. A registry bean absent altogether, a hand-built wrapper with no framework registrar in play,
    // answers null (ready) rather than guessing by subscription id, since an id-only match could land on a wrapper
    // for a different PushSubscriptionModel entirely (ADR 102 permits two wrappers sharing an id), pausing a bridge
    // that has nothing to do with it.
    private static @Nullable CatchupThenPushSubscriptionModel wrapperFor(ApplicationContext applicationContext, PushSubscriptionModel liveFeed,
                                                                          AtomicReference<@Nullable CatchupThenPushSubscriptionModel> identityMatch) {
        CatchupThenPushSubscriptionModel cached = identityMatch.get();
        if (cached != null) {
            return cached;
        }
        if (!applicationContext.containsBean(WRAPPERS_BY_LIVE_FEED_BEAN_NAME)) {
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<PushSubscriptionModel, CatchupThenPushSubscriptionModel> wrappersByLiveFeed =
                (Map<PushSubscriptionModel, CatchupThenPushSubscriptionModel>) applicationContext.getBean(WRAPPERS_BY_LIVE_FEED_BEAN_NAME, Map.class);
        CatchupThenPushSubscriptionModel byIdentity = wrappersByLiveFeed.get(liveFeed);
        if (byIdentity != null) {
            identityMatch.set(byIdentity);
        }
        return byIdentity;
    }
}
