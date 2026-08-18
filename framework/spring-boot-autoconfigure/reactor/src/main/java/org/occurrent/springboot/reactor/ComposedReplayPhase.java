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

package org.occurrent.springboot.reactor;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.projection.ReplayPhase;
import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The holder the default reactive Mongo auto-configuration fills, working around the gap
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>
 * decision 8 names: the reactor stack's plain capability lookup is a direct {@code instanceof} against the bean
 * asked, so it cannot see a {@code ReactorCatchupSubscriptionModel} composed one level inside the
 * {@code @Primary} durable model {@code OccurrentReactiveMongoAutoConfiguration.occurrentDurableSubscriptionModel}
 * exposes. Whoever built that composition already held the inner model before wrapping it, and hands it here.
 * <p>
 * A bean of this type, {@link #suppliedBy(Object)} called once during that bean's own construction, and read
 * afterwards through {@link #forSubscription(String)} by the reactor
 * {@code @Projection(recordAppliedAppends = true)} registrar. {@code public} for the same reason
 * {@code PushCatchupStatusImpl} is: the auto-configuration and the registrar that use it live in different packages.
 * <p>
 * <a href="https://github.com/johanhaleby/occurrent/issues/842">#842</a> tracks fixing the underlying capability-lookup
 * gap. This class is the workaround ADR 132 sanctions until then, not a replacement for it.
 */
@NullMarked
public final class ComposedReplayPhase {

    private volatile @Nullable ReplayAwareSubscriptions replayAware;

    /**
     * Supplies the composed subscription model this instance answers for, {@code instanceof}-checked against
     * {@link ReplayAwareSubscriptions} once, here, rather than on every {@link #forSubscription(String)} call: the
     * composition is fixed once this bean is built, so there is nothing to re-resolve later. Called at most once,
     * from the bean method that composed {@code composedModel}.
     * <p>
     * Takes {@link Object} rather than a narrower capability marker, since what a catch-up composition returns
     * (a {@code CheckpointAwareSubscriptionModel} on the reactor stack) is not itself typed as one. Only some of its
     * possible concrete shapes, {@code ReactorCatchupSubscriptionModel} among them, are.
     */
    public void suppliedBy(Object composedModel) {
        requireNonNull(composedModel, "composedModel cannot be null");
        this.replayAware = composedModel instanceof ReplayAwareSubscriptions replayAwareSubscriptions ? replayAwareSubscriptions : null;
    }

    /**
     * A {@link ReplayPhase} answering for {@code subscriptionId} against the composition {@link #suppliedBy} was
     * given, or empty when {@link #suppliedBy} was never called or the composition cannot say whether it is
     * replaying (no catch-up layer). Empty is the caller's cue to fall back to another phase source, ultimately
     * {@link ReplayPhase#neverReplays()} with the warning ADR 132 decision 2 requires.
     */
    public Optional<ReplayPhase> forSubscription(String subscriptionId) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        ReplayAwareSubscriptions capability = replayAware;
        if (capability == null) {
            return Optional.empty();
        }
        return Optional.of(() -> capability.isCatchingUp(subscriptionId));
    }
}
