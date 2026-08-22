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
import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.reactor.SubscriptionModelCapability;
import org.springframework.aop.framework.AopProxyUtils;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The holder the default reactive Mongo auto-configuration fills, working around the gap
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>
 * decision 8 names. The reactor stack's plain capability lookup is a direct {@code instanceof} against the bean
 * asked, so it cannot see a {@code ReactorCatchupSubscriptionModel} composed one level inside the
 * {@code @Primary} durable model {@code OccurrentReactiveMongoAutoConfiguration.occurrentDurableSubscriptionModel}
 * exposes. Whoever built that composition already held the inner model before wrapping it, and hands it here.
 * <p>
 * A bean of this type, {@link #suppliedBy(Object)} called once during that bean's own construction, and read
 * afterwards through {@link #catchupModel()} by the reactor {@code @Projection(recordAppliedAppends = true)}
 * registrar. {@code public} for the same reason {@code PushCatchupStatusImpl} is, the auto-configuration and the
 * registrar that use it live in different packages.
 * <p>
 * <a href="https://github.com/johanhaleby/occurrent/issues/842">#842</a> tracks fixing the underlying capability-lookup
 * gap. This class is the workaround ADR 132 sanctions until then, not a replacement for it.
 */
@NullMarked
public final class ComposedCatchupModel {

    // Distinct from replayAware being null: that also happens when suppliedBy was given a composition with no
    // catch-up layer at all, a known fact rather than an unknown one, and the two must not read the same.
    private volatile boolean supplied = false;
    private volatile @Nullable ReplayAwareSubscriptions replayAware;
    private volatile boolean defaultBypassesCatchup = false;
    private volatile @Nullable SubscriptionModelCapability composedModel;

    /**
     * Supplies the composed subscription model this instance answers for, {@code instanceof}-checked against
     * {@link ReplayAwareSubscriptions} once, here, rather than on every {@link #catchupModel()} call. The
     * composition is fixed once this bean is built, so there is nothing to re-resolve later. Called at most once,
     * from the bean method that composed {@code composedModel}.
     * <p>
     * Takes {@link Object} rather than a narrower capability marker, since what a catch-up composition returns
     * (a {@code CheckpointAwareSubscriptionModel} on the reactor stack) is not itself typed as one. Only some of its
     * possible concrete shapes, {@code ReactorCatchupSubscriptionModel} among them, are.
     */
    public void suppliedBy(Object composedModel) {
        requireNonNull(composedModel, "composedModel cannot be null");
        this.supplied = true;
        this.replayAware = composedModel instanceof ReplayAwareSubscriptions replayAwareSubscriptions ? replayAwareSubscriptions : null;
    }

    /**
     * The composition {@link #suppliedBy} was given, when it can say what its catch-ups are doing. Empty when it was
     * given a composition that cannot, a store with no catch-up layer at all (ADR 132 decision 9), and also empty
     * when {@link #suppliedBy} was never called. Those two are different facts, and {@link #isSupplied()} tells them
     * apart. The first is a known thing about that composition, the second is the caller's cue to fall back to
     * another source, ultimately to the warning decision 2 requires for a composition nothing can see into.
     */
    public Optional<ReplayAwareSubscriptions> catchupModel() {
        return Optional.ofNullable(replayAware);
    }

    /**
     * Whether {@link #suppliedBy} was called at all.
     */
    public boolean isSupplied() {
        return supplied;
    }

    /**
     * Records, as a known fact, that {@link org.occurrent.annotation.StartPosition#DEFAULT} bypasses this
     * composition's catch-up layer unconditionally. {@code StartAt.subscriptionModelDefault()} never replays here,
     * the checkpoint is never consulted, so a wiped checkpoint changes nothing (ADR 132 decision 7). Called at most
     * once, by the same bean method that calls {@link #suppliedBy} and {@link #identifiedAs}, since only the
     * auto-configuration that composed this model actually knows how it resolves that marker. Never inferred here,
     * and never assumed true for a composition an application supplied itself, whose own {@code DEFAULT} semantics
     * are its own to declare.
     */
    public void defaultBypassesCatchup() {
        this.defaultBypassesCatchup = true;
    }

    /**
     * Identifies this holder with {@code durableModel}, the exact bean the auto-configuration's
     * {@code occurrentDurableSubscriptionModel} method returns, and the same bean every DSL wrapping it (or a bare
     * {@code getBean(FluxSubscriptionModel.class)} lookup) resolves to. {@link #isDefaultKnownLiveOnlyFor} compares
     * a projection's own model against this reference, not against {@link #suppliedBy}'s {@code catchupLayer}: this
     * stack's capability lookup is a direct {@code instanceof} with no unwrap (ADR 132 decision 8), so a projection
     * never actually sees {@code catchupLayer}, only the durable wrapper around it. Typed as
     * {@link SubscriptionModelCapability} rather than {@link Object}, the same type
     * {@link #isDefaultKnownLiveOnlyFor} compares it against, since the durable model always satisfies it. Called at
     * most once, by the same bean method that calls {@link #suppliedBy} and {@link #defaultBypassesCatchup()}.
     */
    public void identifiedAs(SubscriptionModelCapability durableModel) {
        this.composedModel = ultimateTarget(requireNonNull(durableModel, "durableModel cannot be null"));
    }

    /**
     * Whether {@code candidate}, the model a particular projection actually runs on, is the exact composition
     * {@link #identifiedAs} was given and {@link #defaultBypassesCatchup()} was recorded for. {@code false} until
     * both were called, and {@code false} for any composition that is not that same instance, including one an
     * application supplied itself by replacing the durable model or the {@code FluxSubscriptionModel} it also
     * satisfies. Both sides are unwrapped to their ultimate AOP target first (a fixed-singleton proxy only, the same
     * rule {@code SubscriptionAnnotations.invokeDescriptorFactory} follows), since {@code identifiedAs} runs inside
     * the {@code @Bean} method with the raw target, while a later {@code getBean} lookup can return a proxy around
     * it. A warning keyed on this answers honestly rather than by inferring composition-specific behavior it cannot
     * verify.
     */
    public boolean isDefaultKnownLiveOnlyFor(@Nullable SubscriptionModelCapability candidate) {
        return defaultBypassesCatchup && candidate != null && ultimateTarget(candidate) == composedModel;
    }

    // Unwraps through any number of nested AOP proxies to the innermost fixed target (AopProxyUtils.getSingletonTarget
    // stops at one layer, hence the loop), mirroring SubscriptionAnnotations.ultimateTarget. Returns model itself when
    // it is not a proxy, when a proxy's TargetSource is not a fixed singleton (a prototype- or pool-scoped source is
    // compared as the proxy it is rather than risking a side-effecting getTarget() call), or when the next layer in
    // does not itself implement SubscriptionModelCapability, which happens when a proxy adds that capability through
    // an introduction its target never had. Unwrapping past that layer would compare against an object this method
    // could not honestly return as one, so it stops one layer short instead.
    private static SubscriptionModelCapability ultimateTarget(SubscriptionModelCapability model) {
        Object current = model;
        Object next;
        while ((next = AopProxyUtils.getSingletonTarget(current)) != null && next instanceof SubscriptionModelCapability) {
            current = next;
        }
        return (SubscriptionModelCapability) current;
    }
}
