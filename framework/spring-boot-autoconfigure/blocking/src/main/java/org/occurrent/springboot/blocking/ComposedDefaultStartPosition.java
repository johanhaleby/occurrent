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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;
import org.springframework.aop.framework.AopProxyUtils;

import static java.util.Objects.requireNonNull;

/**
 * The holder the default blocking Mongo auto-configuration fills when it knows its own composed subscription
 * model's {@code StartPosition.DEFAULT} bypasses the catch-up layer unconditionally (ADR 132 decision 7), read
 * afterwards by the {@code @Projection(recordAppliedAppends = true)} registrar to decide whether warning about a
 * default-position projection that never resets is a fact this registrar can verify.
 * <p>
 * The fact is bound to the composed model's identity, not just recorded as a context-wide flag. The starter also
 * lets {@code Subscriptions}, {@code StreamSubscriptions} and {@code DcbSubscriptions} be replaced independently of
 * the model {@link #suppliedBy(SubscriptionModelCapability)} was given, so a projection running on a replacement composition of its own,
 * one whose {@code DEFAULT} genuinely replays, must not inherit this fact. {@link #isDefaultKnownLiveOnlyFor(SubscriptionModelCapability)}
 * only answers true for the exact model {@link #suppliedBy(SubscriptionModelCapability)} was given.
 * <p>
 * The blocking stack's equivalent of the reactor stack's {@code ComposedCatchupModel}, mirroring its
 * {@code suppliedBy}/{@code defaultBypassesCatchup} split rather than its full replay-phase machinery. This stack's
 * capability lookup already unwraps a wrapper chain to find {@code ReplayAwareSubscriptions} on its own (ADR 132
 * decision 8), so it has no equivalent gap on that side to work around.
 * <p>
 * {@code public} for the same reason {@code PushCatchupStatusImpl} is, the auto-configuration and the registrar
 * that use it live in different modules.
 */
@NullMarked
public final class ComposedDefaultStartPosition {

    private volatile boolean defaultBypassesCatchup = false;
    private volatile @Nullable SubscriptionModelCapability composedModel;

    /**
     * Supplies the composed subscription model {@link #isDefaultKnownLiveOnlyFor(SubscriptionModelCapability)}
     * later compares a projection's own model against, by reference rather than by type or capability, since two
     * different compositions can both expose {@code ReplayAwareSubscriptions}. Unwrapped to its ultimate AOP target
     * first (a fixed-singleton proxy only), since a projection's own capability lookup can see either the raw bean
     * this method was given or a proxy Spring wraps around it afterwards ({@code @Transactional}, a metrics or retry
     * aspect, any {@code BeanPostProcessor}), and the two would otherwise never compare equal. Called at most once,
     * by the same auto-configuration bean method that calls {@link #defaultBypassesCatchup()}, since only it knows
     * both facts about the model it just composed.
     */
    public void suppliedBy(SubscriptionModelCapability composedModel) {
        requireNonNull(composedModel, "composedModel cannot be null");
        if (this.composedModel != null) {
            throw new IllegalStateException("suppliedBy was already called once for this holder, it must not be called a second time.");
        }
        this.composedModel = ultimateTarget(composedModel);
    }

    /**
     * Records, as a known fact, that this composition's {@code StartPosition.DEFAULT} bypasses its catch-up layer
     * unconditionally, the checkpoint is never consulted, so a wiped checkpoint changes nothing (ADR 132 decision 7).
     * Called at most once, by the auto-configuration bean method that composed this model, since only it actually
     * knows how its own composition resolves that marker. Never inferred here, and never assumed true for a
     * composition an application supplied itself, whose own {@code DEFAULT} semantics are its own to declare.
     */
    public void defaultBypassesCatchup() {
        this.defaultBypassesCatchup = true;
    }

    /**
     * Whether {@code candidate}, the model a particular projection actually runs on, is the exact composition
     * {@link #suppliedBy(SubscriptionModelCapability)} was given and {@link #defaultBypassesCatchup()} was recorded
     * for. {@code false} until both were called, and {@code false} for any composition that is not that same
     * instance, including one an application supplied itself by replacing {@code Subscriptions},
     * {@code StreamSubscriptions} or {@code DcbSubscriptions} independently of the model this holder was told about.
     * {@code candidate} is unwrapped to its ultimate AOP target first, the same way {@link #suppliedBy} already
     * unwraps what it is given, so a proxy around either side still compares equal to the raw target the other side
     * holds. A warning keyed on this answers honestly rather than by inferring composition-specific behavior it
     * cannot verify.
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
