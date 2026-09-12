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
 * Every answer here is about one composition and is bound to that composition's identity. A context can hold more
 * than one subscription model, and a projection is told about its catch-ups by the one its own subscription runs
 * on, or by nothing at all. Being told by a second bean of the same type would hand a whole replay to a layer that
 * has never heard of that subscription id, which answers "not catching up", records the replay as live appends, and
 * leaves {@code waitUntilApplied} saying yes for an append the rebuilt read model has not applied. So
 * {@link #suppliedBy(SubscriptionModelCapability, Object)} takes the model a projection resolves as well as the
 * catch-up layer hidden inside it, and every read takes the candidate it is answering for.
 * <p>
 * A bean of this type, {@link #suppliedBy(SubscriptionModelCapability, Object)} called once during that bean's own
 * construction, and read afterwards through {@link #catchupModelFor(SubscriptionModelCapability)} by the reactor
 * {@code @Projection(recordAppliedAppends = true)} registrar. {@code public} for the same reason
 * {@code PushCatchupStatusImpl} is, the auto-configuration and the registrar that use it live in different packages.
 * <p>
 * The reactor stack's equivalent of the blocking stack's {@code ComposedDefaultStartPosition}, which carries the
 * {@code DEFAULT} fact alone because that stack's capability lookup finds the catch-up layer by itself.
 * <p>
 * <a href="https://github.com/johanhaleby/occurrent/issues/842">#842</a> tracks fixing the underlying capability-lookup
 * gap. This class is the workaround ADR 132 sanctions until then, not a replacement for it.
 */
@NullMarked
public final class ComposedCatchupModel {

    // composedModel doubles as the record of whether suppliedBy ran at all, which replayAware cannot do, since it
    // is also null when suppliedBy was given a composition with no catch-up layer, a known fact rather than an
    // unknown one, and the two must not read the same.
    private volatile @Nullable SubscriptionModelCapability composedModel;
    private volatile @Nullable ReplayAwareSubscriptions replayAware;
    private volatile boolean defaultBypassesCatchup = false;

    /**
     * Supplies the composed subscription model this instance answers for, together with the catch-up layer inside
     * it. Both are facts about the one composition the caller just built, and neither is usable without the other.
     * {@code catchupLayer} is what a projection listens to, and {@code composedModel} is how a later read tells a
     * projection running on that composition from one running on some other model the context also holds.
     * <p>
     * {@code composedModel} is the bean a projection resolves and subscribes through, the outermost wrapper rather
     * than the layer handed as {@code catchupLayer}, since that outer bean is what a projection's own capability
     * lookup returns. It is compared by reference rather than by type or capability, since two different
     * compositions can both expose {@link ReplayAwareSubscriptions}, and it is unwrapped to its ultimate AOP target
     * first, since a projection's own lookup can see either the raw bean this method was given or a proxy Spring's
     * own AOP auto-proxying wraps around it afterward ({@code @Transactional}, {@code @Async}, a metrics or retry
     * advisor), and the two would otherwise never compare equal. That unwrap only reaches a genuine Spring AOP
     * proxy backed by a fixed singleton target, which is what the framework's own auto-proxying produces. A wrapper
     * built outside that framework, or one backed by a prototype- or pool-scoped target source, is compared as the
     * wrapper it is.
     * <p>
     * {@code catchupLayer} is {@code instanceof}-checked against {@link ReplayAwareSubscriptions} once, here, rather
     * than on every read. The composition is fixed once this bean is built, so there is nothing to re-resolve later.
     * It takes {@link Object} rather than a narrower capability marker, since what a catch-up composition returns
     * (a {@code CheckpointAwareSubscriptionModel} on the reactor stack) is not itself typed as one. Only some of its
     * possible concrete shapes, {@code ReactorCatchupSubscriptionModel} among them, are.
     * <p>
     * Called at most once, from the bean method that composed both, and refuses a second call, since a holder
     * answering for two compositions could not say which one a candidate is, which is the very thing it exists to
     * say.
     */
    public void suppliedBy(SubscriptionModelCapability composedModel, Object catchupLayer) {
        requireNonNull(composedModel, "composedModel cannot be null");
        requireNonNull(catchupLayer, "catchupLayer cannot be null");
        if (this.composedModel != null) {
            throw new IllegalStateException("suppliedBy was already called once for this holder, it must not be called a second time.");
        }
        this.replayAware = catchupLayer instanceof ReplayAwareSubscriptions replayAwareSubscriptions ? replayAwareSubscriptions : null;
        this.composedModel = ultimateTarget(composedModel);
    }

    /**
     * The catch-up layer of the composition {@code candidate} is, when that composition can say what its catch-ups
     * are doing. Empty when {@link #suppliedBy} was given a composition that cannot, a store with no catch-up layer
     * at all (ADR 132 decision 9), empty when {@link #suppliedBy} was never called, and empty when {@code candidate}
     * is some other model than the one this holder was told about. Those are different facts, and
     * {@link #isSuppliedFor(SubscriptionModelCapability)} tells the first apart from the rest. The first is a known
     * thing about this composition, the others are the caller's cue to fall back to another source, ultimately to
     * the warning decision 2 requires for a composition nothing can see into.
     */
    public Optional<ReplayAwareSubscriptions> catchupModelFor(@Nullable SubscriptionModelCapability candidate) {
        return isSuppliedFor(candidate) ? Optional.ofNullable(replayAware) : Optional.empty();
    }

    /**
     * Whether {@code candidate}, the model a particular projection actually runs on, is the exact composition
     * {@link #suppliedBy} was given. {@code false} until {@link #suppliedBy} is called, and {@code false} for any
     * other model the context holds. {@code candidate} is unwrapped to its ultimate AOP target first, the same way
     * {@link #suppliedBy} already unwraps what it is given (see that method for which proxies this reaches and which
     * it does not), so a genuine Spring AOP proxy around either side still compares equal to the raw target the
     * other side holds.
     */
    public boolean isSuppliedFor(@Nullable SubscriptionModelCapability candidate) {
        return candidate != null && composedModel != null && ultimateTarget(candidate) == composedModel;
    }

    /**
     * Records, as a known fact, that {@link org.occurrent.annotation.StartPosition#DEFAULT} bypasses this
     * composition's catch-up layer unconditionally. {@code StartAt.subscriptionModelDefault()} never replays here,
     * the checkpoint is never consulted, so a wiped checkpoint changes nothing (ADR 132 decision 7). Called at most
     * once, by the same bean method that calls {@link #suppliedBy}, since only the auto-configuration that composed
     * this model actually knows how it resolves that marker. Never inferred here, and never assumed true for a
     * composition an application supplied itself, whose own {@code DEFAULT} semantics are its own to declare.
     */
    public void defaultBypassesCatchup() {
        this.defaultBypassesCatchup = true;
    }

    /**
     * Whether {@link #defaultBypassesCatchup} was called for the composition {@code candidate} is. {@code false}
     * until both it and {@link #suppliedBy} were called, and {@code false} for any composition that is not the one
     * this holder was told about, including one an application supplied itself. A warning keyed on this answers
     * honestly rather than by inferring composition-specific behavior it cannot verify.
     */
    public boolean isDefaultKnownLiveOnlyFor(@Nullable SubscriptionModelCapability candidate) {
        return defaultBypassesCatchup && isSuppliedFor(candidate);
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
