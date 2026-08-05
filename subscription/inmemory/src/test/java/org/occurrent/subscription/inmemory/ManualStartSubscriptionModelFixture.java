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

package org.occurrent.subscription.inmemory;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.api.blocking.ManualStartSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.subscription.blocking.StartAtVariant;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;

import java.util.List;
import java.util.Set;

/**
 * Wires {@code ManualStartSubscriptionModel.stoppedByDefault(new InMemorySubscriptionModel())} through this module
 * rather than through {@code subscription/api/blocking}, where the class actually lives. That module cannot
 * test-depend on {@code occurrent-tck-subscription-blocking}: the TCK itself depends on
 * {@code occurrent-subscription-api-blocking} for {@code SubscriptionModel}, and Maven's reactor build order treats a
 * dependency back from there to the TCK as a cycle even though it would only ever be a test-scope one. This module
 * already test-depends on the TCK and already hosts {@code InMemorySubscriptionModelConformanceTest}, so the wiring
 * lives here instead, next to the delegate it wraps.
 * <p>
 * <strong>What this fixture tests, and what it deliberately does not.</strong> A model that is "stopped by default"
 * withholds every registration until something starts it, and {@code SubscriptionModelConformance} has no way to
 * express that: it subscribes and then expects an event published afterwards to actually arrive. Weakening those
 * assertions is against the rules the suite sets itself, and there is no fixture flag for "not started at all" either
 * ({@code deliversEventsPublishedWhilePaused} is about a paused subscription that has already run once). So this
 * fixture calls {@link ManualStartSubscriptionModel#start(boolean)} once, before handing the model to the suite, which
 * means every subscription the suite creates reaches the delegate directly: from the suite's point of view this is
 * conformance-testing a <em>started</em> {@code ManualStartSubscriptionModel}, its forwarding of every
 * {@code SubscriptionModelLifeCycle} method and its {@code subscriptionIds()} union. What it does <em>not</em> cover,
 * and what stays covered only by {@code ManualStartSubscriptionModelTest} instead, is the withholding mechanism
 * itself: that a first registration never reaches the delegate until started, and the start-position pinning that
 * {@code stoppedByDefault(delegate, positionSource, checkpointStorage)} adds (this fixture uses the two-argument
 * form, which pins nothing, since there is no position source to capture).
 */
class ManualStartSubscriptionModelFixture implements SubscriptionModelFixture {

    private final InMemorySubscriptionModel delegate = new InMemorySubscriptionModel();
    private final ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);

    ManualStartSubscriptionModelFixture() {
        // Started up front, see the class javadoc: a model still withholding cannot satisfy a suite that expects
        // subscribe(...) to actually reach the delegate.
        model.start(true);
    }

    @Override
    public SubscriptionModel subscriptionModel() {
        return model;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        delegate.accept(events);
    }

    /**
     * Same mechanism and same answer as {@code InMemorySubscriptionModelFixture}: once started, every subscribe,
     * pause and resume this fixture triggers reaches {@code InMemorySubscriptionModel} unchanged, and its
     * {@code accept(...)} skips a subscription that is not running, so an event fed in while paused is dropped
     * rather than queued.
     */
    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return false;
    }

    /**
     * Same mechanism as {@code InMemorySubscriptionModelFixture}: delivery happens on the delegate's own pool thread
     * behind a {@code RetryStrategy}, and {@code ManualStartSubscriptionModel} adds nothing in front of the handler
     * that would change that once it is started.
     */
    @Override
    public boolean retriesAFailingHandler() {
        return true;
    }

    /**
     * Only reached for the refusal below, since {@code CHECKPOINT} is not accepted and this model rejects the
     * variant without looking at the value, same as {@code InMemorySubscriptionModelFixture}.
     */
    @Override
    public Checkpoint aCheckpointToStartFrom() {
        return GlobalCheckpoint.of(0);
    }

    /**
     * Everything but a checkpoint, and for two stacked reasons rather than one. {@code ManualStartSubscriptionModel}
     * itself never inspects {@code startAt}, it only stores whatever it is given and hands it to the delegate
     * unchanged once started (see {@code subscribe} and {@code resumeSubscription}), so it narrows nothing on its
     * own. The restriction is entirely the delegate's: {@code InMemorySubscriptionModel.subscribe} throws unless the
     * position resolves to {@code now} or {@code default}, exactly what
     * {@code InMemorySubscriptionModelFixture.acceptedStartAtVariants} already declares.
     */
    @Override
    public Set<StartAtVariant> acceptedStartAtVariants() {
        return Set.of(StartAtVariant.NOW, StartAtVariant.SUBSCRIPTION_MODEL_DEFAULT, StartAtVariant.DYNAMIC);
    }

    @Override
    public void close() {
        model.shutdown();
    }
}
