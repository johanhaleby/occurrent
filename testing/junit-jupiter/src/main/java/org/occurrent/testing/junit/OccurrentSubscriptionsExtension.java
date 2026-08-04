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

package org.occurrent.testing.junit;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Stops every subscription before and after each test, so a test only runs the subscriptions it names with
 * {@link #start(String)}. A test that writes events then cannot race a subscription it never asked for.
 * <p>
 * Stopping after each test matters because a Spring test context is cached across test classes, so a subscription one
 * class started would otherwise still be running for the next.
 */
public final class OccurrentSubscriptionsExtension implements BeforeEachCallback, AfterEachCallback {

    private final SubscriptionModelLifeCycle subscriptionModel;
    private final Set<String> alwaysStartIds = new LinkedHashSet<>();
    private final Set<String> knownIds = new LinkedHashSet<>();
    private @Nullable Runnable clearState;
    private @Nullable CheckpointStorage checkpointStorage;

    private OccurrentSubscriptionsExtension(SubscriptionModelLifeCycle subscriptionModel) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel must not be null");
    }

    /**
     * An extension over {@code subscriptionModel} where no subscription runs until a test asks for one. Register it
     * with {@code @RegisterExtension}.
     *
     * @param subscriptionModel the subscription model to stop and start, must not be {@code null}
     * @return a new extension
     */
    public static OccurrentSubscriptionsExtension stoppedByDefault(SubscriptionModelLifeCycle subscriptionModel) {
        return new OccurrentSubscriptionsExtension(subscriptionModel);
    }

    /**
     * Start these subscriptions before every test, for a test class where each test needs the same ones.
     *
     * @param subscriptionIds the ids to start before every test, must not be {@code null} and must not contain {@code null}
     * @return this extension, so the call can be chained onto {@link #stoppedByDefault(SubscriptionModelLifeCycle)}
     */
    public OccurrentSubscriptionsExtension alwaysStart(String... subscriptionIds) {
        Objects.requireNonNull(subscriptionIds, "subscriptionIds must not be null");
        for (String subscriptionId : subscriptionIds) {
            Objects.requireNonNull(subscriptionId, "subscriptionIds must not contain null");
            alwaysStartIds.add(subscriptionId);
            knownIds.add(subscriptionId);
        }
        return this;
    }

    /**
     * Clear whatever a test must not inherit, after every subscription is stopped and before any
     * {@link #alwaysStart(String...)} subscription is resumed. A database flush goes here, so no test has to pin
     * extension order with {@code @Order}, which is the only way to express it otherwise.
     *
     * @param clearState run once before each test, must not be {@code null}
     * @return this extension, so the call can be chained
     */
    public OccurrentSubscriptionsExtension clearingStateWith(Runnable clearState) {
        this.clearState = Objects.requireNonNull(clearState, "clearState must not be null");
        return this;
    }

    /**
     * Delete every known subscription's checkpoint before each test, so a subscription never resumes from where an
     * earlier test left it and receives events that test wrote.
     * <p>
     * Clearing the events alone does not achieve this. A stored checkpoint is what decides where a resumed subscription
     * starts, so it has to go too, and it is not necessarily stored next to the events: a MongoDB event store keeping
     * its checkpoints in Redis is an ordinary setup.
     *
     * @param checkpointStorage the storage holding the checkpoints, must not be {@code null}
     * @return this extension, so the call can be chained
     */
    public OccurrentSubscriptionsExtension clearingCheckpoints(CheckpointStorage checkpointStorage) {
        this.checkpointStorage = Objects.requireNonNull(checkpointStorage, "checkpointStorage must not be null");
        return this;
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        subscriptionModel.stop();
        // State first, then checkpoints, so a flush that recreates the checkpoint collection cannot leave one behind.
        if (clearState != null) {
            clearState.run();
        }
        if (checkpointStorage != null) {
            deleteCheckpoints(checkpointStorage);
        }
        for (String subscriptionId : alwaysStartIds) {
            resumeAndWait(subscriptionId);
        }
    }

    private void deleteCheckpoints(CheckpointStorage storage) {
        Set<String> ids = modelSubscriptionIds().orElse(knownIds);
        if (ids.isEmpty()) {
            throw new IllegalStateException("Cannot clear checkpoints because there are no subscription ids to clear "
                    + "them for. " + describeAvailableIds() + " Name the subscriptions with alwaysStart(String...), or "
                    + "use a model implementing " + IntrospectableSubscriptionModel.class.getSimpleName() + ".");
        }
        ids.forEach(storage::delete);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        subscriptionModel.stop();
    }

    /**
     * Start one subscription and block until it is actually listening, so a write that follows cannot outrun it.
     *
     * @param subscriptionId the id of a currently stopped subscription, must not be {@code null}
     * @return the running {@link Subscription}
     * @throws IllegalArgumentException if the model has no stopped subscription with that id
     */
    public Subscription start(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId must not be null");
        return resumeAndWait(subscriptionId);
    }

    /**
     * Start every subscription the model has, for the one test that needs to see them all working together. A
     * subscription that is already running is left alone.
     *
     * @return the ids that were started, in no particular order
     * @throws IllegalStateException if the subscription model cannot list its subscriptions
     */
    public Set<String> startAll() {
        Set<String> ids = new LinkedHashSet<>(modelSubscriptionIds().orElseThrow(() -> new IllegalStateException(
                "Cannot start all subscriptions because " + subscriptionModel.getClass().getName() + " cannot list them. "
                        + "Name each subscription with start(String) instead, or use a model implementing "
                        + IntrospectableSubscriptionModel.class.getSimpleName() + ".")));
        ids.removeIf(subscriptionId -> !subscriptionModel.isPaused(subscriptionId));
        ids.forEach(this::resumeAndWait);
        return Set.copyOf(ids);
    }

    private Subscription resumeAndWait(String subscriptionId) {
        Subscription subscription;
        try {
            subscription = subscriptionModel.resumeSubscription(subscriptionId);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Could not start subscription '" + subscriptionId + "', " + e.getMessage() + ". " + describeAvailableIds(), e);
        }
        knownIds.add(subscriptionId);
        subscription.waitUntilStarted();
        return subscription;
    }

    // Prefers what the model actually knows, since the ids this extension was told about are useless on a typo, they
    // contain the same wrong id and nothing else.
    private String describeAvailableIds() {
        return modelSubscriptionIds()
                .map(ids -> ids.isEmpty() ? "The subscription model has no subscriptions." : "Subscriptions on the model: " + new TreeSet<>(ids) + ".")
                .orElseGet(() -> knownIds.isEmpty()
                        ? "This subscription model cannot list its subscriptions, and this extension has not been told about any."
                        : "This subscription model cannot list its subscriptions. Ids named via alwaysStart or start: " + knownIds + ".");
    }

    private Optional<Set<String>> modelSubscriptionIds() {
        return IntrospectableSubscriptionModel.of(subscriptionModel).map(IntrospectableSubscriptionModel::subscriptionIds);
    }
}
