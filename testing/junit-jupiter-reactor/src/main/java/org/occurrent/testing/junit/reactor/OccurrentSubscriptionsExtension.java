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

package org.occurrent.testing.junit.reactor;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.IntrospectableSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModelLifeCycle;

import java.time.Duration;
import java.util.*;

/**
 * The reactive counterpart of the blocking {@code OccurrentSubscriptionsExtension}: stops every subscription model
 * before and after each test, so a test only runs the subscriptions it names with {@link #start(String)}. A test that
 * writes events then cannot race a subscription it never asked for.
 * <p>
 * Stopping after each test matters because a Spring test context is cached across test classes, so a subscription one
 * class started would otherwise still be running for the next.
 * <p>
 * <strong>Two differences from the blocking twin, both forced by the reactive types.</strong> Resuming a subscription
 * waits on {@link Subscription#waitUntilStarted()}, and clearing a checkpoint waits on
 * {@link CheckpointStorage#delete(String)}, both of which return a {@code Mono} rather than blocking the calling
 * thread. A JUnit {@code beforeEach} is synchronous, so this extension blocks on them itself, bounded to 10 seconds
 * rather than waiting forever, instead of asking every test to. And there is no reactive
 * {@code DelegatingSubscriptionModel} to unwrap, so introspection is a plain {@code instanceof} check on the model
 * handed in, through {@link IntrospectableSubscriptionModel}, rather than the recursive {@code of(..)} the blocking
 * side has.
 * <p>
 * Accepts more than one subscription model, because a reactive Spring context typically has two life-cycle bearing
 * ones, the durable model and a {@code SynchronousSubscriptionModel}. Every model given is stopped and resumed the
 * same way, and a subscription id is looked for across all of them.
 */
public final class OccurrentSubscriptionsExtension implements BeforeEachCallback, AfterEachCallback {

    // Every block() in this class is bounded by this, matching the rest of the reactor stack's tests, so a hung
    // checkpoint storage or a subscription that never starts fails the test rather than hanging the run.
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(10);

    private final List<SubscriptionModelLifeCycle> subscriptionModels;
    private final Set<String> alwaysStartIds = new LinkedHashSet<>();
    private final Set<String> knownIds = new LinkedHashSet<>();
    private @Nullable Runnable clearState;
    private @Nullable CheckpointStorage checkpointStorage;

    private OccurrentSubscriptionsExtension(List<SubscriptionModelLifeCycle> subscriptionModels) {
        this.subscriptionModels = List.copyOf(subscriptionModels);
    }

    /**
     * An extension over {@code subscriptionModels} where no subscription runs until a test asks for one. Register it
     * with {@code @RegisterExtension}.
     *
     * @param subscriptionModel the subscription model to stop and start, must not be {@code null}
     * @param moreModels        further models to stop and start the same way, for a context with more than one
     *                          life-cycle bearing model, must not contain {@code null}
     * @return a new extension
     */
    public static OccurrentSubscriptionsExtension stoppedByDefault(SubscriptionModelLifeCycle subscriptionModel, SubscriptionModelLifeCycle... moreModels) {
        Objects.requireNonNull(subscriptionModel, "subscriptionModel must not be null");
        Objects.requireNonNull(moreModels, "moreModels must not be null");
        List<SubscriptionModelLifeCycle> models = new ArrayList<>();
        models.add(subscriptionModel);
        for (SubscriptionModelLifeCycle model : moreModels) {
            models.add(Objects.requireNonNull(model, "moreModels must not contain null"));
        }
        return new OccurrentSubscriptionsExtension(models);
    }

    /**
     * As {@link #stoppedByDefault(SubscriptionModelLifeCycle, SubscriptionModelLifeCycle...)}, for a caller that
     * already holds the models in a list, such as every {@code SubscriptionModelLifeCycle} bean in a Spring context.
     *
     * @param subscriptionModels the subscription models to stop and start, must not be {@code null}, must not be
     *                           empty, and must not contain {@code null}
     * @return a new extension
     */
    public static OccurrentSubscriptionsExtension stoppedByDefault(List<? extends SubscriptionModelLifeCycle> subscriptionModels) {
        Objects.requireNonNull(subscriptionModels, "subscriptionModels must not be null");
        if (subscriptionModels.isEmpty()) {
            throw new IllegalArgumentException("subscriptionModels must not be empty");
        }
        List<SubscriptionModelLifeCycle> copy = new ArrayList<>();
        for (SubscriptionModelLifeCycle model : subscriptionModels) {
            copy.add(Objects.requireNonNull(model, "subscriptionModels must not contain null"));
        }
        return new OccurrentSubscriptionsExtension(copy);
    }

    /**
     * Start these subscriptions before every test, for a test class where each test needs the same ones.
     *
     * @param subscriptionIds the ids to start before every test, must not be {@code null} and must not contain {@code null}
     * @return this extension, so the call can be chained onto {@link #stoppedByDefault(SubscriptionModelLifeCycle, SubscriptionModelLifeCycle...)}
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
        subscriptionModels.forEach(SubscriptionModelLifeCycle::stop);
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
        // Sequential rather than fanned out, since this runs once per test and a flush is not a race to win.
        ids.forEach(id -> storage.delete(id).block(WAIT_TIMEOUT));
    }

    @Override
    public void afterEach(ExtensionContext context) {
        subscriptionModels.forEach(SubscriptionModelLifeCycle::stop);
    }

    /**
     * Start one subscription and block until it is actually listening, so a write that follows cannot outrun it.
     *
     * @param subscriptionId the id of a currently stopped subscription, must not be {@code null}
     * @return the running {@link Subscription}
     * @throws IllegalArgumentException if no model has a stopped subscription with that id
     */
    public Subscription start(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId must not be null");
        return resumeAndWait(subscriptionId);
    }

    /**
     * Start every subscription every model has, for the one test that needs to see them all working together. A
     * subscription that is already running is left alone.
     *
     * @return the ids that were started, in no particular order
     * @throws IllegalStateException if any model cannot list its subscriptions
     */
    public Set<String> startAll() {
        Set<String> ids = new LinkedHashSet<>(modelSubscriptionIds().orElseThrow(() -> new IllegalStateException(
                "Cannot start all subscriptions because at least one subscription model cannot list them. "
                        + "Name each subscription with start(String) instead, or use models implementing "
                        + IntrospectableSubscriptionModel.class.getSimpleName() + ".")));
        ids.removeIf(subscriptionId -> subscriptionModels.stream().noneMatch(model -> model.isPaused(subscriptionId)));
        ids.forEach(this::resumeAndWait);
        return Set.copyOf(ids);
    }

    // Tries each model in turn, since the id's owner is not known up front, and none of the reactive DSL wrappers
    // forward introspection to say so directly.
    private Subscription resumeAndWait(String subscriptionId) {
        List<IllegalArgumentException> failures = new ArrayList<>();
        for (SubscriptionModelLifeCycle model : subscriptionModels) {
            try {
                Subscription subscription = model.resumeSubscription(subscriptionId);
                knownIds.add(subscriptionId);
                subscription.waitUntilStarted().block(WAIT_TIMEOUT);
                return subscription;
            } catch (IllegalArgumentException e) {
                failures.add(e);
            }
        }
        IllegalArgumentException failure = new IllegalArgumentException(
                "Could not start subscription '" + subscriptionId + "', " + failures.get(failures.size() - 1).getMessage()
                        + ". " + describeAvailableIds());
        failures.forEach(failure::addSuppressed);
        throw failure;
    }

    // Prefers what the models actually know, since the ids this extension was told about are useless on a typo, they
    // contain the same wrong id and nothing else.
    private String describeAvailableIds() {
        return modelSubscriptionIds()
                .map(ids -> ids.isEmpty() ? "No subscription model has any subscriptions." : "Subscriptions on the model(s): " + new TreeSet<>(ids) + ".")
                .orElseGet(() -> knownIds.isEmpty()
                        ? "At least one subscription model cannot list its subscriptions, and this extension has not been told about any."
                        : "At least one subscription model cannot list its subscriptions. Ids named via alwaysStart or start: " + knownIds + ".");
    }

    // All-or-nothing: if any model in the list cannot be introspected, the union would silently under-report, so this
    // answers empty rather than a partial list a caller could mistake for the whole truth.
    private Optional<Set<String>> modelSubscriptionIds() {
        Set<String> ids = new LinkedHashSet<>();
        for (SubscriptionModelLifeCycle model : subscriptionModels) {
            if (!(model instanceof IntrospectableSubscriptionModel introspectable)) {
                return Optional.empty();
            }
            ids.addAll(introspectable.subscriptionIds());
        }
        return Optional.of(ids);
    }
}
