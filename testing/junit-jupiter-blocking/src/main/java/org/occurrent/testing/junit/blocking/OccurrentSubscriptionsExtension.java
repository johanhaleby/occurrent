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

package org.occurrent.testing.junit.blocking;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle;

import java.time.Duration;
import java.util.*;

/**
 * Stops every subscription model before and after each test, so a test only runs the subscriptions it names with
 * {@link #start(String)}. A test that writes events then cannot race a subscription it never asked for.
 * <p>
 * Stopping after each test matters because a Spring test context is cached across test classes, so a subscription one
 * class started would otherwise still be running for the next.
 * <p>
 * Accepts more than one subscription model, because a Spring context can have two life-cycle bearing ones, for example
 * a durable model and a {@code SynchronousSubscriptionModel}. Every model given is stopped and resumed the same way,
 * and a subscription id is looked for across all of them.
 */
public final class OccurrentSubscriptionsExtension implements BeforeEachCallback, AfterEachCallback {

    // Generous rather than tight, because this bounds a subscription model that may be talking to a container on a
    // loaded CI machine, and a default that is too tight turns a working subscription into a flaky test. Its job is to
    // turn a subscription that never starts into a failing test rather than a run that hangs, which any finite bound
    // does.
    private static final Duration DEFAULT_START_TIMEOUT = Duration.ofSeconds(30);

    private final List<SubscriptionModelLifeCycle> subscriptionModels;
    private final Set<String> alwaysStartIds = new LinkedHashSet<>();
    private final Set<String> knownIds = new LinkedHashSet<>();
    private Duration startTimeout = DEFAULT_START_TIMEOUT;
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
     * starts, so it has to go too, and it is not necessarily stored next to the events. A MongoDB event store keeping
     * its checkpoints in Redis is an ordinary setup.
     * <p>
     * Known means every id the models report plus every id named on this extension. When that comes to nothing, there
     * is no checkpoint to clear and nothing is deleted. Use {@link #clearingCheckpointsFor(CheckpointStorage, String...)}
     * to clear checkpoints for subscriptions no model reports.
     *
     * @param checkpointStorage the storage holding the checkpoints, must not be {@code null}
     * @return this extension, so the call can be chained
     */
    public OccurrentSubscriptionsExtension clearingCheckpoints(CheckpointStorage checkpointStorage) {
        this.checkpointStorage = Objects.requireNonNull(checkpointStorage, "checkpointStorage must not be null");
        return this;
    }

    /**
     * As {@link #clearingCheckpoints(CheckpointStorage)}, and additionally clear these subscriptions' checkpoints
     * whether or not any model reports them.
     * <p>
     * This is what to reach for when the checkpoints to clear belong to subscriptions the models cannot list, or to
     * ones a test starts by hand later. {@link #alwaysStart(String...)} would name them too, but it also resumes them
     * before every test, which is a different thing to ask for.
     *
     * @param checkpointStorage the storage holding the checkpoints, must not be {@code null}
     * @param subscriptionIds   the ids to clear a checkpoint for, must not be {@code null}, must not contain
     *                          {@code null}, and must not be empty
     * @return this extension, so the call can be chained
     */
    public OccurrentSubscriptionsExtension clearingCheckpointsFor(CheckpointStorage checkpointStorage, String... subscriptionIds) {
        Objects.requireNonNull(subscriptionIds, "subscriptionIds must not be null");
        if (subscriptionIds.length == 0) {
            throw new IllegalArgumentException("subscriptionIds must not be empty. Use clearingCheckpoints("
                    + CheckpointStorage.class.getSimpleName() + ") to clear every known subscription's checkpoint.");
        }
        for (String subscriptionId : subscriptionIds) {
            knownIds.add(Objects.requireNonNull(subscriptionId, "subscriptionIds must not contain null"));
        }
        return clearingCheckpoints(checkpointStorage);
    }

    /**
     * How long to wait for a subscription to actually start before failing the test, whether it is started by
     * {@link #start(String)}, {@link #startAll()} or {@link #alwaysStart(String...)}. Defaults to 30 seconds.
     * <p>
     * There is a bound at all because a subscription that never starts would otherwise hang the whole run rather than
     * fail one test, and the default is generous because a model talking to a container on a loaded machine is slower
     * than the same model locally. Widen it for a genuinely slow model rather than narrowing it to catch a fast one.
     *
     * @param startTimeout how long to wait, must not be {@code null} and must be positive
     * @return this extension, so the call can be chained
     */
    public OccurrentSubscriptionsExtension withStartTimeout(Duration startTimeout) {
        Objects.requireNonNull(startTimeout, "startTimeout must not be null");
        if (startTimeout.isZero() || startTimeout.isNegative()) {
            throw new IllegalArgumentException("startTimeout must be positive, was " + startTimeout + ".");
        }
        this.startTimeout = startTimeout;
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

    // Every id this extension can name, rather than the models' ids or the named ones. Naming an id must not narrow
    // what gets cleared, and an id named for clearing is exactly the one no model reports. Deleting a checkpoint that
    // does not exist is a no-op, so the union costs nothing when the two overlap, which is the normal case. An empty
    // union means there is no checkpoint to clear, and deleting nothing is the right answer to that rather than a
    // failure raised from beforeEach before any test body runs.
    private void deleteCheckpoints(CheckpointStorage storage) {
        Set<String> ids = new LinkedHashSet<>(knownIds);
        modelSubscriptionIds().ifPresent(ids::addAll);
        ids.forEach(storage::delete);
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
     * @throws UnknownSubscriptionException        if no model has a subscription with that id
     * @throws SubscriptionAlreadyRunningException if the model that has it reports it is already running
     * @throws IllegalStateException               if it does not start within {@link #withStartTimeout(Duration)}
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
     * @throws IllegalStateException if any model cannot list its subscriptions, or if a subscription does not start
     *                               within {@link #withStartTimeout(Duration)}
     */
    public Set<String> startAll() {
        Set<String> ids = new LinkedHashSet<>(modelSubscriptionIds().orElseThrow(() -> new IllegalStateException(
                "Cannot start all subscriptions because at least one subscription model cannot list them. "
                        + "Name each subscription with start(String) instead, or use models implementing "
                        + IntrospectableSubscriptions.class.getSimpleName() + ".")));
        ids.removeIf(subscriptionId -> subscriptionModels.stream().noneMatch(model -> model.isPaused(subscriptionId)));
        ids.forEach(this::resumeAndWait);
        return Set.copyOf(ids);
    }

    // Tries each model in turn, since the id's owner is not known up front, and none of the DSL wrappers forward
    // introspection to say so directly.
    // A model that does not have the id says so with UnknownSubscriptionException, which is the one refusal worth
    // searching past. Every other refusal comes from the model that does own the id, so it is the answer rather than
    // something to keep looking behind.
    private Subscription resumeAndWait(String subscriptionId) {
        List<UnknownSubscriptionException> notHere = new ArrayList<>();
        for (SubscriptionModelLifeCycle model : subscriptionModels) {
            try {
                Subscription subscription = model.resumeSubscription(subscriptionId);
                knownIds.add(subscriptionId);
                // Bounded rather than waitUntilStarted(), whose no-argument form waits forever. beforeEach runs this
                // for every alwaysStart id, so a subscription that never starts would hang the run rather than fail
                // the test that asked for it.
                if (!subscription.waitUntilStarted(startTimeout)) {
                    throw new IllegalStateException("Subscription '" + subscriptionId + "' was resumed but had not "
                            + "started after " + startTimeout + ". Widen the wait with withStartTimeout(Duration) if "
                            + "the model is genuinely this slow to start, otherwise the subscription is not starting "
                            + "at all.");
                }
                return subscription;
            } catch (UnknownSubscriptionException e) {
                notHere.add(e);
            }
        }
        UnknownSubscriptionException failure = new UnknownSubscriptionException(subscriptionId,
                "Could not start subscription '" + subscriptionId + "', no subscription model has it. " + describeAvailableIds());
        notHere.forEach(failure::addSuppressed);
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
            Optional<IntrospectableSubscriptions> introspectable = IntrospectableSubscriptions.findIn(model);
            if (introspectable.isEmpty()) {
                return Optional.empty();
            }
            ids.addAll(introspectable.get().subscriptionIds());
        }
        return Optional.of(ids);
    }
}
