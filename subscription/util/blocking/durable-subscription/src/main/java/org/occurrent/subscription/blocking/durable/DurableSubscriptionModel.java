/*
 * Copyright 2021 Johan Haleby
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

package org.occurrent.subscription.blocking.durable;

import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.*;
import org.occurrent.subscription.util.predicate.EveryN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.occurrent.subscription.CheckpointAwareCloudEvent.getCheckpointOrThrowIAE;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * Combines a {@link SubscriptionModel} with a {@link CheckpointStorage}, persisting the checkpoint after each
 * successful call to the action in {@link DurableSubscriptionModel#subscribe(String, Consumer)}.
 *
 * <p>
 * By default the checkpoint is written after every event, doubling write load but resuming right after the
 * last delivered event on crash. Pass a {@link DurableSubscriptionModelConfig} with
 * {@link org.occurrent.subscription.util.predicate.EveryN#every(int)} to checkpoint less often, trading fewer
 * writes for events being re-delivered (must be handled idempotently) after a crash.
 *
 * <p>
 * A subscription that asks for {@link StartAt#subscriptionModelDefault()} and has no checkpoint stored yet is
 * recorded from the wrapped model's {@link CheckpointAwareSubscriptionModel#globalCheckpoint()} before anything
 * is delivered, so a crash before the first checkpoint write resumes from the recorded position instead of
 * starting over from wherever the feed has reached by then. A wrapped model that answers {@code null}, which is
 * how it reports a problem it cannot resolve, refuses the subscription with {@link IllegalStateException} from
 * {@link #subscribe(String, SubscriptionFilter, StartAt, Consumer)} rather than starting it without that promise.
 * The subscription the wrapped model accepted is cancelled, so subscribe again once the model can answer. When that
 * cancel throws too, the wrapped model may still hold the id. The exception then has a suppressed exception saying so,
 * and {@link #cancelSubscription(String)} tries the cancel again. A subscription with a
 * checkpoint already stored starts from that checkpoint and is never refused this way, and one subscribing with
 * a {@link StartAt} of its own records no position and is never refused either. This is the same answer
 * {@link ManualStartSubscriptionModel} gives for a {@code null} position source and the same one the reactor
 * {@code ReactorDurableSubscriptionModel} gives for the same registration.
 * {@link DurableSubscriptionModelConfig#startWhenNoStartPositionCanBeRecorded(boolean)} turns the refusal into a
 * start without a recorded position, accepting the loss window it documents.
 * <p>
 * The position is recorded once the wrapped model's {@code subscribe(..)} has returned, or earlier if the wrapped model
 * evaluates the start position before then, and the wrapped model gets no start position before it is stored. So with
 * the MongoDB subscription models Occurrent ships, a subscribe that the wrapped model refuses with
 * {@link DuplicateSubscriptionIdException}, because it already holds the id, stores no position.
 * <p>
 * A wrapped model of your own has two requirements that this model doesn't check:
 * <ul>
 * <li>It refuses an id it already holds before it evaluates the start position. A model that evaluates first can
 * store a position for a subscribe it then refuses, as in 0.33.0.</li>
 * <li>When it evaluates the start position before its {@code subscribe(..)} returns, and that evaluation throws, its
 * {@code subscribe(..)} throws as well. The evaluation throws when the position can't be recorded, so a model that
 * waits and evaluates it again doesn't return from {@code subscribe(..)} until the position can be recorded.</li>
 * </ul>
 */
@NullMarked
public class DurableSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModelWrapper {

    private static final Logger log = LoggerFactory.getLogger(DurableSubscriptionModel.class);

    private final CheckpointAwareSubscriptionModel subscriptionModel;
    private final CheckpointStorage storage;
    private final DurableSubscriptionModelConfig config;
    private final @Nullable CheckpointWriteVersionSource writeVersionSource;
    // subscribe(..) records a subscription id here when its StartAt resolved to null, opting it out of this model's
    // checkpoint management (the same "not allowed to start" case CompetingConsumerSubscriptionModel has its own
    // set for). resumeSubscription reads this so it forwards such a subscription unchanged too, rather than
    // resuming it from a checkpoint this model was never asked to manage. A plain set is safe here only because
    // subscribe, cancelSubscription and resumeSubscription all run under the lock for the id, which makes at most
    // one of them active for a given id at a time, so no two attempts for the same id are ever both live against
    // this set.
    private final Set<String> notCheckpointedSubscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    // Ids this model stores checkpoints for, so a restart after lost history stores a position only for those
    private final Set<String> checkpointedSubscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    // Kept so shutdown can remove the same instance it added, since every method reference is a new object
    private final HistoryLossReportingSubscriptions.HistoryLossListener historyLossListener = this::storeRestartPositionAfterHistoryLoss;
    private final QuietPositionReportingSubscriptions.QuietPositionListener quietPositionListener = this::quietPositionSaverFor;
    // The current subscribe of each id this model stores checkpoints for. A new object for every subscribe, so a read
    // that began before a cancel saves nothing for a later subscribe of the id
    private final ConcurrentMap<String, CheckpointRegistration> registrations = new ConcurrentHashMap<>();
    // One lock per id, which exists while a call holds or waits for it. Per id, so a checkpoint store that hangs in
    // one id's call blocks only calls for that id. Removed once no call needs it, so an unknown or made-up id passed
    // to cancelSubscription or resumeSubscription leaves nothing behind. The checkpoint write for an event takes none
    // of these, only the lock of its own CheckpointRegistration, which a cancel of that id also takes. Neither is a
    // monitor, since a virtual thread that waits for the checkpoint store while it holds one keeps its carrier thread
    // on JDK 21 to 23
    private final ConcurrentMap<String, IdLock> idLocks = new ConcurrentHashMap<>();

    private <T> T underLockFor(String subscriptionId, Supplier<T> call) {
        IdLock idLock = idLocks.compute(subscriptionId, (__, held) -> held == null ? new IdLock() : held.oneMoreCaller());
        try {
            idLock.lock.lock();
            try {
                return call.get();
            } finally {
                idLock.lock.unlock();
            }
        } finally {
            idLocks.computeIfPresent(subscriptionId, (__, held) -> held.oneCallerLess() ? null : held);
        }
    }

    private void runUnderLockFor(String subscriptionId, Runnable call) {
        underLockFor(subscriptionId, () -> {
            call.run();
            return null;
        });
    }

    /**
     * Create a subscription that combines a {@link CheckpointAwareSubscriptionModel} with a {@link CheckpointStorage} to automatically
     * store the subscription after each successful call to <code>action</code> (The "consumer" in {@link #subscribe(String, Consumer)}).
     *
     * @param subscriptionModel The subscription that will read events from the event store
     * @param storage           The {@link CheckpointStorage} that'll be used to persist the stream position
     */
    public DurableSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CheckpointStorage storage) {
        this(subscriptionModel, storage, new DurableSubscriptionModelConfig(everyEvent()));
    }

    /**
     * Create a subscription that combines a {@link CheckpointAwareSubscriptionModel} with a {@link CheckpointStorage} to automatically
     * store the subscription when the predicate defined in {@link DurableSubscriptionModelConfig#persistCloudEventPositionPredicate} is fulfilled.
     *
     * @param subscriptionModel The subscription that will read events from the event store
     * @param storage           The {@link CheckpointStorage} that'll be used to persist the stream position
     */
    public DurableSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CheckpointStorage storage,
                                    DurableSubscriptionModelConfig config) {
        this(subscriptionModel, storage, config, null);
    }

    /**
     * Create a subscription that combines a {@link CheckpointAwareSubscriptionModel} with a {@link CheckpointStorage} to automatically
     * store the subscription after each successful call to <code>action</code> (The "consumer" in {@link #subscribe(String, Consumer)}),
     * stamping every checkpoint write with a version from {@code writeVersionSource}.
     *
     * @param subscriptionModel  The subscription that will read events from the event store
     * @param storage            The {@link CheckpointStorage} that'll be used to persist the stream position
     * @param writeVersionSource Asked for a version before each checkpoint write. A version stamps the write
     *                           {@link CheckpointWriteCondition#notOlderThan(long) notOlderThan} it, an empty answer
     *                           or no source at all stamps it {@link CheckpointWriteCondition#any() any()}.
     */
    public DurableSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CheckpointStorage storage,
                                    CheckpointWriteVersionSource writeVersionSource) {
        this(subscriptionModel, storage, new DurableSubscriptionModelConfig(everyEvent()), writeVersionSource);
    }

    /**
     * Create a subscription that combines a {@link CheckpointAwareSubscriptionModel} with a {@link CheckpointStorage} to automatically
     * store the subscription when the predicate defined in {@link DurableSubscriptionModelConfig#persistCloudEventPositionPredicate} is fulfilled,
     * stamping every checkpoint write with a version from {@code writeVersionSource}.
     *
     * @param subscriptionModel  The subscription that will read events from the event store
     * @param storage            The {@link CheckpointStorage} that'll be used to persist the stream position
     * @param config             The {@link DurableSubscriptionModelConfig} to use
     * @param writeVersionSource Asked for a version before each checkpoint write. A version stamps the write
     *                           {@link CheckpointWriteCondition#notOlderThan(long) notOlderThan} it, an empty answer
     *                           or no source at all stamps it {@link CheckpointWriteCondition#any() any()}.
     */
    public DurableSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CheckpointStorage storage,
                                    DurableSubscriptionModelConfig config, @Nullable CheckpointWriteVersionSource writeVersionSource) {
        requireNonNull(subscriptionModel, "subscription cannot be null");
        requireNonNull(storage, CheckpointStorage.class.getSimpleName() + " cannot be null");
        requireNonNull(config, DurableSubscriptionModelConfig.class.getSimpleName() + " cannot be null");

        this.storage = storage;
        this.subscriptionModel = subscriptionModel;
        this.config = config;
        this.writeVersionSource = writeVersionSource;
        HistoryLossReportingSubscriptions.findIn(subscriptionModel)
                .ifPresent(model -> model.addHistoryLossListener(historyLossListener));
        if (config.quietPositionSaveInterval != null) {
            QuietPositionReportingSubscriptions.findIn(subscriptionModel)
                    .ifPresent(model -> model.addQuietPositionListener(quietPositionListener));
        }
    }

    // Answers nothing until the interval has passed since the last checkpoint write, so a subscription that stores a
    // checkpoint for an event at least once per interval gets no extra write. It also answers nothing while an event
    // is being delivered, or when the current delivery that started last is of an event the persist predicate declined
    // to store, since the quiet position would move the checkpoint past it. The save checks that again. The read is
    // numbered first, whatever the answer, so the next delivery on this thread is known to come from it. The write
    // condition is read here, before the wrapped model reads, so the save uses the token of the lease the read was
    // made under, like the write for an event. A source that cannot answer is asked again after the interval rather
    // than before every read
    private @Nullable Consumer<Checkpoint> quietPositionSaverFor(String subscriptionId) {
        Duration interval = config.quietPositionSaveInterval;
        CheckpointRegistration registration = registrations.get(subscriptionId);
        if (registration != null) {
            registration.reading();
        }
        if (interval == null || registration == null || !registration.quietSaveAllowed() || System.nanoTime() - registration.lastWrite.get() < interval.toNanos()) {
            return null;
        }
        CheckpointWriteCondition writeCondition;
        try {
            writeCondition = writeConditionFor(subscriptionId);
        } catch (RuntimeException e) {
            registration.lastWrite.set(System.nanoTime());
            log.warn("Could not read the write version for subscription {}, so its quiet position is not saved this time. Trying again in {}.", subscriptionId, interval, e);
            return null;
        }
        return quietPosition -> saveQuietPosition(subscriptionId, quietPosition, writeCondition, registration);
    }

    // A refused write is thrown, so the wrapped model ends delivery on a node whose lease moved, as it does for an
    // event. Any other failure is logged and tried again after the interval, since the subscription has lost nothing
    private void saveQuietPosition(String subscriptionId, Checkpoint quietPosition, CheckpointWriteCondition writeCondition, CheckpointRegistration registration) {
        runUnderLockFor(subscriptionId, () -> {
            if (registrations.get(subscriptionId) != registration) {
                return;
            }
            registration.saveQuietPositionIfAllowed(() -> {
                registration.lastWrite.set(System.nanoTime());
                try {
                    storage.save(subscriptionId, quietPosition, writeCondition);
                } catch (CheckpointWriteConditionNotFulfilledException e) {
                    throw e;
                } catch (RuntimeException e) {
                    log.warn("Failed to save the quiet position of subscription {}. Trying again in {}.", subscriptionId, config.quietPositionSaveInterval, e);
                }
            });
        });
    }

    // Stored now rather than with the next event, since a process stopping before that event would restart from
    // the lost position and skip everything written in between. Same write condition as any other checkpoint. Asked
    // under the lock resumeSubscription and cancelSubscription hold, so a run that a resume replaced or a cancel ended
    // while it asked for the present stores nothing
    private void storeRestartPositionAfterHistoryLoss(String subscriptionId, Checkpoint restartedFrom, BooleanSupplier stillCurrent) {
        runUnderLockFor(subscriptionId, () -> {
            if (!checkpointedSubscriptions.contains(subscriptionId) || !stillCurrent.getAsBoolean()) {
                return;
            }
            try {
                storage.save(subscriptionId, restartedFrom, writeConditionFor(subscriptionId));
            } catch (CheckpointWriteConditionNotFulfilledException e) {
                log.warn("Did not store the position subscription {} restarts from after its history was lost, since another node has written its checkpoint with a newer lease: {}",
                        subscriptionId, e.getMessage());
            }
        });
    }

    /**
     * Subscribe to events, persisting the checkpoint after each successful call to {@code action} per this
     * model's {@link DurableSubscriptionModelConfig}.
     *
     * @throws IllegalStateException When {@code startAt} resolves to {@link StartAt#subscriptionModelDefault()},
     *                               no checkpoint is stored for {@code subscriptionId}, and the wrapped model's
     *                               {@link CheckpointAwareSubscriptionModel#globalCheckpoint()} answers
     *                               {@code null}, which is how it reports a problem it cannot resolve. The
     *                               subscription the wrapped model accepted is cancelled, so subscribe again once the
     *                               model can answer, pass a {@link StartAt} of your own, which records no position
     *                               and makes no resume promise, or configure
     *                               {@link DurableSubscriptionModelConfig#startWhenNoStartPositionCanBeRecorded(boolean)}.
     *                               When that cancel throws too, the exception has a suppressed exception saying the
     *                               wrapped model may still hold the id, and {@link #cancelSubscription(String)} tries
     *                               the cancel again.
     */
    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, @Nullable StartAt startAt, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, startAt, action, false);
    }

    /**
     * Records the start position as {@link #subscribe(String, SubscriptionFilter, StartAt, Consumer)} does, and has
     * the wrapped model hold the subscription paused, so it starts from the recorded position once it is resumed.
     */
    @Override
    public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, startAt, action, true);
    }

    private Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, @Nullable StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused) {
        Objects.requireNonNull(startAt, StartAt.class.getSimpleName() + " supplier cannot be null");

        // Held for the whole method, not just the opt-out branch, so subscribe, resumeSubscription and
        // cancelSubscription for the same id stay serialized against notCheckpointedSubscriptions (see the field
        // comment above). The blocking MongoDB models evaluate the returned StartAt on their executor each time
        // they open a change stream, outside this lock, so a cancelSubscription can run while a later evaluation
        // reads the checkpoint or writes a first position. An evaluation that comes while this records the first
        // position waits for it.
        return underLockFor(subscriptionId, () -> {
            AtomicReference<@Nullable FirstPosition> firstPositionToRecord = new AtomicReference<>();
            StartAt startAtToUse = generateStartAtPositionFrom(subscriptionId, startAt, firstPositionToRecord::set);
            if (startAtToUse == null) {
                // Not allowed to start, delegate to the wrapped subscription instead. Whether it was already
                // marked is captured before marking it, so a duplicate attempt against an already-active,
                // opted-out id releases nothing on failure.
                boolean alreadyMarked = notCheckpointedSubscriptions.contains(subscriptionId);
                notCheckpointedSubscriptions.add(subscriptionId);
                try {
                    Subscription optedOut = holdPaused
                            ? getWrappedSubscriptionModel().subscribePaused(subscriptionId, filter, startAt, action)
                            : getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
                    checkpointedSubscriptions.remove(subscriptionId);
                    registrations.remove(subscriptionId);
                    return optedOut;
                } catch (Throwable t) {
                    if (!alreadyMarked) {
                        notCheckpointedSubscriptions.remove(subscriptionId);
                    }
                    throw t;
                }
            }

            // One per subscription, so an EveryN configured for the whole model counts this subscription's events only
            Predicate<CloudEvent> persistCheckpoint = EveryN.forOneSubscription(config.persistCloudEventPositionPredicate);
            CheckpointRegistration registration = new CheckpointRegistration();
            Consumer<CloudEvent> checkpointingAction = cloudEvent -> {
                // Taken before anything the delivery waits on, so no quiet position is saved until it has finished
                long delivery = registration.delivering();
                boolean stored = false;
                try {
                    // Read before the action runs, so the write uses the token of the lease this event was
                    // delivered under, even if this node lost that lease and won a newer one meanwhile
                    CheckpointWriteCondition writeCondition = writeConditionFor(subscriptionId);
                    action.accept(cloudEvent);
                    if (persistCheckpoint.test(cloudEvent)) {
                        Checkpoint checkpoint = getCheckpointOrThrowIAE(cloudEvent);
                        stored = registration.saveUnlessCancelled(() -> {
                            storage.save(subscriptionId, checkpoint, writeCondition);
                            registration.lastWrite.set(System.nanoTime());
                        });
                    }
                } finally {
                    registration.delivered(delivery, stored);
                }
            };
            FirstPosition firstPosition = firstPositionToRecord.get();
            Subscription subscription;
            try {
                subscription = holdPaused
                        ? subscriptionModel.subscribePaused(subscriptionId, filter, startAtToUse, checkpointingAction)
                        : subscriptionModel.subscribe(subscriptionId, filter, startAtToUse, checkpointingAction);
                if (firstPosition != null) {
                    recordOrCancel(subscriptionId, firstPosition, registration);
                }
            } finally {
                if (firstPosition != null) {
                    firstPosition.subscribeEnded();
                }
            }
            track(subscriptionId, registration);
            return subscription;
        });
    }

    // Called only once the delegate accepted this managed subscription. A previous subscribe may have left this id
    // opted out and still active, and a duplicate id the delegate refuses must leave that active subscription's marker
    // alone rather than losing it to this failed attempt.
    private void track(String subscriptionId, CheckpointRegistration registration) {
        notCheckpointedSubscriptions.remove(subscriptionId);
        checkpointedSubscriptions.add(subscriptionId);
        registrations.put(subscriptionId, registration);
    }

    // Cancels the subscription the wrapped model accepted when recording its first position fails, so the caller gets
    // the refusal. A wrapped model whose cancel fails as well may still hold the subscription, so it stays tracked like
    // an accepted one, and cancelSubscription tries the cancel again
    private void recordOrCancel(String subscriptionId, FirstPosition firstPosition, CheckpointRegistration registration) {
        try {
            firstPosition.recordOnceAccepted();
        } catch (RuntimeException | Error refusal) {
            // Ended before the cancel, since a wrapped model's cancel can wait for the thread whose evaluation waits
            // for the position
            firstPosition.subscribeEnded();
            try {
                subscriptionModel.cancelSubscription(subscriptionId);
            } catch (RuntimeException | Error cancelFailure) {
                track(subscriptionId, registration);
                refusal.addSuppressed(new IllegalStateException("Cancelling subscription " + subscriptionId + " in the wrapped " +
                                                                "subscription model " + subscriptionModel.getClass().getName() +
                                                                " failed after its start position could not be recorded, so the " +
                                                                "wrapped model may still hold it. Call cancelSubscription(\"" +
                                                                subscriptionId + "\") to try the cancel again before subscribing " +
                                                                "it again.", cancelFailure));
            }
            throw refusal;
        }
    }

    // A version from writeVersionSource stamps notOlderThan. An empty answer or no source stamps any(). Always the
    // 3-arg save, never a choice between two.
    private CheckpointWriteCondition writeConditionFor(String subscriptionId) {
        if (writeVersionSource == null) {
            return CheckpointWriteCondition.any();
        }
        OptionalLong version = writeVersionSource.writeVersion(subscriptionId);
        return version.isPresent() ? CheckpointWriteCondition.notOlderThan(version.getAsLong()) : CheckpointWriteCondition.any();
    }

    // Runs on the subscriber's own thread once the wrapped model's subscribe has returned, so the refusal reaches the
    // caller, or earlier on the thread of an evaluation that comes before then. A refusal there records nothing, so the
    // subscriber's thread tries again once the wrapped subscribe has returned. Answers the checkpoint it recorded, for
    // the supplier's first evaluation, and null when something was stored already or the override let an unanswerable
    // source through.
    private @Nullable Checkpoint recordFirstPositionOrRefuse(String subscriptionId) {
        Checkpoint checkpoint = storage.read(subscriptionId);
        if (checkpoint != null) {
            return null;
        }
        Checkpoint globalCheckpoint = subscriptionModel.globalCheckpoint();
        if (globalCheckpoint == null) {
            if (config.startWhenNoStartPositionCanBeRecorded) {
                return null;
            }
            throw new IllegalStateException("The wrapped subscription model " + subscriptionModel.getClass().getName() +
                                            " answered nothing when asked for the current position for subscription " +
                                            subscriptionId + ", which is how it reports a problem it cannot resolve, and no " +
                                            "checkpoint is stored for the subscription either. Starting it anyway would begin " +
                                            "wherever the feed has reached, and a crash before the first checkpoint is saved " +
                                            "would then start over from wherever the feed has reached by that time, silently " +
                                            "skipping whatever was delivered and failed in between. The subscription is " +
                                            "therefore refused rather than started, so subscribe again once the model can " +
                                            "answer. To start anyway, accepting that loss " +
                                            "window, configure DurableSubscriptionModelConfig." +
                                            "startWhenNoStartPositionCanBeRecorded(true), or set " +
                                            "occurrent.subscription.start-when-no-start-position-can-be-recorded=true when " +
                                            "using the Spring Boot starter. A subscription with a checkpoint already stored " +
                                            "starts from that checkpoint and is never refused this way. Subscribing with a " +
                                            "StartAt of your own records no position and makes no such promise.");
        }
        return saveFirstPosition(subscriptionId, globalCheckpoint);
    }

    // Pinned with ifAbsent(), the same protocol ManualStartSubscriptionModel and ReactorDurableSubscriptionModel
    // use, rather than the read-then-write this replaced, which could overwrite a first checkpoint another node
    // wrote in between. A storage able to compare the two settles a lost race by position instead, through
    // resolveFirstCheckpointRace. One that cannot falls back to reading the stored position back and checking it
    // is the one this node itself computed.
    private Checkpoint saveFirstPosition(String subscriptionId, Checkpoint globalCheckpoint) {
        if (!storage.evaluatesWriteConditionsFor(subscriptionId)) {
            // Nothing here can make a storage that writes unconditionally do otherwise, so this is the write
            // before this method existed and two nodes recording a first position at the same moment keep the
            // race. Logged rather than refused, because refusing would take out a storage that has worked until
            // now over a capability it never claimed.
            log.warn("Checkpoint storage {} does not evaluate write conditions for subscription {}, so the first " +
                     "position recorded for it is written unconditionally. Two nodes recording a first position " +
                     "for this subscription at the same moment can then lose the events between the two positions. " +
                     "Answer true from evaluatesWriteConditionsFor(String) on a storage that does evaluate " +
                     "ifAbsent(), or use one of the storages Occurrent ships, to close that.",
                    storage.getClass().getName(), subscriptionId);
            return storage.save(subscriptionId, globalCheckpoint);
        }
        try {
            return storage.save(subscriptionId, globalCheckpoint, CheckpointWriteCondition.ifAbsent());
        } catch (CheckpointWriteConditionNotFulfilledException e) {
            return storage.resolveFirstCheckpointRace(subscriptionId, globalCheckpoint)
                          .orElseGet(() -> refuseUnlessTheStoredPositionIsTheOneRead(subscriptionId, globalCheckpoint));
        }
    }

    // Never lets StartPositionAlreadyPinnedException escape, though a storage failure still can. This runs inside
    // the StartAt.dynamic supplier below, which a wrapped model can evaluate under its own retry loop, the exact
    // case recordFirstPositionOrRefuse's own placement outside that supplier exists to avoid.
    // StartPositionAlreadyPinnedException here means another node's write already settled the position,
    // so its own positionStored is adopted instead of refusing. The rare case where the confirm-read behind that
    // exception itself found nothing or failed falls back to globalCheckpoint instead, the position this node
    // itself computed and would have started from had the race gone the other way. That risks a duplicate
    // delivery against whatever the other node's write actually holds, never a loss, unlike falling through to
    // the caller's model-default fallback a few lines below, which would skip everything between here and now.
    private Checkpoint saveFirstPositionOrAdoptWhatWon(String subscriptionId, Checkpoint globalCheckpoint) {
        try {
            return saveFirstPosition(subscriptionId, globalCheckpoint);
        } catch (StartPositionAlreadyPinnedException e) {
            return e.positionStored.orElse(globalCheckpoint);
        }
    }

    // Something was stored between the read above and this write, so it was written where this model cannot order
    // it against the position it read. Reading it back answers the only question that settles it, whether it
    // holds that same position. Anything else is refused rather than started from a position this node never
    // read, which would skip whatever lies between the two.
    private Checkpoint refuseUnlessTheStoredPositionIsTheOneRead(String subscriptionId, Checkpoint positionRead) {
        @Nullable Checkpoint stored;
        try {
            stored = storage.read(subscriptionId);
        } catch (RuntimeException e) {
            throw StartPositionAlreadyPinnedException.readingTheStoredPositionBackFailed(subscriptionId, positionRead, e);
        }
        if (stored == null) {
            throw StartPositionAlreadyPinnedException.readingTheStoredPositionBackFoundNothing(subscriptionId, positionRead);
        }
        if (positionRead.asString().equals(stored.asString())) {
            return stored;
        }
        throw new StartPositionAlreadyPinnedException(subscriptionId, positionRead, stored);
    }

    @Nullable
    private StartAt generateStartAtPositionFrom(String subscriptionId, StartAt originalStartAt, Consumer<FirstPosition> firstPositionToRecord) {
        final StartAt startAtToUse;
        if (originalStartAt.isDefault()) {
            FirstPosition firstPosition = new FirstPosition(subscriptionId);
            firstPositionToRecord.accept(firstPosition);
            StartAt startAtIfNoSubscriptionFound = StartAt.subscriptionModelDefault();
            startAtToUse = StartAt.dynamic(() -> {
                Checkpoint recorded = firstPosition.forEvaluation();
                if (recorded != null) {
                    return StartAt.checkpoint(recorded);
                }
                // Read inside the supplier so a retry picks up the latest checkpoint, not a stale one
                Checkpoint checkpoint = storage.read(subscriptionId);
                if (checkpoint == null) {
                    Checkpoint globalCheckpoint = subscriptionModel.globalCheckpoint();
                    if (globalCheckpoint != null) {
                        checkpoint = saveFirstPositionOrAdoptWhatWon(subscriptionId, globalCheckpoint);
                    }
                }

                return checkpoint == null ? startAtIfNoSubscriptionFound : StartAt.checkpoint(checkpoint);
            });
        } else if (originalStartAt.isDynamic()) {
            var subscriptionModelContext = new SubscriptionModelContext(DurableSubscriptionModel.class);
            var nextStartAt = originalStartAt.get(subscriptionModelContext);
            if (nextStartAt != null) {
                return generateStartAtPositionFrom(subscriptionId, nextStartAt, firstPositionToRecord);
            }
            return null;
        } else {
            startAtToUse = originalStartAt;
        }
        return startAtToUse;
    }

    @Override
    public void stop() {
        getWrappedSubscriptionModel().stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        getWrappedSubscriptionModel().start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return getWrappedSubscriptionModel().isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return getWrappedSubscriptionModel().isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return getWrappedSubscriptionModel().isPaused(subscriptionId);
    }

    /**
     * Resume a paused subscription from the checkpoint stored for it, rather than from the position the wrapped
     * model itself last read to. Those two agree for a subscription only this model ever drives, but not for one a
     * {@code CompetingConsumerSubscriptionModel} pauses and resumes on lease handover, where another node can have
     * moved the checkpoint forward while this node held no lease at all, and its own wrapped model has no way to
     * know that happened.
     * <p>
     * Falls back to the wrapped model's own {@link SubscriptionModelLifeCycle#resumeSubscription(String)} when no
     * checkpoint is stored yet, when the wrapped model does not implement {@link RepositionableSubscriptions},
     * or when the subscription opted out of this model's checkpoint management in the first place (see
     * {@link #subscribe(String, SubscriptionFilter, StartAt, Consumer)}). The fallback is deliberately the wrapped
     * model's own tracked position, never {@link StartAt#subscriptionModelDefault()}, which resolves to the
     * present and would silently drop whatever was published while this subscription was paused.
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        // Held for the whole decision, reposition call included, so a concurrent subscribe or cancelSubscription
        // for this id cannot land between the marker check and acting on it.
        return underLockFor(subscriptionId, () -> {
            if (!notCheckpointedSubscriptions.contains(subscriptionId)) {
                Optional<RepositionableSubscriptions> repositionable = RepositionableSubscriptions.findIn(getWrappedSubscriptionModel());
                if (repositionable.isPresent()) {
                    Checkpoint checkpoint = storage.read(subscriptionId);
                    if (checkpoint != null) {
                        return repositionable.get().resumeSubscription(subscriptionId, StartAt.checkpoint(checkpoint));
                    }
                }
            }
            return getWrappedSubscriptionModel().resumeSubscription(subscriptionId);
        });
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        getWrappedSubscriptionModel().pauseSubscription(subscriptionId);
    }

    /**
     * Cancel a subscription. This means that it'll no longer receive events as they are persisted to the event store.
     * The checkpoint that is persisted in the {@link CheckpointStorage} will also be removed.
     *
     * @param subscriptionId The subscription id to cancel
     */
    @Override
    public void cancelSubscription(String subscriptionId) {
        runUnderLockFor(subscriptionId, () -> {
            subscriptionModel.cancelSubscription(subscriptionId);
            // The wrapped model doesn't wait for an action that is running, so its checkpoint is written before this
            // deletes it or not at all
            CheckpointRegistration registration = registrations.remove(subscriptionId);
            if (registration == null) {
                storage.delete(subscriptionId);
            } else {
                registration.cancelled(() -> storage.delete(subscriptionId));
            }
            notCheckpointedSubscriptions.remove(subscriptionId);
            checkpointedSubscriptions.remove(subscriptionId);
        });
    }

    @Override
    @PreDestroy
    public void shutdown() {
        // Removed even when shutting the wrapped model down throws, since a wrapped model that outlives this model
        // would otherwise keep it reachable and keep telling it about lost history
        try {
            subscriptionModel.shutdown();
        } finally {
            HistoryLossReportingSubscriptions.findIn(subscriptionModel).ifPresent(model -> model.removeHistoryLossListener(historyLossListener));
            QuietPositionReportingSubscriptions.findIn(subscriptionModel).ifPresent(model -> model.removeQuietPositionListener(quietPositionListener));
        }
    }

    @Nullable
    @Override
    public Checkpoint globalCheckpoint() {
        return subscriptionModel.globalCheckpoint();
    }

    @Override
    public CheckpointAwareSubscriptionModel getWrappedSubscriptionModel() {
        return subscriptionModel;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DurableSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("subscriptionModel=" + subscriptionModel)
                .add("storage=" + storage)
                .add("config=" + config)
                .toString();
    }

    // The first position of a subscribe with the model default, handed to the first evaluation of the start position
    // that finds it recorded. An evaluation while the subscribe records it waits for it. An evaluation before the
    // wrapped model's subscribe returns records it itself, because the wrapped model may wait for that evaluation, and
    // throws when recording fails
    private final class FirstPosition {
        private final String subscriptionId;
        private final ReentrantLock recording = new ReentrantLock();
        private final CompletableFuture<@Nullable Checkpoint> recorded = new CompletableFuture<>();
        private final AtomicBoolean handedOut = new AtomicBoolean();
        // Read and written only while holding recording
        private boolean wrappedModelReturned;

        FirstPosition(String subscriptionId) {
            this.subscriptionId = subscriptionId;
        }

        void recordOnceAccepted() {
            recording.lock();
            try {
                wrappedModelReturned = true;
                recordUnlessRecorded();
            } finally {
                recording.unlock();
            }
        }

        // Fails an evaluation still waiting when the subscribe ended without recording a position
        void subscribeEnded() {
            recorded.cancel(false);
        }

        // Null for every evaluation after the first, which reads what is stored by then, and when nothing was recorded
        @Nullable Checkpoint forEvaluation() {
            recording.lock();
            try {
                if (!wrappedModelReturned) {
                    recordUnlessRecorded();
                }
            } finally {
                recording.unlock();
            }
            Checkpoint position = awaitRecorded();
            return handedOut.compareAndSet(false, true) ? position : null;
        }

        private void recordUnlessRecorded() {
            if (!recorded.isDone()) {
                recorded.complete(recordFirstPositionOrRefuse(subscriptionId));
            }
        }

        private @Nullable Checkpoint awaitRecorded() {
            try {
                return recorded.get();
            } catch (CancellationException | ExecutionException e) {
                throw new IllegalStateException("Subscription " + subscriptionId + " was refused before its start position was recorded, so it has none.", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for the start position of subscription " + subscriptionId + " to be recorded.", e);
            }
        }
    }

    // One for each subscribe of an id this model stores checkpoints for
    private static final class CheckpointRegistration {
        // When a checkpoint was last written for the id, as System.nanoTime(). Starts now, so the first quiet position
        // is saved one interval after the subscribe
        final AtomicLong lastWrite = new AtomicLong(System.nanoTime());
        // Held while a delivery starts or finishes and while a quiet position is saved, so no quiet position is saved
        // while a delivery of any run of this subscribe is under way
        private final ReentrantLock deliveryLock = new ReentrantLock();
        // True only while no delivery is under way and the current delivery that started last stored the checkpoint
        // of its event. True before the first event, since no event can then come before the quiet position without a
        // checkpoint of its own
        private volatile boolean quietSaveAllowed = true;
        private int deliveriesUnderWay;
        // Counts the deliveries of every run of this subscribe
        private long deliveries;
        // Numbers the reads of every run of this subscribe. A run reads and delivers on one thread, and asks the
        // listener before each read, so a delivery belongs to the read its thread made last
        private final AtomicLong reads = new AtomicLong();
        private final ThreadLocal<Long> lastReadOnThisThread = ThreadLocal.withInitial(() -> 0L);
        // A delivery is current unless a current delivery started before it belongs to a later read. One that isn't
        // comes from a run a pause closed after it read, and the run that resumed has read and delivered past it
        private long readOfLatestCurrentDelivery;
        private long latestCurrentDelivery;
        // Whether the current delivery that started last stored its checkpoint, once it has finished. An action a
        // pause stopped waiting for can still return, or still be called, after a resume has delivered later events,
        // and what it stored says nothing about them
        private boolean latestStored = true;
        // Held while the checkpoint for an event is written, and by a cancel while it deletes the checkpoint
        private final ReentrantLock saveLock = new ReentrantLock();
        private boolean cancelled;

        void reading() {
            lastReadOnThisThread.set(reads.incrementAndGet());
        }

        long delivering() {
            long read = lastReadOnThisThread.get();
            deliveryLock.lock();
            try {
                quietSaveAllowed = false;
                deliveriesUnderWay++;
                long delivery = ++deliveries;
                if (read >= readOfLatestCurrentDelivery) {
                    readOfLatestCurrentDelivery = read;
                    latestCurrentDelivery = delivery;
                }
                return delivery;
            } finally {
                deliveryLock.unlock();
            }
        }

        void delivered(long delivery, boolean stored) {
            deliveryLock.lock();
            try {
                deliveriesUnderWay--;
                if (delivery == latestCurrentDelivery) {
                    latestStored = stored;
                }
                quietSaveAllowed = deliveriesUnderWay == 0 && latestStored;
            } finally {
                deliveryLock.unlock();
            }
        }

        // Read without the lock only to skip reading the write condition for a save that would be refused
        boolean quietSaveAllowed() {
            return quietSaveAllowed;
        }

        void saveQuietPositionIfAllowed(Runnable save) {
            deliveryLock.lock();
            try {
                if (quietSaveAllowed) {
                    save.run();
                }
            } finally {
                deliveryLock.unlock();
            }
        }

        // Whether it saved
        boolean saveUnlessCancelled(Runnable save) {
            saveLock.lock();
            try {
                if (cancelled) {
                    return false;
                }
                save.run();
                return true;
            } finally {
                saveLock.unlock();
            }
        }

        void cancelled(Runnable deleteCheckpoint) {
            saveLock.lock();
            try {
                cancelled = true;
                deleteCheckpoint.run();
            } finally {
                saveLock.unlock();
            }
        }
    }

    private static final class IdLock {
        final ReentrantLock lock = new ReentrantLock();
        // Changed only inside compute and computeIfPresent of idLocks, which run one at a time for an id
        private int callers = 1;

        IdLock oneMoreCaller() {
            callers++;
            return this;
        }

        boolean oneCallerLess() {
            return --callers == 0;
        }
    }
}