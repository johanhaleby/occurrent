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

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
 * and {@link #cancelSubscription(String)} tries the cancel again and keeps the checkpoint stored for the id, which the
 * held subscription itself, an earlier run or another node may have written. A subscription with a
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
 * An evaluation of the start position never waits for {@code subscribe(..)} to return. When {@code subscribe(..)} or
 * another evaluation of the same subscribe has decided the position or is writing it to storage, the evaluation takes
 * that outcome, after waiting for a write still under way. Otherwise it decides the position itself, from the
 * checkpoint stored or by recording the wrapped model's position. It is refused when nothing is stored and the wrapped
 * model answers no position, unless
 * {@link DurableSubscriptionModelConfig#startWhenNoStartPositionCanBeRecorded(boolean) startWhenNoStartPositionCanBeRecorded(true)}
 * is configured, and when that recording fails.
 * <p>
 * The first evaluation that takes the outcome starts from the position recorded there, when one was. Every other
 * evaluation, and that one when nothing was recorded, reads the stored checkpoint and starts from it. When nothing is
 * stored, it records a position the way another node would, or starts from the wrapped model's default and stores
 * nothing when the wrapped model answers no position. It is refused instead of recording that position once a cancel or
 * a later {@code subscribe(..)} of the id has stopped the checkpoint writes of its subscribe. On a storage that
 * evaluates write conditions, an evaluation whose write loses starts from the position
 * {@link CheckpointStorage#resolveFirstCheckpointRace(String, Checkpoint)} answers, or else from the one it reads back,
 * and is refused when that read fails or finds nothing. On a storage that doesn't, its write replaces whatever is
 * stored.
 * <p>
 * When a wrapped model's {@code subscribe(..)} throws, this model cancels nothing on the wrapped model, as in 0.33.0,
 * since a subscription the wrapped model holds for the id may belong to another subscribe. A wrapped model of your own
 * that held the id before it threw still holds that subscription, and any position an evaluation of the start position
 * stored stays stored.
 * <p>
 * For a subscribe with {@link StartAt#subscriptionModelDefault()}, an evaluation of the start position that starts
 * after this model's {@code subscribe(..)} threw can fail with {@link IllegalStateException}, and the subscription the
 * wrapped model still holds may then get no start position. In 0.33.0 that evaluation returned a start position.
 * <p>
 * For a subscribe with {@link StartAt#subscriptionModelDefault()}, when the wrapped model evaluated the start position
 * before it threw, the exception has a suppressed exception saying the wrapped model may still hold a subscription for
 * the id, unless the exception is {@link DuplicateSubscriptionIdException}, whose id belongs to another subscribe. When
 * nothing else subscribed the id, {@code getWrappedSubscriptionModel().cancelSubscription(id)} frees that subscription
 * and keeps the checkpoint stored for the id, while {@link #cancelSubscription(String)} can delete that checkpoint as
 * well. It also stops the checkpoint writes of the held subscription before it deletes anything, so an action that
 * returns after the cancel doesn't write its checkpoint back, and an evaluation of its start position records no
 * position. A later {@code subscribe(..)} of the id, before it hands the wrapped model anything, stops the checkpoint
 * an action of the held subscription writes and the first position an evaluation of its start position records, so
 * neither overwrites a checkpoint that subscribe stores. A later {@code subscribe(..)} that the wrapped model refuses
 * stops them too, so the held subscription then stores no checkpoint for an event it delivers until a cancel of the
 * id, and an evaluation of its start position that finds nothing stored is refused. Until a cancel or a later
 * {@code subscribe(..)} of the id, the held subscription writes its checkpoints.
 * <p>
 * This model stores the position a wrapped {@link QuietPositionReportingSubscriptions} reports for a quiet
 * subscription, and the one a wrapped {@link HistoryLossReportingSubscriptions} restarts a subscription from after its
 * history was lost, for the id rather than for one subscribe. When a wrapped model of your own reports either for the
 * held subscription, that position can replace the checkpoint of a later {@code subscribe(..)} of the id.
 * <p>
 * A wrapped model of your own has three requirements that this model doesn't check:
 * <ul>
 * <li>It refuses an id it already holds before it evaluates the start position. A model that evaluates first can
 * store a position for a subscribe it then refuses, as in 0.33.0.</li>
 * <li>When its evaluation inside {@code subscribe(..)} throws, its {@code subscribe(..)} throws as well and holds no
 * subscription for the id. The evaluation throws when the position can't be recorded, so a model that waits and
 * evaluates it again doesn't return from {@code subscribe(..)} until the position can be recorded.</li>
 * <li>Its {@code globalCheckpoint()} doesn't need a lock that its {@code pauseSubscription(..)}, or another of its
 * lifecycle calls, holds while it waits for the thread evaluating the start position. An evaluation that finds no
 * position recorded or stored calls {@code globalCheckpoint()} itself, as every evaluation that found no checkpoint
 * stored did in 0.33.0, so neither the lifecycle call nor the evaluation would return.</li>
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
    // Kept so shutdown can remove the same instance it added
    private final HistoryLossReportingSubscriptions.HistoryLossListener historyLossListener = new HistoryLossReportingSubscriptions.HistoryLossListener() {
        @Override
        public void restartingAfterHistoryLoss(String subscriptionId, Checkpoint restartedFrom, BooleanSupplier stillCurrent) {
            storeRestartPositionAfterHistoryLoss(subscriptionId, restartedFrom, stillCurrent);
        }

        // Under the lock subscribe holds until it has recorded the id, so a restart that comes before that waits for
        // it rather than restarting from a present this model doesn't store
        @Override
        public boolean storesRestartPositionOf(String subscriptionId) {
            return underLockFor(subscriptionId, () -> checkpointedSubscriptions.contains(subscriptionId));
        }
    };
    private final QuietPositionReportingSubscriptions.QuietPositionListener quietPositionListener = this::quietPositionSaverFor;
    // The current subscribe of each id this model stores checkpoints for. A new object for every subscribe, so a read
    // that began before a cancel saves nothing for a later subscribe of the id
    private final ConcurrentMap<String, CheckpointRegistration> registrations = new ConcurrentHashMap<>();
    // The registration of an id's last subscribe that ended without being tracked. A wrapped model can still hold a run
    // that writes through it, so cancelSubscription and the next subscribe of the id stop its writes and remove it.
    // There is at most one per id, since every subscribe of the id removes the one before it. Held weakly, so it is
    // kept only as long as such a run, or an action it is still calling, holds it. Put and removed by id only under
    // the lock for the id. A collected one is removed by a later subscribe or cancel of any id, which takes no lock
    // of that id and removes the entry only while it still holds that same collected reference
    private final ConcurrentMap<String, UntrackedRegistration> untrackedRegistrations = new ConcurrentHashMap<>();
    private final ReferenceQueue<CheckpointRegistration> collectedUntrackedRegistrations = new ReferenceQueue<>();
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
    // to store, since the quiet position would move the checkpoint past it, and before the first event while no
    // position of the subscription is stored. The save checks that again. The read is
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
     *                               the cancel again and keeps the checkpoint stored for the id.
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
        // position waits for that write and takes its outcome, see FirstPosition.
        return underLockFor(subscriptionId, () -> {
            stopUntrackedRegistrations(subscriptionId);
            CheckpointRegistration registration = new CheckpointRegistration();
            AtomicReference<@Nullable FirstPosition> firstPositionToRecord = new AtomicReference<>();
            StartAt startAtToUse = generateStartAtPositionFrom(subscriptionId, startAt, registration, firstPositionToRecord::set);
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
                    // Stops writing for the same reason as a registration track replaces
                    CheckpointRegistration previous = registrations.remove(subscriptionId);
                    if (previous != null) {
                        previous.stopWriting();
                    }
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
            } catch (Throwable wrappedFailure) {
                // Nothing is cancelled, since a subscription the wrapped model holds for the id may belong to another
                // subscribe. For a duplicate it always does, so a duplicate gets no suppressed exception
                keepUntracked(subscriptionId, registration);
                if (firstPosition != null) {
                    firstPosition.subscribeEnded();
                    if (firstPosition.evaluated && !(wrappedFailure instanceof DuplicateSubscriptionIdException)) {
                        wrappedFailure.addSuppressed(mayStillHoldTheSubscription(subscriptionId));
                    }
                }
                throw wrappedFailure;
            }
            if (firstPosition != null) {
                try {
                    recordOrCancel(subscriptionId, firstPosition, registration);
                } finally {
                    firstPosition.subscribeEnded();
                }
            }
            track(subscriptionId, registration);
            return subscription;
        });
    }

    // Called once the delegate accepted this managed subscription, or once a failed cancel may have left the delegate
    // holding it. A previous subscribe may have left this id opted out and still active, and a duplicate id the
    // delegate refuses must leave that active subscription's marker alone rather than losing it to this failed attempt.
    // A registration this replaces, such as one a failed cancel kept, may belong to a run the wrapped model still
    // delivers to, so it writes no checkpoint from then on
    private void track(String subscriptionId, CheckpointRegistration registration) {
        notCheckpointedSubscriptions.remove(subscriptionId);
        checkpointedSubscriptions.add(subscriptionId);
        CheckpointRegistration previous = registrations.put(subscriptionId, registration);
        if (previous != null && previous != registration) {
            previous.stopWriting();
        }
    }

    // Runs under the lock for the id, before a subscribe hands the wrapped model anything, so once it has, no
    // registration an earlier subscribe of the id left untracked writes a checkpoint. A subscribe that is then refused
    // stops them too, which costs replays and refuses an evaluation of the held run's start position that finds nothing
    // stored. A tracked registration stops once a later subscribe of the id is tracked or opted out, since the wrapped
    // model may still deliver to it and refuse this subscribe
    private void stopUntrackedRegistrations(String subscriptionId) {
        removeCollectedUntrackedRegistrations();
        stopWritingOf(untrackedRegistrations.remove(subscriptionId));
    }

    // For a subscribe that ended without being tracked, while the wrapped model may still hold a run of it. Runs under
    // the lock for the id, after the subscribe removed the one before it, so this replaces nothing. One it did replace
    // would stop writing all the same
    private void keepUntracked(String subscriptionId, CheckpointRegistration registration) {
        removeCollectedUntrackedRegistrations();
        stopWritingOf(untrackedRegistrations.put(subscriptionId, new UntrackedRegistration(subscriptionId, registration, collectedUntrackedRegistrations)));
    }

    private static void stopWritingOf(@Nullable UntrackedRegistration untracked) {
        CheckpointRegistration registration = untracked == null ? null : untracked.get();
        if (registration != null) {
            registration.stopWriting();
        }
    }

    // A collected registration writes nothing, so removing its entry stops nothing and needs no lock for the id
    private void removeCollectedUntrackedRegistrations() {
        Reference<? extends CheckpointRegistration> collected;
        while ((collected = collectedUntrackedRegistrations.poll()) != null) {
            UntrackedRegistration untracked = (UntrackedRegistration) collected;
            untrackedRegistrations.remove(untracked.subscriptionId, untracked);
        }
    }

    // For tests, how many ids have an untracked registration kept
    int idsWithUntrackedRegistrations() {
        return untrackedRegistrations.size();
    }

    private IllegalStateException mayStillHoldTheSubscription(String subscriptionId) {
        return new IllegalStateException("The wrapped subscription model " + subscriptionModel.getClass().getName() +
                                         " evaluated the start position of subscription " + subscriptionId +
                                         " before its subscribe failed, so it may still hold a subscription for the id. " +
                                         "When nothing else subscribed the id, getWrappedSubscriptionModel()" +
                                         ".cancelSubscription(\"" + subscriptionId + "\") frees it and keeps the checkpoint " +
                                         "stored for the id, while cancelSubscription(\"" + subscriptionId + "\") on this " +
                                         "model can delete that checkpoint as well, and stops the checkpoint writes of " +
                                         "that subscription before it does.");
    }

    // Cancels the subscription the wrapped model accepted when recording its first position fails, so the caller gets
    // the refusal
    private void recordOrCancel(String subscriptionId, FirstPosition firstPosition, CheckpointRegistration registration) {
        try {
            firstPosition.recordOnceAccepted();
        } catch (RuntimeException | Error refusal) {
            // Ended before the cancel, so an evaluation that the cancel waits for stores nothing
            firstPosition.subscribeEnded();
            cancelAfterFailedSubscribe(subscriptionId, registration, refusal);
            throw refusal;
        }
    }

    // A wrapped model whose cancel fails may still hold the subscription, so it stays tracked like an accepted one and
    // writes no checkpoint once a later cancelSubscription has cancelled it or a later subscribe has replaced it. That
    // cancelSubscription keeps the stored checkpoint, which the held subscription itself, an earlier run or another
    // node may have written
    private void cancelAfterFailedSubscribe(String subscriptionId, CheckpointRegistration registration, Throwable failure) {
        try {
            subscriptionModel.cancelSubscription(subscriptionId);
            // The wrapped model doesn't wait for an action that is running
            keepUntracked(subscriptionId, registration);
        } catch (RuntimeException | Error cancelFailure) {
            registration.keepCheckpointWhenCancelled();
            track(subscriptionId, registration);
            failure.addSuppressed(new IllegalStateException("Cancelling subscription " + subscriptionId + " in the wrapped " +
                                                            "subscription model " + subscriptionModel.getClass().getName() +
                                                            " failed after subscribing it failed, so the wrapped model may " +
                                                            "still hold it. Call cancelSubscription(\"" + subscriptionId +
                                                            "\") to try the cancel again before subscribing it again. That " +
                                                            "call keeps the checkpoint stored for the subscription.", cancelFailure));
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

    // Thrown on the subscriber's own thread once the wrapped model's subscribe has returned, so the refusal reaches
    // the caller, or on the thread of an evaluation that finds no position settled. A refusal there settles nothing, so
    // the subscriber's thread still records the position, in the recording it already started or once the wrapped
    // subscribe has returned, and so can a later evaluation.
    private IllegalStateException noStartPositionCanBeRecorded(String subscriptionId) {
        return new IllegalStateException("The wrapped subscription model " + subscriptionModel.getClass().getName() +
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

    // Runs inside the StartAt.dynamic supplier below, for an evaluation that gets no recorded position, so for a
    // wrapped model that evaluates the StartAt on a thread of its own, what it throws reaches that evaluation and not
    // the caller of subscribe. A stored position that was read back is adopted, since every later evaluation starts
    // from it too. When the read back failed or found nothing, the stored position may be earlier than
    // globalCheckpoint, so the evaluation is refused rather than started from a position that could skip the events
    // between the two. An evaluation once storage can be read starts from what it holds then.
    private Checkpoint saveFirstPositionOrAdoptWhatWon(String subscriptionId, Checkpoint globalCheckpoint) {
        try {
            return saveFirstPosition(subscriptionId, globalCheckpoint);
        } catch (StartPositionAlreadyPinnedException e) {
            return e.positionStored.orElseThrow(() -> storedPositionCouldNotBeReadBack(subscriptionId, globalCheckpoint, e));
        }
    }

    // Keeps the refusal's cause, so a read back that failed still has one and a read back that found nothing still
    // has none, which is how StartPositionAlreadyPinnedException tells the two apart
    private static StartPositionAlreadyPinnedException storedPositionCouldNotBeReadBack(String subscriptionId, Checkpoint positionRead,
                                                                                       StartPositionAlreadyPinnedException refusal) {
        return new StartPositionAlreadyPinnedException(subscriptionId, positionRead, null,
                "No checkpoint was stored for subscription " + subscriptionId + " when its start position was " +
                "evaluated, so recording " + positionRead.asString() + " as its first position was tried. Storage " +
                "refused that write because a checkpoint was stored in between, and reading that checkpoint back " +
                "did not name it. It can hold an earlier position, so starting from " + positionRead.asString() +
                " could skip the events between the two, and the start is refused instead. Evaluating the start " +
                "position again once storage can be read starts the subscription from the checkpoint storage holds " +
                "then. Reading it back produced the refusal \"" + refusal.getMessage() + "\"",
                refusal.getCause());
    }

    // Something was stored between the read above and this write, so it was written where this model cannot order
    // it against the position it read. Reading it back answers the only question that settles it, whether it
    // holds that same position. Anything else is refused rather than started from a position this node never
    // read, which would skip whatever lies between the two. The refusal names the stored position when the read
    // back found one, and names none when that read failed or found nothing.
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

    // The registration learns once a position is stored for a subscription from the model default, the one read,
    // recorded or adopted, and before any evaluation returns it. It learns at most once for each subscribe, and never
    // for a refused one or for a StartAt of the caller's own. An evaluation that records a first position outside the
    // settled outcome writes it through the registration, so once the registration has stopped writing, it is refused
    @Nullable
    private StartAt generateStartAtPositionFrom(String subscriptionId, StartAt originalStartAt, CheckpointRegistration registration,
                                                Consumer<FirstPosition> firstPositionToRecord) {
        final StartAt startAtToUse;
        if (originalStartAt.isDefault()) {
            FirstPosition firstPosition = new FirstPosition(subscriptionId, registration::startPositionStored);
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
                        checkpoint = registration.saveFirstPositionUnlessCancelled(() -> saveFirstPositionOrAdoptWhatWon(subscriptionId, globalCheckpoint),
                                () -> new IllegalStateException("The checkpoint writes of this subscribe of " + subscriptionId + " were stopped, " +
                                                                "by a cancel or a later subscribe of the id, before this evaluation recorded a " +
                                                                "first position, so it gets none."));
                    }
                }
                if (checkpoint == null) {
                    return startAtIfNoSubscriptionFound;
                }
                firstPosition.positionStored();
                return StartAt.checkpoint(checkpoint);
            });
        } else if (originalStartAt.isDynamic()) {
            var subscriptionModelContext = new SubscriptionModelContext(DurableSubscriptionModel.class);
            var nextStartAt = originalStartAt.get(subscriptionModelContext);
            if (nextStartAt != null) {
                return generateStartAtPositionFrom(subscriptionId, nextStartAt, registration, firstPositionToRecord);
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
     * present and would silently drop whatever was published while this subscription was paused. For the same
     * reason a MongoDB model resumes from its own tracked position when the stored checkpoint is one it cannot read,
     * such as the position a catch-up stores while it replays.
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
     * The checkpoint that is persisted in the {@link CheckpointStorage} will also be removed, except after a
     * {@link #subscribe(String, SubscriptionFilter, StartAt, Consumer)} that threw with a suppressed exception about a
     * failed cancel. This call then tries that cancel again and keeps the checkpoint.
     * <p>
     * It also stops the checkpoint writes of every subscription the wrapped model may still hold after a
     * {@code subscribe(..)} of the id threw, before it deletes the checkpoint. An action of such a subscription that
     * returns after this call has returned writes no checkpoint, and an evaluation of its start position records none.
     *
     * @param subscriptionId The subscription id to cancel
     */
    @Override
    public void cancelSubscription(String subscriptionId) {
        runUnderLockFor(subscriptionId, () -> {
            subscriptionModel.cancelSubscription(subscriptionId);
            // The wrapped model doesn't wait for an action that is running, so its checkpoint is written before the
            // registrations are cancelled below or not at all. The untracked ones stop before anything is deleted
            stopUntrackedRegistrations(subscriptionId);
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
    public boolean canResumeFrom(Checkpoint checkpoint) {
        return subscriptionModel.canResumeFrom(checkpoint);
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

    // The first position of a subscribe with the model default. The subscribing thread records it once the wrapped
    // model's subscribe has returned, and so does every evaluation of the start position that finds it unsettled, the
    // same way two nodes each record one. It keeps these properties.
    // - No thread calls into the wrapped model while another waits for that call through this class. settling is
    //   held only to settle the outcome and write it to storage, never while reading the checkpoint or asking the
    //   wrapped model for its position.
    // - An evaluation never waits for the subscribing thread to call the wrapped model, so an evaluation that the
    //   wrapped subscribe waits for can't hang on the subscribing thread. It can wait for a storage write that the
    //   subscribing thread or another evaluation started.
    // - settle stores at most one first position for each subscribe on this node, by whoever takes settling first while
    //   nothing is settled. Every party after that takes that outcome, and the first evaluation that gets it returns
    //   what was stored. A later evaluation that finds nothing stored records outside settling, as two nodes do, under
    //   the registration's saveLock, and is refused once the registration has stopped writing.
    // - Nothing is stored once the subscribe ended without a settled position, so a subscribe the wrapped model
    //   refuses before evaluating stores nothing, and every evaluation from then on throws.
    // - A failed evaluation settles nothing, so a later evaluation or the subscribing thread records again. A
    //   RuntimeException on the subscribing thread ends the subscribe, unless an evaluation settled the position first.
    //   An Error there ends the subscribe either way, and a position an evaluation settled stays stored.
    // - positionStored runs once a position of the subscription is stored, whether read, recorded or adopted, and
    //   before any evaluation gets that position. It runs at most once. It doesn't run once the subscribe ended
    //   without a settled position, nor while nothing is stored because the override let an unanswerable source
    //   through.
    private final class FirstPosition {
        private static final Settled ENDED = new Settled(null);

        private final String subscriptionId;
        private final ReentrantLock settling = new ReentrantLock();
        // Null until settled, and written only while holding settling
        private volatile @Nullable Settled settled;
        private final AtomicBoolean handedOut = new AtomicBoolean();
        // Set first thing in every evaluation, so a subscribe the wrapped model failed can tell whether one started,
        // including one still recording
        private volatile boolean evaluated;
        private final Runnable positionStored;
        private final AtomicBoolean positionStoredRan = new AtomicBoolean();

        FirstPosition(String subscriptionId, Runnable positionStored) {
            this.subscriptionId = subscriptionId;
            this.positionStored = positionStored;
        }

        // Once a position of the subscription is stored. Only the first call runs it, since a later one would find
        // nothing left to allow
        void positionStored() {
            if (positionStoredRan.compareAndSet(false, true)) {
                positionStored.run();
            }
        }

        void recordOnceAccepted() {
            try {
                settle();
            } catch (RuntimeException refusal) {
                if (endUnlessSettled()) {
                    throw refusal;
                }
            }
        }

        void subscribeEnded() {
            endUnlessSettled();
        }

        // Null for every evaluation after the first that gets a settled position, which reads what is stored by
        // then, and when the settled outcome recorded nothing
        @Nullable Checkpoint forEvaluation() {
            evaluated = true;
            Settled outcome = settle();
            if (outcome == ENDED) {
                throw new IllegalStateException("Subscribing " + subscriptionId + " failed before this evaluation got its start position, so it gets none.");
            }
            return handedOut.compareAndSet(false, true) ? outcome.recorded : null;
        }

        private Settled settle() {
            Settled outcome = settled;
            if (outcome != null) {
                return outcome;
            }
            Checkpoint stored = storage.read(subscriptionId);
            Checkpoint position = stored == null ? subscriptionModel.globalCheckpoint() : null;
            if (stored == null && position == null && !config.startWhenNoStartPositionCanBeRecorded) {
                throw noStartPositionCanBeRecorded(subscriptionId);
            }
            settling.lock();
            try {
                outcome = settled;
                if (outcome == null) {
                    outcome = new Settled(position == null ? null : saveFirstPosition(subscriptionId, position));
                    if (stored != null || position != null) {
                        // Before the outcome is published, so no evaluation returns the position before it runs
                        positionStored();
                    }
                    settled = outcome;
                }
                return outcome;
            } finally {
                settling.unlock();
            }
        }

        private boolean endUnlessSettled() {
            settling.lock();
            try {
                if (settled == null) {
                    settled = ENDED;
                    return true;
                }
                return false;
            } finally {
                settling.unlock();
            }
        }
    }

    // What a first position settled on. A null recorded means nothing was recorded, because a checkpoint was stored
    // already or the override let an unanswerable source through
    private record Settled(@Nullable Checkpoint recorded) {
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
        // of its event. Before the first event, true once a position of the subscription is stored, see
        // startPositionStored
        private volatile boolean quietSaveAllowed;
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
        private boolean latestStored;
        // Held while the checkpoint for an event or a first position an evaluation records outside the settled outcome
        // is written, by a cancel while it marks this registration cancelled and, unless the checkpoint is kept, deletes
        // it, and by a replacement while it marks this registration cancelled. No other lock of this model is taken while
        // it is held
        private final ReentrantLock saveLock = new ReentrantLock();
        private boolean cancelled;
        // Set before the registration is tracked, for a subscribe that threw and failed to cancel the subscription
        private boolean keepCheckpointWhenCancelled;

        void reading() {
            lastReadOnThisThread.set(reads.incrementAndGet());
        }

        // A position of the subscription is stored, so before the first event a quiet save moves that position on and
        // never stores the first one for a subscription that stores no position of its own
        void startPositionStored() {
            deliveryLock.lock();
            try {
                if (deliveries == 0) {
                    latestStored = true;
                    quietSaveAllowed = true;
                }
            } finally {
                deliveryLock.unlock();
            }
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

        // What the save returned, or the refusal without saving once this registration writes nothing more
        Checkpoint saveFirstPositionUnlessCancelled(Supplier<Checkpoint> save, Supplier<IllegalStateException> refusal) {
            saveLock.lock();
            try {
                if (cancelled) {
                    throw refusal.get();
                }
                return save.get();
            } finally {
                saveLock.unlock();
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

        void keepCheckpointWhenCancelled() {
            keepCheckpointWhenCancelled = true;
        }

        // Writes nothing from then on and deletes nothing, leaving the stored checkpoint to the subscribe that replaced it
        // or to the cancel that stopped it
        void stopWriting() {
            saveLock.lock();
            try {
                cancelled = true;
            } finally {
                saveLock.unlock();
            }
        }

        void cancelled(Runnable deleteCheckpoint) {
            saveLock.lock();
            try {
                cancelled = true;
                if (!keepCheckpointWhenCancelled) {
                    deleteCheckpoint.run();
                }
            } finally {
                saveLock.unlock();
            }
        }
    }

    // Compared by identity, so removing a collected one never removes a later registration of its id
    private static final class UntrackedRegistration extends WeakReference<CheckpointRegistration> {
        final String subscriptionId;

        UntrackedRegistration(String subscriptionId, CheckpointRegistration registration, ReferenceQueue<CheckpointRegistration> collected) {
            super(registration, collected);
            this.subscriptionId = subscriptionId;
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