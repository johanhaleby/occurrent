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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * a {@link StartAt} of its own that doesn't resolve to the model default records no position and is never refused
 * either. This is the same answer
 * {@link ManualStartSubscriptionModel} gives for a {@code null} position source and the same one the reactor
 * {@code ReactorDurableSubscriptionModel} gives for the same registration.
 * {@link DurableSubscriptionModelConfig#startWhenNoStartPositionCanBeRecorded(boolean)} turns the refusal into a
 * start without a recorded position, accepting the loss window it documents.
 * <p>
 * With a {@code CheckpointWriteVersionSource} that answers a version, the first position is settled through
 * {@link CheckpointStorage#resolveFirstCheckpointRace(String, Checkpoint, long)}, which stores it and the version in one
 * write, and the subscription starts from the checkpoint the call answers. Of two nodes recording one at the same
 * moment, {@code NativeMongoCheckpointStorage} and {@code SpringMongoCheckpointStorage} store the earlier position.
 * When the storage holds a newer write version, the node that stored it holds a newer lease. Nothing is written, and
 * the subscribe throws the storage's {@link CheckpointWriteConditionNotFulfilledException}, the exception a refused
 * checkpoint write for an event throws. A {@code CompetingConsumerSubscriptionModel} over this model then has the
 * subscription wait for a grant of the lease again, and its subscribe returns without throwing.
 * A stored checkpoint that is the position read is kept, the version is stored with it, and the subscription starts
 * from it. When the storage holds the same version, an older one or none, and another checkpoint it can't compare with
 * the position read is stored, the call writes nothing and answers empty. On {@code SpringRedisCheckpointStorage} and
 * {@code InMemoryCheckpointStorage} that is every other stored checkpoint. On the MongoDB storages it is one stored for
 * a delivered event, a generic checkpoint, and a legacy one stored under {@code subscriptionPosition}. The lost race
 * is then settled as it is without a version, below, so the subscribe is refused with
 * {@link StartPositionAlreadyPinnedException} unless the stored checkpoint is the position read, as in 0.34.0. So on
 * these four storages, once every node runs Occurrent 0.35.0 or later, a first position written with the newest version
 * refuses the writes of every node with an older one from then on, apart from one sequence. A node's
 * {@code ifAbsent()} write is refused because a checkpoint is stored, a cancel on another node deletes that checkpoint,
 * a third node records a first position with a newer version, and then the first node's two-argument
 * {@code resolveFirstCheckpointRace} runs. On the MongoDB storages that call compares no version and can replace the
 * third node's position with an earlier one. 0.34.0 wrote the same on that sequence, and it delivers events again but
 * skips none. A node on 0.34.0 settles a lost race without a version, and on the MongoDB storages it can replace a
 * first position stored with a version with an earlier one, which delivers events again but skips none.
 * <p>
 * A subscription that starts from a checkpoint already stored, such as the start boundary the Spring Boot starter
 * stores for it before it subscribes, first stores the version with that checkpoint through
 * {@link CheckpointStorage#storeWriteVersion(String, long)}, which leaves the checkpoint stored as it is, and starts
 * from the checkpoint the call answers. So a node that has lost its lease can't store the checkpoint of a later event
 * over the checkpoint this node starts from. The subscribe does this, and so does every evaluation of the start
 * position that reads a stored checkpoint. When the storage holds a newer write version, the subscribe or the
 * evaluation throws that {@link CheckpointWriteConditionNotFulfilledException}. When nothing is stored any more, for
 * example because a cancel on another node deleted the checkpoint after the read, nothing is written and the
 * subscription starts from the checkpoint read without a version, as in 0.34.0. A storage that answers empty, as one
 * without the method does, gets no version stored either. An evaluation reads the checkpoint without the lock a cancel
 * takes. Once a cancel or a later {@code subscribe(..)} of the id has stopped the checkpoint writes of its subscribe,
 * settling the first position of that subscribe stores nothing and the subscribe is refused with
 * {@link IllegalStateException}. An evaluation after the first position settled stores nothing then, and starts from
 * the checkpoint it read, as in 0.34.0.
 * <p>
 * Without a version, or for a storage whose three-argument {@code resolveFirstCheckpointRace} answers empty, the first
 * position is written with
 * {@link CheckpointWriteCondition#ifAbsent()} and no write version, as in 0.34.0, so of two nodes recording one at the
 * same moment, only one write is stored. This model doesn't save the position again with the version, since that save
 * would write over whatever another node stored between the two writes. Until the next checkpoint write stores the
 * version, a node that has lost its lease can write over the first position, which delivers events again but skips
 * none. A {@link CheckpointStorage} that answers {@code false} from
 * {@link CheckpointStorage#evaluatesWriteConditionsFor(String)} could only write it unconditionally, and then the two
 * nodes could overwrite each other's and lose the events between the two positions. Such a subscription is refused with
 * {@link IllegalStateException} where a subscription with no position to record is, before anything is written for it.
 * A subscription with a checkpoint already stored, or with a {@link StartAt} of its own that doesn't resolve to the
 * model default, records no first position and is never refused this way. Every storage Occurrent ships evaluates
 * write conditions, apart from {@code SpringRedisCheckpointStorage} in Cluster-safe mode for an id
 * Redis Cluster can't put in one slot with its version key, and for an id in the reserved namespace of its version
 * key, which it refuses for every read and write anyway.
 * {@link DurableSubscriptionModelConfig#startWhenFirstPositionCanBeOverwritten(boolean)} writes the first position
 * unconditionally instead, with a warning, accepting that loss.
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
 * When the one that decides the position fails to write it, that failure is the outcome the others take. Every other
 * evaluation of the same subscribe is then refused with {@link IllegalStateException}, and {@code subscribe(..)}
 * throws what the write threw, with a checked exception wrapped in {@link IllegalStateException}, unless it failed on
 * its own before it took that outcome, in which case it throws its own exception. So a subscribe whose
 * evaluation was refused with {@link StartPositionAlreadyPinnedException} is refused too, and doesn't store this
 * node's version with the checkpoint another node stored.
 * <p>
 * The first evaluation that takes the outcome starts from the position recorded there, when one was. Every other
 * evaluation, and that one when nothing was recorded, reads the stored checkpoint and starts from it. When nothing is
 * stored, it records a position the way another node would, or starts from the wrapped model's default and stores
 * nothing when the wrapped model answers no position. It is refused instead of recording that position once a cancel or
 * a later {@code subscribe(..)} of the id has stopped the checkpoint writes of its subscribe. An evaluation with a
 * version that reads a stored checkpoint starts from the checkpoint {@link CheckpointStorage#storeWriteVersion(String, long)}
 * answers, or from the one it read when that call answers empty or the checkpoint writes of its subscribe have
 * stopped, and throws that {@link CheckpointWriteConditionNotFulfilledException} when the storage holds a newer write
 * version. The blocking MongoDB models don't retry it. One whose {@code ifAbsent()} write loses starts from the position
 * {@link CheckpointStorage#resolveFirstCheckpointRace(String, Checkpoint)} answers, or else from the one it reads back,
 * and is refused when that read fails or finds nothing. A storage
 * that doesn't evaluate write conditions had a checkpoint stored when the position was decided, since this model
 * refuses the subscribe otherwise. An evaluation there that finds nothing stored any more, for example because a
 * cancel on another node deleted the checkpoint, starts from the checkpoint read when the position was decided, also
 * once the checkpoint writes of its subscribe have stopped. That can deliver events again but skips none. With
 * {@link DurableSubscriptionModelConfig#startWhenFirstPositionCanBeOverwritten(boolean) startWhenFirstPositionCanBeOverwritten(true)}
 * the evaluation records a position instead, and its write replaces whatever is stored.
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
 * position and stores no write version. A later {@code subscribe(..)} of the id, before it hands the wrapped model
 * anything, stops the checkpoint an action of the held subscription writes, and the first position or the write
 * version an evaluation of its start position stores, so none of them overwrites a checkpoint that subscribe stores.
 * A later {@code subscribe(..)} that the wrapped model refuses stops them too, so the held subscription then stores no
 * checkpoint for an event it delivers until a cancel of the id. An evaluation of its start position that finds a
 * checkpoint stored then starts from it without storing a version, as in 0.34.0, and one that finds nothing stored is
 * refused unless the storage doesn't evaluate write conditions, where it starts from the checkpoint read when the
 * position was decided. Until a cancel or a later {@code subscribe(..)} of the id, the held subscription writes its
 * checkpoints.
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
public class DurableSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModelWrapper, RepositionableSubscriptions, CommitOrderedSubscriptions, RedeliveringSubscriptions {

    private static final Logger log = LoggerFactory.getLogger(DurableSubscriptionModel.class);

    private final CheckpointAwareSubscriptionModel subscriptionModel;
    // The stillWanted of a two-argument globalCheckpoint or canResumeFrom running on this thread, for the one-argument
    // method it calls
    private final ThreadLocal<BooleanSupplier> stillWantedOfTheCall = new ThreadLocal<>();
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
    // The registration a subscribe of the id has created and not yet tracked or left untracked, so a run the wrapped
    // model starts during the subscribe gets its reads numbered too. Removed by identity once the subscribe ends
    private final ConcurrentMap<String, CheckpointRegistration> subscribingRegistrations = new ConcurrentHashMap<>();
    private final ReferenceQueue<CheckpointRegistration> collectedUntrackedRegistrations = new ReferenceQueue<>();
    // Set by shutdown before it stops the registrations, and read after a registration is put in either map, so a
    // subscribe that puts one while shutdown runs stops it itself unless shutdown finds it
    private volatile boolean shutDown;
    // One lock per id, which exists while a call holds or waits for it. Per id, so a checkpoint store that hangs in
    // one id's call blocks only calls for that id. Removed once no call needs it, so an unknown or made-up id passed
    // to cancelSubscription or resumeSubscription leaves nothing behind. The checkpoint write for an event takes none
    // of these, only the lock of its own CheckpointRegistration, which a cancel of that id also takes. Neither is a
    // monitor, since a virtual thread that waits for the checkpoint store while it holds one keeps its carrier thread
    // on JDK 21 to 23
    private final ConcurrentMap<String, IdLock> idLocks = new ConcurrentHashMap<>();

    // The subscribes that ask their function before they take the lock for their id, kept until they hold it
    private final Set<Asked> asking = ConcurrentHashMap.newKeySet();

    // How long a resume of an id on another thread waits for the function of a subscribe of the id to return
    private static final Duration ASKED_WAIT = Duration.ofSeconds(10);

    private static final class Asked {
        private final String subscriptionId;
        private final Thread asker = Thread.currentThread();
        // Completes once the function has returned or thrown
        private final CompletableFuture<Void> returned = new CompletableFuture<>();
        // Completes once the subscribe holds the lock for the id or its function has thrown
        private final CompletableFuture<Void> settled = new CompletableFuture<>();
        // Set by a cancel on another thread before the subscribe holds the lock for the id
        private volatile boolean cancelled;

        private Asked(String subscriptionId) {
            this.subscriptionId = subscriptionId;
        }
    }

    // A read of a run, numbered across every run of a subscribe, with the count of resumes at a position when it was made
    record Read(long number, long repositions) {
    }

    // A resume at a position, by its count of resumes at a position and the position it stored, as Checkpoint.asString
    record Reposition(long count, @Nullable String position) {
        static final Reposition NONE = new Reposition(0, null);
    }

    // What a subscribe cancelled while its function was asked returns. It never started and nothing will start it
    private record CancelledWhileAsked(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return false;
        }
    }

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

    private void settle(Asked asked) {
        asking.remove(asked);
        asked.returned.complete(null);
        asked.settled.complete(null);
    }

    // A resume waits for the subscribes of the id whose function another thread asks, so it still comes after them. It
    // waits up to ASKED_WAIT for the functions to return, and then for those subscribes to take the lock with no limit,
    // as a resume always waited for the lock. The asking thread itself goes ahead. An interrupt doesn't end the wait,
    // and is set again once the wait is over
    private void waitForAsksOnOtherThreads(String subscriptionId) {
        Thread current = Thread.currentThread();
        List<Asked> asks = asking.stream().filter(asked -> asked.subscriptionId.equals(subscriptionId) && asked.asker != current).toList();
        if (asks.isEmpty()) {
            return;
        }
        CompletableFuture<Void> returned = CompletableFuture.allOf(asks.stream().map(asked -> asked.returned).toArray(CompletableFuture[]::new));
        long deadline = System.nanoTime() + ASKED_WAIT.toNanos();
        boolean interrupted = false;
        try {
            while (!returned.isDone()) {
                try {
                    returned.get(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    break;
                } catch (TimeoutException e) {
                    throw new IllegalStateException("Cannot resume subscription " + subscriptionId + ", because the StartAt.dynamic function of a subscribe of it on another thread didn't return within "
                            + ASKED_WAIT.toMillis() + " ms. Nothing was changed.");
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        // join doesn't end on an interrupt and keeps the flag set
        CompletableFuture.allOf(asks.stream().map(asked -> asked.settled).toArray(CompletableFuture[]::new)).join();
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
        // Added without a quiet position save interval too, since the listener numbers the reads that tell an event
        // read before a resume at a position from one read after it
        QuietPositionReportingSubscriptions.findIn(subscriptionModel)
                .ifPresent(model -> model.addQuietPositionListener(quietPositionListener));
    }

    // Answers nothing until the interval has passed since the last checkpoint write, so a subscription that stores a
    // checkpoint for an event at least once per interval gets no extra write. It also answers nothing while an event
    // is being delivered, or when the checkpoint stored isn't known to cover the current delivery that ended last, such
    // as one of an event the persist predicate declined to store, since the quiet position would move the checkpoint
    // past it, and before the first event while no position of the subscription is stored. The save checks that again,
    // and reads what is stored first when a write that threw left it unknown. The read is
    // numbered first, whatever the answer, so the next delivery on this thread is known to come from it. The write
    // condition is read here, before the wrapped model reads, so the save uses the token of the lease the read was
    // made under, like the write for an event. A source that cannot answer is asked again after the interval rather
    // than before every read
    private @Nullable Consumer<Checkpoint> quietPositionSaverFor(String subscriptionId) {
        Duration interval = config.quietPositionSaveInterval;
        // Read before the tracked and untracked registrations, since a subscribe tracks its registration or keeps it
        // untracked before it stops counting it as subscribing, so each read here finds it at least once
        CheckpointRegistration subscribing = subscribingRegistrations.get(subscriptionId);
        CheckpointRegistration registration = registrations.get(subscriptionId);
        numberReadOfUntrackedRegistrations(subscriptionId, subscribing, registration);
        if (registration == null) {
            return null;
        }
        Read read = registration.reading();
        if (interval == null || !registration.quietTickAllowed() || System.nanoTime() - registration.lastWrite.get() < interval.toNanos()) {
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
        return quietPosition -> saveQuietPosition(subscriptionId, quietPosition, writeCondition, registration, read);
    }

    // The listener gets the id and not the run, so the read is numbered for the registration a subscribe is creating and
    // for one a failed subscribe left writing too, and they get no quiet save. A run asks before each read and delivers
    // what it read on the same thread, so a read numbered for a registration that has no run on this thread changes
    // none of its deliveries
    private void numberReadOfUntrackedRegistrations(String subscriptionId, @Nullable CheckpointRegistration subscribing,
                                                    @Nullable CheckpointRegistration tracked) {
        if (subscribing != null && subscribing != tracked) {
            subscribing.reading();
        }
        UntrackedRegistration untracked = untrackedRegistrations.get(subscriptionId);
        CheckpointRegistration kept = untracked == null ? null : untracked.get();
        if (kept != null && kept != tracked && kept != subscribing) {
            kept.reading();
        }
    }

    // A refused write is thrown, so the wrapped model ends delivery on a node whose lease moved, as it does for an
    // event. Any other failure is logged and tried again after the interval, since the subscription has lost nothing.
    // So is a failed read of what is stored
    private void saveQuietPosition(String subscriptionId, Checkpoint quietPosition, CheckpointWriteCondition writeCondition, CheckpointRegistration registration,
                                   Read handedOutAt) {
        runUnderLockFor(subscriptionId, () -> {
            if (registrations.get(subscriptionId) != registration) {
                return;
            }
            registration.saveQuietPositionIfStoredCovers(handedOutAt, quietPosition, () -> {
                registration.lastWrite.set(System.nanoTime());
                try {
                    storage.save(subscriptionId, quietPosition, writeCondition);
                    registration.storedACheckpoint();
                    return true;
                } catch (CheckpointWriteConditionNotFulfilledException e) {
                    throw e;
                } catch (RuntimeException e) {
                    log.warn("Failed to save the quiet position of subscription {}. Trying again in {}.", subscriptionId, config.quietPositionSaveInterval, e);
                    return false;
                }
            }, () -> {
                try {
                    return storage.read(subscriptionId);
                } catch (Throwable e) {
                    // Throwable, since a storage written in Kotlin can throw a checked exception
                    registration.lastWrite.set(System.nanoTime());
                    log.warn("Failed to read the checkpoint of subscription {} after a checkpoint write failed, so its quiet position is not saved this time. Trying again in {}.",
                            subscriptionId, config.quietPositionSaveInterval, e);
                    throw e;
                }
            });
        });
    }

    // Stored now rather than with the next event, since a process stopping before that event would restart from
    // the lost position and skip everything written in between. Same write condition as any other checkpoint. Asked
    // under the lock resumeSubscription and cancelSubscription hold, so a run that a resume replaced or a cancel ended
    // while it asked for the present stores nothing. Written under the deliveryLocks and saveLocks of the
    // registrations writing for the id, like a put-back, so no delivery ends and no quiet position is saved before
    // they note what the write left stored
    private void storeRestartPositionAfterHistoryLoss(String subscriptionId, Checkpoint restartedFrom, BooleanSupplier stillCurrent) {
        runUnderLockFor(subscriptionId, () -> {
            if (!checkpointedSubscriptions.contains(subscriptionId) || !stillCurrent.getAsBoolean()) {
                return;
            }
            CheckpointWriteCondition writeCondition = writeConditionFor(subscriptionId);
            List<CheckpointRegistration> writing = registrationsWritingFor(subscriptionId);
            List<ReentrantLock> locks = new ArrayList<>(deliveryLocksOf(writing));
            locks.addAll(saveLocksOf(writing));
            underLocks(locks, () -> {
                try {
                    storage.save(subscriptionId, restartedFrom, writeCondition);
                    writing.forEach(registration -> registration.restartPositionStored(restartedFrom));
                } catch (CheckpointWriteConditionNotFulfilledException e) {
                    writing.forEach(CheckpointRegistration::storedUnknown);
                    log.warn("Did not store the position subscription {} restarts from after its history was lost, since another node has written its checkpoint with a newer lease: {}",
                            subscriptionId, e.getMessage());
                } catch (Throwable e) {
                    // Throwable, since a storage written in Kotlin can throw a checked exception
                    writing.forEach(registration -> registration.restartPositionMayBeStored(restartedFrom));
                    throw e;
                } finally {
                    writing.forEach(CheckpointRegistration::decideQuietTicks);
                }
            });
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
     *                               model can answer, pass a {@link StartAt} of your own that doesn't resolve to the
     *                               model default, which records no position
     *                               and makes no resume promise, or configure
     *                               {@link DurableSubscriptionModelConfig#startWhenNoStartPositionCanBeRecorded(boolean)}.
     *                               When that cancel throws too, the exception has a suppressed exception saying the
     *                               wrapped model may still hold the id, and {@link #cancelSubscription(String)} tries
     *                               the cancel again and keeps the checkpoint stored for the id. Thrown the same way,
     *                               and with the same cancel, when {@code startAt} resolves to the model default, no
     *                               checkpoint is stored for {@code subscriptionId}, and the storage answers
     *                               {@code false} from {@link CheckpointStorage#evaluatesWriteConditionsFor(String)},
     *                               unless {@link DurableSubscriptionModelConfig#startWhenFirstPositionCanBeOverwritten(boolean)}
     *                               is set.
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

        // The caller's function runs here, with no lock held, since it can wait for a cancel of the id on another thread.
        // A cancel made before this takes the lock finds asked, and then nothing reaches the wrapped model. A start that
        // isn't dynamic asks nothing, so a cancel doesn't stop it, as before the function was asked ahead of the lock
        Asked asked = new Asked(subscriptionId);
        if (startAt.isDynamic()) {
            asking.add(asked);
        }
        StartAt answered = startAt;
        try {
            while (answered != null && !answered.isDefault() && answered.isDynamic()) {
                answered = answered.get(new SubscriptionModelContext(DurableSubscriptionModel.class));
            }
        } catch (Throwable e) {
            // Throwable, since a Kotlin function can throw a checked exception, and an entry left in asking makes every
            // later resume of the id on another thread throw after 10 seconds
            settle(asked);
            // A subscribe whose function throws still stops what an earlier subscribe of the id left untracked, as when
            // the function was asked under the lock
            try {
                runUnderLockFor(subscriptionId, () -> stopUntrackedRegistrations(subscriptionId));
            } catch (Throwable stopFailure) {
                // Throwable, since a wrapped model written in Kotlin can throw a checked exception
                if (stopFailure != e) {
                    e.addSuppressed(stopFailure);
                }
            }
            throw e;
        }
        asked.returned.complete(null);
        StartAt askedOnce = answered;
        // Held for the rest of the method, not just the opt-out branch, so subscribe, resumeSubscription and
        // cancelSubscription for the same id stay serialized against notCheckpointedSubscriptions (see the field
        // comment above). The blocking MongoDB models evaluate the returned StartAt on their executor each time
        // they open a change stream, outside this lock, so a cancelSubscription can run while a later evaluation
        // reads the checkpoint or writes a first position. An evaluation that comes while this records the first
        // position waits for that write and takes its outcome, see FirstPosition.
        AtomicReference<@Nullable CheckpointRegistration> subscribing = new AtomicReference<>();
        return forgottenOnceSubscribed(subscriptionId, subscribing, () -> underLockFor(subscriptionId, () -> {
            settle(asked);
            if (asked.cancelled) {
                return new CancelledWhileAsked(subscriptionId);
            }
            stopUntrackedRegistrations(subscriptionId);
            CheckpointRegistration registration = new CheckpointRegistration();
            subscribing.set(registration);
            subscribingRegistrations.put(subscriptionId, registration);
            AtomicReference<@Nullable FirstPosition> firstPositionToRecord = new AtomicReference<>();
            StartAt startAtToUse = askedOnce == null ? null : generateStartAtPositionFrom(subscriptionId, askedOnce, registration, firstPositionToRecord::set);
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
                CheckpointRegistration.Delivery delivery = registration.delivering();
                @Nullable Checkpoint toStore = null;
                try {
                    // Read before the action runs, so the write uses the token of the lease this event was
                    // delivered under, even if this node lost that lease and won a newer one meanwhile
                    CheckpointWriteCondition writeCondition = writeConditionFor(subscriptionId);
                    action.accept(cloudEvent);
                    if (persistCheckpoint.test(cloudEvent)) {
                        Checkpoint checkpoint = getCheckpointOrThrowIAE(cloudEvent);
                        toStore = checkpoint;
                        registration.saveUnlessCancelledRepositionedOrSuperseded(delivery, checkpoint, () -> {
                            storage.save(subscriptionId, checkpoint, writeCondition);
                            registration.lastWrite.set(System.nanoTime());
                        });
                    }
                } finally {
                    registration.delivered(delivery, toStore);
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
            if (askedOnce instanceof StartAt.StartAtCheckpoint && RepositionableSubscriptions.findIn(getWrappedSubscriptionModel()).isPresent()) {
                // Unknown until the read returns, so when an Error from the read is followed by a failed cancel, a resume of
                // the subscription the wrapped model still holds goes on from that model's own position
                registration.ownStartAfter(StoredBefore.UNKNOWN);
                try {
                    registration.ownStartAfter(storedBeforeOwnStart(subscriptionId));
                } catch (Error readFailure) {
                    // The caller sees the subscribe fail, so the subscription the wrapped model accepted is untracked and
                    // cancelled. A failed cancel tracks it again
                    registrations.remove(subscriptionId, registration);
                    checkpointedSubscriptions.remove(subscriptionId);
                    cancelAfterFailedSubscribe(subscriptionId, registration, readFailure);
                    throw readFailure;
                }
            }
            return subscription;
        }));
    }

    // By identity, since a later subscribe of the id can have put its own registration once the lock was let go
    private Subscription forgottenOnceSubscribed(String subscriptionId, AtomicReference<@Nullable CheckpointRegistration> subscribing,
                                                 Supplier<Subscription> subscribe) {
        try {
            return subscribe.get();
        } finally {
            CheckpointRegistration registration = subscribing.get();
            if (registration != null) {
                subscribingRegistrations.remove(subscriptionId, registration);
            }
        }
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
        if (shutDown) {
            registration.stopWriting();
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
        if (shutDown) {
            registration.stopWriting();
        }
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

    // For tests, how many positions the tracked registration of the id compares what is stored with before a quiet
    // save, apart from those of resumes at a position
    int positionsRememberedForQuietSaves(String subscriptionId) {
        CheckpointRegistration registration = registrations.get(subscriptionId);
        return registration == null ? 0 : registration.positionsRemembered();
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
        } catch (Throwable refusal) {
            // Throwable, since a storage or wrapped model written in Kotlin can throw a checked exception
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
        } catch (Throwable cancelFailure) {
            // Throwable, since a wrapped model written in Kotlin can throw a checked exception
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
                                         "StartAt of your own that doesn't resolve to the model default records no " +
                                         "position and makes no such promise.");
    }

    // Thrown where noStartPositionCanBeRecorded is, before anything is written for the subscription, so it reaches the
    // caller of subscribe, or the evaluation that would have recorded the first position
    private IllegalStateException firstPositionCanBeOverwritten(String subscriptionId) {
        return new IllegalStateException("Checkpoint storage " + storage.getClass().getName() + " does not evaluate " +
                                         "write conditions for subscription " + subscriptionId + ", so the first " +
                                         "position recorded for it could only be written unconditionally. Two nodes " +
                                         "recording a first position for this subscription at the same moment could " +
                                         "then overwrite each other's and lose the events between the two positions. " +
                                         "The subscription is therefore refused before anything is written. Use a " +
                                         "storage that evaluates CheckpointWriteCondition.ifAbsent() and answers true " +
                                         "from evaluatesWriteConditionsFor(String) for this subscription. To start " +
                                         "anyway, accepting that loss, configure " +
                                         "DurableSubscriptionModelConfig.startWhenFirstPositionCanBeOverwritten(true), " +
                                         "or, when a Spring Boot starter built this model, set " +
                                         "occurrent.subscription.start-when-first-position-can-be-overwritten=true. " +
                                         "A subscription with a checkpoint already " +
                                         "stored, or with a StartAt of your own that doesn't resolve to the model " +
                                         "default, records no first position and is never refused this way.");
    }

    // Pinned with ifAbsent(), the same protocol ManualStartSubscriptionModel and ReactorDurableSubscriptionModel
    // use, rather than the read-then-write this replaced, which could overwrite a first checkpoint another node
    // wrote in between. A storage able to compare the two settles a lost race by position instead, through
    // resolveFirstCheckpointRace. One that cannot falls back to reading the stored position back and checking it
    // is the one this node itself computed. With a write version, the three-argument resolveFirstCheckpointRace
    // settles it first and stores the version in the same write. When it answers empty, which it does for a stored
    // checkpoint it can't order, the ifAbsent() write and the read back settle it with no version, as in
    // 0.34.0, since a second save of the version would write over whatever another node stored between the two writes
    private Checkpoint saveFirstPosition(String subscriptionId, Checkpoint globalCheckpoint) {
        if (!storage.evaluatesWriteConditionsFor(subscriptionId)) {
            // Nothing here can make a storage that writes unconditionally do otherwise, so two nodes recording a
            // first position at the same moment would race. Refused before anything is written, unless the config
            // accepts that loss. Settling refuses this first while the storage answers the same for the id, so this
            // refuses a storage whose answer changed since
            if (!config.startWhenFirstPositionCanBeOverwritten) {
                throw firstPositionCanBeOverwritten(subscriptionId);
            }
            log.warn("Checkpoint storage {} does not evaluate write conditions for subscription {}, so the first " +
                     "position recorded for it is written unconditionally. Two nodes recording a first position " +
                     "for this subscription at the same moment can then lose the events between the two positions. " +
                     "Answer true from evaluatesWriteConditionsFor(String) on a storage that does evaluate " +
                     "ifAbsent(), or use one of the storages Occurrent ships, to close that.",
                    storage.getClass().getName(), subscriptionId);
            return storage.save(subscriptionId, globalCheckpoint);
        }
        OptionalLong writeVersion = writeVersionOfFirstPosition(subscriptionId);
        if (writeVersion.isPresent()) {
            // One write settles the first position and stores the version, so no write of another node falls between
            // the two. With a newer version stored, the storage writes nothing and throws
            // CheckpointWriteConditionNotFulfilledException, so a node that lost its lease delivers no event
            Optional<Checkpoint> settled = storage.resolveFirstCheckpointRace(subscriptionId, globalCheckpoint, writeVersion.getAsLong());
            if (settled.isPresent()) {
                return settled.get();
            }
        }
        try {
            return storage.save(subscriptionId, globalCheckpoint, CheckpointWriteCondition.ifAbsent());
        } catch (CheckpointWriteConditionNotFulfilledException e) {
            return storage.resolveFirstCheckpointRace(subscriptionId, globalCheckpoint)
                          .orElseGet(() -> refuseUnlessTheStoredPositionIsTheOneRead(subscriptionId, globalCheckpoint));
        }
    }

    // Read before anything is written, as for an event. When the source throws a RuntimeException, the first position
    // is recorded without a version. A checked exception a source written in Kotlin throws fails the subscribe
    private OptionalLong writeVersionOfFirstPosition(String subscriptionId) {
        try {
            return writeConditionFor(subscriptionId) instanceof CheckpointWriteCondition.NotOlderThan notOlderThan
                    ? OptionalLong.of(notOlderThan.writeVersion())
                    : OptionalLong.empty();
        } catch (RuntimeException e) {
            log.warn("Could not read the write version for subscription {}, so its first position is recorded without one. " +
                     "Each later checkpoint write asks for the version again.", subscriptionId, e);
            return OptionalLong.empty();
        }
    }

    // Settling runs this while the subscribe is under way, so once a cancel or a later subscribe of the id stopped the
    // writes of this subscribe, the subscribe fails
    private Checkpoint withThisNodesWriteVersionStoredUnlessCancelled(String subscriptionId, Checkpoint stored,
                                                                      CheckpointRegistration registration) {
        return withThisNodesWriteVersionStored(subscriptionId, stored, registration,
                () -> new IllegalStateException("The checkpoint writes of this subscribe of " + subscriptionId + " were stopped, " +
                                                "by a cancel or a later subscribe of the id, before this evaluation stored this " +
                                                "node's write version with the checkpoint it read, so it gets none."));
    }

    // An evaluation of the start position after settling, a restart of the wrapped subscription for one, starts from the
    // checkpoint read without a write once the writes of this subscribe were stopped, as in 0.34.0. A refused later
    // subscribe stops them and leaves the subscription running, so a refusal here would stop its delivery
    private Checkpoint withThisNodesWriteVersionStoredUnlessStopped(String subscriptionId, Checkpoint stored,
                                                                    CheckpointRegistration registration) {
        try {
            return withThisNodesWriteVersionStored(subscriptionId, stored, registration, CheckpointWritesStopped::new);
        } catch (CheckpointWritesStopped e) {
            return stored;
        }
    }

    // The version is stored through the registration, so nothing is written once its writes were stopped, and
    // through storeWriteVersion, which leaves the checkpoint stored as it is and writes nothing when none is. So a
    // checkpoint a cancel on another node deleted isn't stored again, and a later position another node stored isn't
    // replaced with the one read. Without a version, or when nothing is stored any more, the checkpoint read is started
    // from without a write, as in 0.34.0
    private Checkpoint withThisNodesWriteVersionStored(String subscriptionId, Checkpoint stored, CheckpointRegistration registration,
                                                       Supplier<IllegalStateException> stoppedRefusal) {
        OptionalLong writeVersion = writeVersionOfStoredCheckpoint(subscriptionId);
        if (writeVersion.isEmpty()) {
            return stored;
        }
        return registration.writeUnlessCancelled(() -> storage.storeWriteVersion(subscriptionId, writeVersion.getAsLong()).orElse(stored),
                stoppedRefusal);
    }

    // Thrown and caught inside withThisNodesWriteVersionStoredUnlessStopped only, so it needs no message or stack trace
    private static final class CheckpointWritesStopped extends IllegalStateException {
        CheckpointWritesStopped() {
            super(null, null);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

    // A RuntimeException from the source starts the subscription from the stored checkpoint with no version stored
    private OptionalLong writeVersionOfStoredCheckpoint(String subscriptionId) {
        try {
            return writeConditionFor(subscriptionId) instanceof CheckpointWriteCondition.NotOlderThan notOlderThan
                    ? OptionalLong.of(notOlderThan.writeVersion())
                    : OptionalLong.empty();
        } catch (RuntimeException e) {
            log.warn("Could not read the write version for subscription {}, so it starts from the checkpoint stored for it " +
                     "without storing a version with it. Each later checkpoint write asks for the version again.", subscriptionId, e);
            return OptionalLong.empty();
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
                "refused that write, and reading the checkpoint stored back did not name it. That checkpoint can " +
                "hold an earlier position, so starting from " + positionRead.asString() + " could skip the events " +
                "between the two, and the start is refused instead. Evaluating the start " +
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
        Checkpoint stored = readStoredPositionBack(subscriptionId, positionRead);
        if (positionRead.asString().equals(stored.asString())) {
            return stored;
        }
        throw new StartPositionAlreadyPinnedException(subscriptionId, positionRead, stored);
    }

    // Refused with no stored position named when the read fails or finds nothing
    private Checkpoint readStoredPositionBack(String subscriptionId, Checkpoint positionRead) {
        @Nullable Checkpoint stored;
        try {
            stored = storage.read(subscriptionId);
        } catch (Exception e) {
            // Exception, since a storage written in Kotlin can throw a checked exception
            throw StartPositionAlreadyPinnedException.readingTheStoredPositionBackFailed(subscriptionId, positionRead, e);
        }
        if (stored == null) {
            throw StartPositionAlreadyPinnedException.readingTheStoredPositionBackFoundNothing(subscriptionId, positionRead);
        }
        return stored;
    }

    // The registration learns once a position is stored for a subscription from the model default, the one read,
    // recorded or adopted, and before any evaluation returns it. It learns at most once for each subscribe, and never
    // for a refused one or for a StartAt of the caller's own. An evaluation that records a first position outside the
    // settled outcome, or stores this node's version with the checkpoint it read, writes through the registration.
    // Once the registration has stopped writing, recording a first position is refused, and a checkpoint read is
    // started from without storing the version
    private StartAt generateStartAtPositionFrom(String subscriptionId, StartAt originalStartAt, CheckpointRegistration registration,
                                                Consumer<FirstPosition> firstPositionToRecord) {
        final StartAt startAtToUse;
        if (originalStartAt.isDefault()) {
            FirstPosition firstPosition = new FirstPosition(subscriptionId, registration);
            firstPositionToRecord.accept(firstPosition);
            StartAt startAtIfNoSubscriptionFound = StartAt.subscriptionModelDefault();
            startAtToUse = StartAt.dynamic(() -> {
                Checkpoint recorded = firstPosition.forEvaluation();
                if (recorded != null) {
                    return StartAt.checkpoint(recorded);
                }
                // Read inside the supplier so a retry picks up the latest checkpoint, not a stale one
                Checkpoint checkpoint = storage.read(subscriptionId);
                if (checkpoint != null) {
                    checkpoint = withThisNodesWriteVersionStoredUnlessStopped(subscriptionId, checkpoint, registration);
                }
                if (checkpoint == null && !config.startWhenFirstPositionCanBeOverwritten && !storage.evaluatesWriteConditionsFor(subscriptionId)) {
                    // Settling refused this subscribe unless a checkpoint was stored then, so it was, and something such
                    // as a cancel on another node deleted it since. A first position written here could only be written
                    // unconditionally, and a refusal here is retried without end by the blocking MongoDB models, so the
                    // evaluation starts from the checkpoint read then. That can deliver an event again but skips none, as
                    // when the delete comes after the subscription started
                    checkpoint = firstPosition.storedWhenSettled();
                }
                if (checkpoint == null) {
                    Checkpoint globalCheckpoint = subscriptionModel.globalCheckpoint();
                    if (globalCheckpoint != null) {
                        checkpoint = registration.writeUnlessCancelled(() -> saveFirstPositionOrAdoptWhatWon(subscriptionId, globalCheckpoint),
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
        } else {
            startAtToUse = originalStartAt;
        }
        return startAtToUse;
    }

    // Read once the subscribe is tracked, so a registration it replaced has stopped writing and what is read was stored
    // before this run or by it. The version is read first, as a resume reads it. After a read that threw an Exception, a
    // plain resume goes on from the wrapped model's own position until this subscribe has stored a checkpoint, which can
    // deliver an event twice but never skips one. An Error reaches subscribe, which cancels the subscription
    private StoredBefore storedBeforeOwnStart(String subscriptionId) {
        try {
            OptionalLong writeVersion = storage.writeVersion(subscriptionId);
            Checkpoint stored = storage.read(subscriptionId);
            return new StoredBefore(true, stored == null ? null : stored.asString(), writeVersion);
        } catch (Exception e) {
            log.warn("Could not read the checkpoint stored for subscription {} after it subscribed at a checkpoint of its own, so a resume " +
                     "goes on from the position the wrapped model has read to until the subscription stores a checkpoint, also once " +
                     "another node has stored a later checkpoint for it.", subscriptionId, e);
            return StoredBefore.UNKNOWN;
        }
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
     * reason a MongoDB model resumes from its own tracked position when the stored checkpoint is one it cannot open
     * its change stream at, such as the position a catch-up stores while it replays.
     * <p>
     * A subscription subscribed at a {@code StartAt.checkpoint(..)} of its own, such as a replay, also goes on from the
     * wrapped model's own position until it stores a checkpoint, while the checkpoint and write version stored for the id
     * are the ones read when it was subscribed. That checkpoint can come from an earlier run of the id and lie after the
     * replay's start, so resuming there would skip the events in between. Once it has stored a checkpoint, or a different
     * checkpoint or write version is stored, such as by another node that held the lease, it resumes at the stored
     * checkpoint. When the checkpoint couldn't be read at subscribe, it goes on from the wrapped model's own position until
     * it stores a checkpoint. Going on from there can deliver an event again but never skips one. A subscription subscribed
     * at {@code StartAt.now()} resumes at the stored checkpoint, since a checkpoint stored before it can't lie after its
     * start.
     * <p>
     * An action still running when the subscription was paused, because the wrapped model stopped waiting for it,
     * doesn't store the checkpoint of its event once the resumed subscription has started saving the checkpoint of one
     * of its own events or a quiet position, or has stored the position it restarts from after its history was lost.
     * Neither does a quiet position read before the pause. So the stored checkpoint doesn't move back.
     * <p>
     * A subscribe of the id on another thread whose {@code StartAt.dynamic} function is still being asked is waited
     * for. If that function doesn't return within 10 seconds, this throws {@link IllegalStateException} and changes
     * nothing.
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        // Held for the whole decision, reposition call included, so a concurrent subscribe or cancelSubscription
        // for this id cannot land between the marker check and acting on it.
        waitForAsksOnOtherThreads(subscriptionId);
        return underLockFor(subscriptionId, () -> resumeFromStoredCheckpoint(subscriptionId));
    }

    // Callers hold the id lock
    private Subscription resumeFromStoredCheckpoint(String subscriptionId) {
        if (!notCheckpointedSubscriptions.contains(subscriptionId)) {
            Optional<RepositionableSubscriptions> repositionable = RepositionableSubscriptions.findIn(getWrappedSubscriptionModel());
            if (repositionable.isPresent()) {
                // Asked before anything is read, so a checkpoint this subscribe stores after the answer is the one read
                // below, or makes the version read below differ from the one read at subscribe
                CheckpointRegistration registration = registrations.get(subscriptionId);
                StoredBefore before = registration == null ? null : registration.storedBeforeOwnStartUnlessItStoredOne();
                OptionalLong writeVersion = before == null || !before.known() ? OptionalLong.empty() : storage.writeVersion(subscriptionId);
                Checkpoint checkpoint = storage.read(subscriptionId);
                if (checkpoint != null && (before == null || !before.stillStored(checkpoint, writeVersion))) {
                    return repositionable.get().resumeSubscription(subscriptionId, StartAt.checkpoint(checkpoint));
                }
            }
        }
        return getWrappedSubscriptionModel().resumeSubscription(subscriptionId);
    }

    /**
     * Resume a paused subscription at {@code startAt}, and store that position as the subscription's checkpoint before
     * the wrapped model resumes there. A restart before the next event then resumes from it, instead of from the
     * checkpoint stored before.
     * <ul>
     * <li>{@link StartAt#checkpoint(Checkpoint)} stores that checkpoint.</li>
     * <li>{@link StartAt#now()} stores the position the wrapped model's {@link CheckpointAwareSubscriptionModel#globalCheckpoint()}
     * answers, and resumes there, so the stored and the actual start agree. When it answers {@code null}, nothing is
     * stored or resumed and this throws {@link IllegalStateException}.</li>
     * <li>{@link StartAt#subscriptionModelDefault()} resumes as {@link #resumeSubscription(String)} does, from the stored
     * checkpoint.</li>
     * <li>A dynamic {@code StartAt} is asked once, and its answer is handled as above. When it answers {@code null},
     * nothing is stored or resumed and this throws {@link IllegalArgumentException}.</li>
     * <li>A checkpoint the wrapped model's {@link RepositionableSubscriptions#restartsAt(Checkpoint)} answers
     * {@code false} for is passed on to the wrapped model and not stored. A restart subscribes the wrapped model at the
     * stored checkpoint, and it would start somewhere else for that one. The MongoDB subscription models answer
     * {@code false} for a catch-up's position, a time, and any other checkpoint their change stream can't open at. A
     * model that doesn't override it answers {@code false} for every checkpoint, so nothing is stored over it.</li>
     * </ul>
     * The checkpoint is stored with the same write condition as every other checkpoint this model writes. An action
     * still running when the subscription was paused, because the wrapped model stopped waiting for it, doesn't store
     * the checkpoint of its event over the stored position once it returns, and neither does a quiet position read
     * before this call. That holds when this call throws too.
     * When the checkpoint storage refuses the write with {@link CheckpointWriteConditionNotFulfilledException}, because another
     * node has written with a newer lease, the exception propagates and the subscription stays paused. When the wrapped
     * model's resume throws after the write, the checkpoint stored before is put back with the write condition the
     * position was stored with, and the exception propagates, so a failed resume leaves the stored position as it was
     * before the call. A checkpoint another node has written with a newer lease in the meantime refuses the put-back. A catch-up model that shares the
     * checkpoint storage then replays from where it would have before the call. When nothing was stored before, the
     * position asked for here stays stored. When putting the old checkpoint back throws too, that exception is added
     * to the thrown one as a suppressed exception, and the position asked for here may still be the one stored.
     * After a failed resume, a quiet position is saved only when the model knows the stored checkpoint is the
     * checkpoint of the last event delivered, a position stored after it, or the position of a resume made after that
     * event was read. When the old checkpoint goes back over an event whose checkpoint this call refused, no quiet
     * position is saved until a later event stores its checkpoint. When putting the old checkpoint back throws, the
     * stored checkpoint is read to find out which one is stored. When that read throws too, every quiet position save
     * reads it before it writes, until a read returns.
     * <p>
     * A subscription subscribed without checkpointing (see {@link #subscribe(String, SubscriptionFilter, StartAt, Consumer)}),
     * or one the wrapped model doesn't report as paused, is passed on to the wrapped model as it is and nothing is
     * stored. The wrapped model then throws for a subscription it doesn't hold paused.
     * <p>
     * Holds the same lock per subscription id as {@link #resumeSubscription(String)} for the whole call, and waits for a
     * subscribe of the id on another thread as that method does.
     *
     * @throws UnsupportedOperationException if the wrapped model is not repositionable, in which case
     *                                       {@link RepositionableSubscriptions#findIn} doesn't find this model either.
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        RepositionableSubscriptions repositionable = RepositionableSubscriptions.findIn(getWrappedSubscriptionModel())
                .orElseThrow(() -> new UnsupportedOperationException(getWrappedSubscriptionModel().getClass().getSimpleName() + " is not repositionable"));
        waitForAsksOnOtherThreads(subscriptionId);
        return underLockFor(subscriptionId, () -> {
            if (notCheckpointedSubscriptions.contains(subscriptionId) || !getWrappedSubscriptionModel().isPaused(subscriptionId)) {
                return repositionable.resumeSubscription(subscriptionId, startAt);
            }
            StartAt resolved = startAt.get(new SubscriptionModelContext(DurableSubscriptionModel.class));
            if (resolved == null) {
                throw new IllegalArgumentException("The StartAt passed to resume subscription " + subscriptionId + " answered null, so there is no position to resume it at");
            }
            if (resolved.isDefault()) {
                return resumeFromStoredCheckpoint(subscriptionId);
            }
            Checkpoint position = resolved instanceof StartAt.StartAtCheckpoint at ? at.checkpoint : presentPositionToResumeAt(subscriptionId);
            if (!repositionable.restartsAt(position)) {
                // Not stored, since a restart subscribes the wrapped model at the stored checkpoint and it would start
                // somewhere else
                return repositionable.resumeSubscription(subscriptionId, StartAt.checkpoint(position));
            }
            List<CheckpointRegistration> writing = registrationsWritingFor(subscriptionId);
            // Stored before the resume, since an event delivered after the resume writes a newer checkpoint that this
            // write would otherwise overwrite
            Checkpoint previous = storage.read(subscriptionId);
            CheckpointWriteCondition writeCondition = writeConditionFor(subscriptionId);
            // A handler the pause stopped waiting for can still return and save the checkpoint of its event over the
            // position stored here, so that save is refused from before this write on. It stays refused when the write
            // throws, since the position may have been stored anyway, and when the resume fails, which only delivers
            // its event again
            try {
                underLocks(saveLocksOf(writing), () -> {
                    writing.forEach(CheckpointRegistration::repositioning);
                    try {
                        storage.save(subscriptionId, position, writeCondition);
                    } catch (Throwable failure) {
                        writing.forEach(registration -> registration.repositionMayBeStored(position, previous));
                        throw failure;
                    }
                    writing.forEach(registration -> registration.repositionStored(position, previous));
                });
            } finally {
                // Outside the saveLocks, since deliveryLock is taken before saveLock
                writing.forEach(CheckpointRegistration::decideQuietTicks);
            }
            try {
                return repositionable.resumeSubscription(subscriptionId, StartAt.checkpoint(position));
            } catch (Throwable e) {
                // Throwable, since a wrapped model written in Kotlin can throw a checked exception. With nothing to put
                // back the new position stays, since a delete could remove a checkpoint written after the resume
                // started, and a restart with nothing stored subscribes at the wrapped model's default
                if (previous == null) {
                    throw e;
                }
                // Under the deliveryLocks too, so no delivery ends between learning what is stored and deciding from it
                // whether that covers the latest delivery
                List<ReentrantLock> locks = new ArrayList<>(deliveryLocksOf(writing));
                locks.addAll(saveLocksOf(writing));
                underLocks(locks, () -> {
                    writing.forEach(CheckpointRegistration::puttingBack);
                    if (putBack(subscriptionId, previous, writeCondition, e)) {
                        writing.forEach(registration -> registration.stored(previous));
                    } else {
                        // Unknown also when the put-back was refused, since its condition is the one the position was
                        // just written with, so another lease holder has written since
                        writing.forEach(CheckpointRegistration::storedUnknown);
                        readWhatIsStoredAfterFailedPutBack(subscriptionId, e, writing);
                    }
                    writing.forEach(CheckpointRegistration::decideQuietTicks);
                });
                throw e;
            }
        });
    }

    // The tracked registration of the id and one a failed subscribe left writing, in the order their saveLocks are taken
    private List<CheckpointRegistration> registrationsWritingFor(String subscriptionId) {
        List<CheckpointRegistration> writing = new ArrayList<>(2);
        CheckpointRegistration tracked = registrations.get(subscriptionId);
        if (tracked != null) {
            writing.add(tracked);
        }
        UntrackedRegistration untracked = untrackedRegistrations.get(subscriptionId);
        CheckpointRegistration kept = untracked == null ? null : untracked.get();
        if (kept != null) {
            writing.add(kept);
        }
        return writing;
    }

    private static List<ReentrantLock> saveLocksOf(List<CheckpointRegistration> registrations) {
        return registrations.stream().map(registration -> registration.saveLock).toList();
    }

    private static List<ReentrantLock> deliveryLocksOf(List<CheckpointRegistration> registrations) {
        return registrations.stream().map(registration -> registration.deliveryLock).toList();
    }

    // Takes the locks in the order given and lets them go in the reverse order
    private static void underLocks(List<ReentrantLock> locks, Runnable action) {
        int locked = 0;
        try {
            for (ReentrantLock lock : locks) {
                lock.lock();
                locked++;
            }
            action.run();
        } finally {
            for (int i = locked - 1; i >= 0; i--) {
                locks.get(i).unlock();
            }
        }
    }

    // A put-back that threw may have written or not, so what is stored is read. When the read throws too, what is
    // stored stays unknown and the next quiet tick reads it before it writes
    private void readWhatIsStoredAfterFailedPutBack(String subscriptionId, Throwable resumeFailure, List<CheckpointRegistration> writing) {
        @Nullable Checkpoint stored;
        try {
            stored = storage.read(subscriptionId);
        } catch (Throwable readFailure) {
            // Throwable, since a storage written in Kotlin can throw a checked exception
            if (readFailure != resumeFailure) {
                resumeFailure.addSuppressed(readFailure);
            }
            return;
        }
        writing.forEach(registration -> registration.storedRead(stored));
    }

    // A catch-up model sharing the storage replays from what is stored, so a failed resume puts back what it found.
    // The condition of the first write is reused, so a checkpoint another lease holder wrote since then refuses the
    // put-back. Answers whether it wrote
    private boolean putBack(String subscriptionId, Checkpoint previous, CheckpointWriteCondition writeCondition, Throwable resumeFailure) {
        try {
            storage.save(subscriptionId, previous, writeCondition);
            return true;
        } catch (Throwable putBackFailure) {
            // Throwable, since a storage written in Kotlin can throw a checked exception
            if (putBackFailure != resumeFailure) {
                resumeFailure.addSuppressed(putBackFailure);
            }
            return false;
        }
    }

    /**
     * What the wrapped model answers, since this model subscribes it at the stored checkpoint after a restart.
     */
    @Override
    public boolean restartsAt(Checkpoint checkpoint) {
        return RepositionableSubscriptions.findIn(getWrappedSubscriptionModel()).map(model -> model.restartsAt(checkpoint)).orElse(false);
    }

    private Checkpoint presentPositionToResumeAt(String subscriptionId) {
        Checkpoint present = subscriptionModel.globalCheckpoint();
        if (present == null) {
            throw new IllegalStateException("The wrapped subscription model " + subscriptionModel.getClass().getName() +
                                            " answered nothing when asked for the current position, so subscription " +
                                            subscriptionId + " can't be resumed at StartAt.now() with the position stored. " +
                                            "Nothing was stored and the subscription is still paused.");
        }
        return present;
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
     * <p>
     * It doesn't wait for a subscribe of the id on another thread whose start-position function is still being asked.
     * That subscribe subscribes nothing in the wrapped model, and the subscription it returns never starts. A cancel
     * the function makes on the subscribing thread doesn't stop that subscribe, and neither does a cancel that throws.
     *
     * @param subscriptionId The subscription id to cancel
     */
    @Override
    public void cancelSubscription(String subscriptionId) {
        runUnderLockFor(subscriptionId, () -> {
            // The subscribes whose function another thread asks once this holds the lock. A subscribe made later waits
            // for this cancel and then goes ahead. A cancel the function makes on the subscribing thread doesn't stop
            // that subscribe, as before the function was asked ahead of the lock
            Thread current = Thread.currentThread();
            List<Asked> pending = asking.stream().filter(asked -> asked.subscriptionId.equals(subscriptionId) && asked.asker != current).toList();
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
            // Only once the cancel has run without throwing, and still under the lock the subscribe needs, so a
            // subscribe whose function is asked still goes ahead when the cancel throws
            pending.forEach(asked -> asked.cancelled = true);
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
            // An action the wrapped model didn't wait for then stores no checkpoint after this returns, where a model
            // started after it on the same storage could read it
            shutDown = true;
            registrations.values().forEach(CheckpointRegistration::stopWriting);
            untrackedRegistrations.values().forEach(DurableSubscriptionModel::stopWritingOf);
            HistoryLossReportingSubscriptions.findIn(subscriptionModel).ifPresent(model -> model.removeHistoryLossListener(historyLossListener));
            QuietPositionReportingSubscriptions.findIn(subscriptionModel).ifPresent(model -> model.removeQuietPositionListener(quietPositionListener));
        }
    }

    @Nullable
    @Override
    public Checkpoint globalCheckpoint() {
        BooleanSupplier stillWanted = stillWantedOfTheCall.get();
        return stillWanted == null ? subscriptionModel.globalCheckpoint() : subscriptionModel.globalCheckpoint(stillWanted);
    }

    /**
     * Forwards to the wrapped model, with {@code stillWanted}. A catch-up calls this overload from its replay thread. It
     * calls {@link #globalCheckpoint()}, so a subclass that overrides only that method is still called, and
     * {@code stillWanted} reaches the wrapped model when the override calls {@code super.globalCheckpoint()} on the same
     * thread.
     */
    @Nullable
    @Override
    public Checkpoint globalCheckpoint(BooleanSupplier stillWanted) {
        BooleanSupplier outer = stillWantedOfTheCall.get();
        stillWantedOfTheCall.set(stillWanted);
        try {
            return globalCheckpoint();
        } finally {
            if (outer == null) {
                stillWantedOfTheCall.remove();
            } else {
                stillWantedOfTheCall.set(outer);
            }
        }
    }

    @Override
    public boolean canResumeFrom(Checkpoint checkpoint) {
        BooleanSupplier stillWanted = stillWantedOfTheCall.get();
        return stillWanted == null ? subscriptionModel.canResumeFrom(checkpoint) : subscriptionModel.canResumeFrom(checkpoint, stillWanted);
    }

    /**
     * Forwards to the wrapped model, with {@code stillWanted}. A catch-up calls this overload from its replay thread. It
     * calls {@link #canResumeFrom(Checkpoint)}, so a subclass that overrides only that method is still called, and
     * {@code stillWanted} reaches the wrapped model when the override calls {@code super.canResumeFrom(checkpoint)} on
     * the same thread.
     */
    @Override
    public boolean canResumeFrom(Checkpoint checkpoint, BooleanSupplier stillWanted) {
        BooleanSupplier outer = stillWantedOfTheCall.get();
        stillWantedOfTheCall.set(stillWanted);
        try {
            return canResumeFrom(checkpoint);
        } finally {
            if (outer == null) {
                stillWantedOfTheCall.remove();
            } else {
                stillWantedOfTheCall.set(outer);
            }
        }
    }

    @Override
    public CheckpointAwareSubscriptionModel getWrappedSubscriptionModel() {
        return subscriptionModel;
    }

    /**
     * {@code false}. A plain {@link #resumeSubscription(String)}, and a {@link #resumeSubscription(String, StartAt)} at
     * {@link StartAt#subscriptionModelDefault()}, resume at the checkpoint stored for the id, and another process can store
     * one there that lies after events this model never delivered. An earlier run of the same application that hasn't
     * stopped yet is one. {@link #shutdown()} stops the checkpoint writes of this instance, not those of another instance
     * or process.
     * <p>
     * It is also {@code false} with {@link DurableSubscriptionModelConfig#startWhenNoStartPositionCanBeRecorded(boolean)}
     * turned on. A subscription that starts with no position recorded opens where the wrapped model's default points, and
     * each time the wrapped model evaluates its start position again before the first checkpoint is stored, it starts from
     * wherever the feed has reached by then and skips the events written in between.
     */
    @Override
    public boolean deliversEveryEventInCommitOrder() {
        return false;
    }

    /**
     * What the wrapped model answers, and {@code false} when it doesn't declare it. This model saves the checkpoint of
     * an event only once the action has returned normally, and after an action that throws it saves no quiet position
     * until a later event's checkpoint is saved. So the stored checkpoint doesn't move past an event whose action threw
     * {@code AttemptNotMade}.
     */
    @Override
    public boolean redeliversAnAttemptNotMade() {
        return RedeliveringSubscriptions.findIn(subscriptionModel).map(RedeliveringSubscriptions::redeliversAnAttemptNotMade).orElse(false);
    }

    /**
     * Answers {@link RepositionableSubscriptions} and {@link RedeliveringSubscriptions} only when the wrapped model has
     * it, since {@link #resumeSubscription(String, StartAt)} resumes the subscription there, and an event is delivered
     * again only when the wrapped model delivers it again. Answers every other capability as any model does.
     */
    @Override
    public <T extends SubscriptionModelCapability> Optional<T> capability(Class<T> type) {
        if ((type == RepositionableSubscriptions.class || type == RedeliveringSubscriptions.class) && !subscriptionModel.hasCapability(type)) {
            return Optional.empty();
        }
        return CheckpointAwareSubscriptionModel.super.capability(type);
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
    //   held only to settle the outcome and write it to storage, never for the read that decides what to write or
    //   while asking the wrapped model for its position.
    // - An evaluation never waits for the subscribing thread to call the wrapped model, so an evaluation that the
    //   wrapped subscribe waits for can't hang on the subscribing thread. It can wait for a storage write that the
    //   subscribing thread or another evaluation started.
    // - settle writes for one party only of each subscribe on this node, whoever takes settling first while nothing is
    //   settled. Only that party records a first position or stores this node's version with the checkpoint read, and
    //   every party after it takes its outcome, a failure included. The first evaluation that gets it returns what was
    //   recorded. A later evaluation that finds nothing stored records outside settling, as two nodes do, under the
    //   registration's saveLock, and is refused once the registration has stopped writing. The version stored with a
    //   checkpoint read is written under that lock too, whoever reads it.
    // - Nothing is stored once the subscribe ended without a settled position, so a subscribe the wrapped model
    //   refuses before evaluating stores nothing, and every evaluation from then on throws.
    // - A failure before settling settles nothing, so a later evaluation or the subscribing thread records again. A
    //   failure while settling is the outcome, so the subscribe is refused and every evaluation from then on throws. A
    //   RuntimeException on the subscribing thread ends the subscribe, unless an evaluation settled the position first.
    //   An Error there ends the subscribe either way, and a position an evaluation settled stays stored.
    // - positionStored runs once a position of the subscription is stored, whether read, recorded or adopted, and
    //   before any evaluation gets that position. It runs at most once. It doesn't run once the subscribe ended
    //   without a settled position, nor while nothing is stored because the override let an unanswerable source
    //   through.
    private final class FirstPosition {
        private static final Settled ENDED = new Settled(null, null, null);

        private final String subscriptionId;
        private final ReentrantLock settling = new ReentrantLock();
        // Null until settled, and written only while holding settling
        private volatile @Nullable Settled settled;
        private final AtomicBoolean handedOut = new AtomicBoolean();
        // Set first thing in every evaluation, so a subscribe the wrapped model failed can tell whether one started,
        // including one still recording
        private volatile boolean evaluated;
        private final CheckpointRegistration registration;
        private final AtomicBoolean positionStoredRan = new AtomicBoolean();

        FirstPosition(String subscriptionId, CheckpointRegistration registration) {
            this.subscriptionId = subscriptionId;
            this.registration = registration;
        }

        // Once a position of the subscription is stored. Only the first call runs it, since a later one would find
        // nothing left to allow
        void positionStored() {
            if (positionStoredRan.compareAndSet(false, true)) {
                registration.startPositionStored();
            }
        }

        void recordOnceAccepted() {
            Settled outcome;
            try {
                outcome = settle();
            } catch (Exception refusal) {
                // Exception, since a wrapped model or a storage written in Kotlin can throw a checked exception
                if (endUnlessSettled() || requireNonNull(settled).failure() != null) {
                    throw refusal;
                }
                return;
            }
            // An evaluation wrote the first position and failed, so the subscribe is refused as if this thread had
            Throwable failure = outcome.failure();
            if (failure instanceof RuntimeException e) {
                throw e;
            } else if (failure instanceof Error e) {
                throw e;
            } else if (failure != null) {
                throw new IllegalStateException("Recording the first position of subscription " + subscriptionId + " failed.", failure);
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
            if (outcome.failure() != null) {
                throw new IllegalStateException("Recording the first position of subscription " + subscriptionId + " failed on another " +
                                                "thread of this subscribe, so this evaluation gets none.", outcome.failure());
            }
            return handedOut.compareAndSet(false, true) ? outcome.recorded : null;
        }

        // The checkpoint read when the first position settled, for an evaluation that has settled it
        @Nullable Checkpoint storedWhenSettled() {
            Settled outcome = settled;
            return outcome == null ? null : outcome.stored;
        }

        // Only the party that takes settling while nothing is settled writes, and what it ends with, a position or a
        // failure, is what every party after it gets. What it writes with depends on a read and on the wrapped model,
        // so both come before settling
        private Settled settle() {
            Settled outcome = settled;
            if (outcome != null) {
                return outcome;
            }
            Checkpoint read = storage.read(subscriptionId);
            Checkpoint position = read == null ? subscriptionModel.globalCheckpoint() : null;
            if (read == null && position == null && !config.startWhenNoStartPositionCanBeRecorded) {
                throw noStartPositionCanBeRecorded(subscriptionId);
            }
            // With nothing stored, a first position is recorded now or, when the override let a missing position through,
            // by a later evaluation once the wrapped model has one. A refusal there comes after subscribe has returned,
            // and the blocking MongoDB models retry it without end, so it is refused here, where it reaches subscribe
            if (read == null && !config.startWhenFirstPositionCanBeOverwritten && !storage.evaluatesWriteConditionsFor(subscriptionId)) {
                throw firstPositionCanBeOverwritten(subscriptionId);
            }
            settling.lock();
            try {
                outcome = settled;
                if (outcome == null) {
                    try {
                        outcome = write(read, position);
                    } catch (Throwable failure) {
                        // Whatever it wrote before it failed stays as it is, since no other party writes
                        settled = new Settled(null, null, failure);
                        throw failure;
                    }
                    settled = outcome;
                }
                return outcome;
            } finally {
                settling.unlock();
            }
        }

        private Settled write(@Nullable Checkpoint read, @Nullable Checkpoint position) {
            Checkpoint stored = null;
            if (read != null) {
                // Before the outcome settles, so the refusal for a newer version stored is the outcome, and subscribe
                // throws it whichever party wrote
                stored = withThisNodesWriteVersionStoredUnlessCancelled(subscriptionId, read, registration);
            }
            Settled outcome = new Settled(position == null ? null : saveFirstPosition(subscriptionId, position), stored, null);
            if (stored != null || position != null) {
                // Before the outcome is published, so no evaluation returns the position before it runs
                positionStored();
            }
            return outcome;
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
    // already, the override let an unanswerable source through or the write failed. stored is the checkpoint read when
    // it settled. failure is what the party that wrote threw, and null when it threw nothing
    private record Settled(@Nullable Checkpoint recorded, @Nullable Checkpoint stored, @Nullable Throwable failure) {
    }

    // The checkpoint and write version stored for an id once a subscribe at a checkpoint of the caller's own was
    // tracked, or that the read failed
    record StoredBefore(boolean known, @Nullable String checkpoint, OptionalLong writeVersion) {
        static final StoredBefore UNKNOWN = new StoredBefore(false, null, OptionalLong.empty());

        // Compared by asString, since a Checkpoint promises no equals. A failed read counts as still stored, so the
        // resume can deliver an event twice but never skips one
        boolean stillStored(Checkpoint stored, OptionalLong storedWriteVersion) {
            return !known || (stored.asString().equals(checkpoint) && storedWriteVersion.equals(writeVersion));
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