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

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel.Read;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel.Reposition;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel.StoredBefore;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel.StoredCheckpoint;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

// One for each subscribe of an id this model stores checkpoints for
final class CheckpointRegistration {
    // Set only for a subscribe at a checkpoint of the caller's own, once it is tracked
    private volatile @Nullable StoredBefore storedBeforeOwnStart;
    // Set once a checkpoint for an event or a quiet position of this subscribe is stored
    private volatile boolean storedACheckpoint;
    // When a checkpoint was last written for the id, as System.nanoTime(). Starts now, so the first quiet position
    // is saved one interval after the subscribe
    final AtomicLong lastWrite = new AtomicLong(System.nanoTime());
    // Held while a delivery starts or finishes, while a quiet position is saved, and while a resume at a position that
    // failed puts back what was stored before it, so no quiet position is saved while a delivery of any run of this
    // subscribe is under way. A resume takes the deliveryLocks of the tracked and the untracked registration of an
    // id in that order, under the lock for the id, and before their saveLocks
    final ReentrantLock deliveryLock = new ReentrantLock();
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
    // The last read this thread made, with the count of resumes at a position when it made it. An event that read
    // returns belongs to the run that read it, also when it is delivered after a resume
    private final ThreadLocal<@Nullable Read> lastReadOnThisThread = new ThreadLocal<>();
    // A delivery is current unless a current delivery started before it belongs to a later read. One that isn't
    // comes from a run a pause closed after it read, and the run that resumed has read and delivered past it
    private long readOfLatestCurrentDelivery;
    private long latestCurrentDelivery;
    // Whether the current delivery that started last stored its checkpoint, once it has finished. An action a
    // pause stopped waiting for can still return, or still be called, after a resume has delivered later events,
    // and what it stored says nothing about them
    private boolean latestStored;
    // Compared with what is stored once a put-back after a failed resume at a position has written or thrown, to
    // learn whether that covers the current delivery that started last. The checkpoint that delivery stored, as
    // Checkpoint.asString, or the quiet position saved after it, null when it stored none. The count of resumes at
    // a position when that delivery read its event or the quiet position was saved, since a resume after that
    // stored a position that covers it
    private @Nullable String latestPosition;
    private long latestRepositions;
    // The quiet positions whose save threw since the current delivery that started last ended or a quiet position
    // was last saved, as Checkpoint.asString. Each may be stored, so a put-back that finds one of them stored finds
    // a position saved after that delivery. A set, since a later save that threw may not have written, and the
    // put-back can find any of them stored. Read and written holding deliveryLock
    private final Set<String> quietPositionsThatMayBeStored = new HashSet<>();
    // Whether a position of the subscription was stored before the first delivery, until a current delivery ends.
    // Any stored checkpoint covers a subscribe that has delivered nothing
    private boolean storedBeforeAnyDelivery;
    // Held while the checkpoint for an event, a quiet position or a first position an evaluation records outside the
    // settled outcome is written, by a cancel while it marks this registration cancelled and, unless the checkpoint is
    // kept, deletes it, and by a replacement or a shutdown while it marks this registration cancelled. No other lock of
    // this model is taken while it is held, apart from the saveLock of an untracked registration of the same id,
    // which a resume at a position takes inside the tracked one
    final ReentrantLock saveLock = new ReentrantLock();
    private boolean cancelled;
    // Counts the resumes at a position and the put-backs after one failed. Moved before the write and never back,
    // so a count taken before a resume never comes round again. Written while holding saveLock, so a save that
    // started before a resume and checks it under saveLock is refused, and a save already under way finishes first
    private volatile long repositions;
    // The latest resume at a position whose position is still stored, unless a put-back failed too, and the one
    // before it. Written while holding saveLock
    private volatile Reposition storedReposition = Reposition.NONE;
    private Reposition storedRepositionBeforeTheLatest = Reposition.NONE;
    // The resume the latest put-back wrote over, put back as the stored one when what is stored is its position
    private Reposition putBackOver = Reposition.NONE;
    // Counts the put-backs after a failed resume at a position. Written holding deliveryLock and saveLock, so a delivery
    // that stored its checkpoint before a put-back wrote over it learns that when it finishes
    private long putBacks;
    // Set before the registration is tracked, for a subscribe that threw and failed to cancel the subscription
    private boolean keepCheckpointWhenCancelled;

    Read reading() {
        Read read = new Read(reads.incrementAndGet(), repositions);
        lastReadOnThisThread.set(read);
        return read;
    }

    void ownStartAfter(StoredBefore storedBefore) {
        storedBeforeOwnStart = storedBefore;
    }

    // A checkpoint stored before a start at a checkpoint of the caller's own may lie after that start, so a resume
    // there could skip events. Once this subscribe has stored one, what is stored is its own or a later one
    @Nullable StoredBefore storedBeforeOwnStartUnlessItStoredOne() {
        return storedACheckpoint ? null : storedBeforeOwnStart;
    }

    void storedACheckpoint() {
        storedACheckpoint = true;
    }

    // A position of the subscription is stored, so before the first event a quiet save moves that position on and
    // never stores the first one for a subscription that stores no position of its own
    void startPositionStored() {
        deliveryLock.lock();
        try {
            if (deliveries == 0) {
                storedBeforeAnyDelivery = true;
                latestStored = true;
                quietSaveAllowed = true;
            }
        } finally {
            deliveryLock.unlock();
        }
    }

    // The position a resume at a position stored is the latest one stored, also when a delivery a failed resume
    // refused was the latest before it. A delivery still under way decides again when it finishes
    void latestIsStored() {
        deliveryLock.lock();
        try {
            latestStored = true;
            quietSaveAllowed = deliveriesUnderWay == 0;
        } finally {
            deliveryLock.unlock();
        }
    }

    long delivering() {
        Read lastRead = lastReadOnThisThread.get();
        long read = lastRead == null ? 0 : lastRead.number();
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

    // A delivery that started before a resume at a position whose position is still stored counts as stored, since
    // the position stored is the resume's and not one this delivery could have moved past. One whose checkpoint a
    // put-back may have written over since it was stored doesn't, until a later put-back finds that checkpoint stored
    void delivered(long delivery, @Nullable StoredCheckpoint stored, long repositionsWhenDelivered) {
        deliveryLock.lock();
        try {
            deliveriesUnderWay--;
            if (delivery == latestCurrentDelivery) {
                latestStored = (stored != null && stored.putBacks() == putBacks) || storedReposition.count() > repositionsWhenDelivered;
                latestPosition = stored == null ? null : stored.checkpoint().asString();
                latestRepositions = repositionsWhenDelivered;
                quietPositionsThatMayBeStored.clear();
                storedBeforeAnyDelivery = false;
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

    long repositionsForDelivery() {
        Read lastRead = lastReadOnThisThread.get();
        return lastRead == null ? repositions : lastRead.repositions();
    }

    // Called holding saveLock, before a resume at a position writes it
    void repositioning() {
        repositions++;
    }

    // Called holding saveLock, once a resume at a position has written it
    void repositionStored(Checkpoint position) {
        storedRepositionBeforeTheLatest = storedReposition;
        storedReposition = new Reposition(repositions, position.asString());
    }

    // Called holding deliveryLock and saveLock, before a resume at a position that failed puts back what was stored
    // before it
    void puttingBack() {
        repositions++;
        putBacks++;
        putBackOver = storedReposition;
        storedReposition = storedRepositionBeforeTheLatest;
    }

    // Called holding deliveryLock and saveLock, with what is stored once the put-back after a failed resume at a
    // position wrote or threw, null when that is unknown. The current delivery that started last counts as stored
    // only when what is stored is its own checkpoint, a quiet position saved after it, also one whose save threw,
    // the position of a resume after it, or any checkpoint when nothing has been delivered. A delivery under way
    // decides again when it finishes
    void putBackLeft(@Nullable Checkpoint stored) {
        String position = stored == null ? null : stored.asString();
        if (position != null && position.equals(putBackOver.position())) {
            // The put-back didn't write, or wrote the resume's position, so what the resume stored is stored
            storedReposition = putBackOver;
        }
        if (position != null && quietPositionsThatMayBeStored.contains(position)) {
            // A quiet position saved after the current delivery that started last ended is stored. A resume after
            // that delivery still covers it, so latestRepositions stays
            latestPosition = position;
            quietPositionsThatMayBeStored.clear();
        }
        latestStored = position != null && (storedBeforeAnyDelivery || position.equals(latestPosition)
                || (position.equals(storedReposition.position()) && storedReposition.count() > latestRepositions));
        quietSaveAllowed = deliveriesUnderWay == 0 && latestStored;
    }

    // Taken inside deliveryLock, so it writes nothing once this registration has stopped writing, nor a position
    // read before a resume at a position stored its own. The save answers true when it wrote and false when it
    // threw, in which case it may have written
    void saveQuietPositionIfAllowed(long repositionsWhenRead, Checkpoint quietPosition, BooleanSupplier save) {
        deliveryLock.lock();
        try {
            if (quietSaveAllowed) {
                saveLock.lock();
                try {
                    if (!cancelled && repositions == repositionsWhenRead) {
                        if (save.getAsBoolean()) {
                            latestPosition = quietPosition.asString();
                            latestRepositions = repositions;
                            quietPositionsThatMayBeStored.clear();
                        } else {
                            quietPositionsThatMayBeStored.add(quietPosition.asString());
                        }
                    }
                } finally {
                    saveLock.unlock();
                }
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

    // What it saved, null when it saved nothing. A delivery that started before a resume at a position stored its own
    // saves nothing
    @Nullable StoredCheckpoint saveUnlessCancelledOrRepositioned(long repositionsWhenDelivered, Checkpoint checkpoint, Runnable save) {
        saveLock.lock();
        try {
            if (cancelled || repositions != repositionsWhenDelivered) {
                return null;
            }
            save.run();
            storedACheckpoint = true;
            return new StoredCheckpoint(checkpoint, putBacks);
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
