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
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel.Read;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel.Reposition;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel.StoredBefore;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
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
    // Held while a delivery starts or finishes, while a quiet position is saved, while a resume at a position that
    // failed puts back what was stored before it, and while the position a restart after a lost history starts from
    // is written, so no quiet position is saved while a delivery of any run of this subscribe is under way. Taken
    // before saveLock. A resume and that restart take the deliveryLocks of the tracked and the untracked
    // registration of an id in that order, under the lock for the id, and before their saveLocks
    final ReentrantLock deliveryLock = new ReentrantLock();
    // True only while no delivery is under way and the checkpoint stored covers the current delivery that ended
    // last, or isn't known, in which case a quiet save reads it first. Written holding deliveryLock
    private volatile boolean quietTickAllowed;
    private int deliveriesUnderWay;
    // Counts the deliveries of every run of this subscribe
    private long deliveries;
    // Numbers the reads of every run of this subscribe. A run reads and delivers on one thread, and asks the
    // listener before each read, so a delivery belongs to the read its thread made last
    private final AtomicLong reads = new AtomicLong();
    // The last read this thread made, with the count of resumes at a position when it made it. An event that read
    // returns belongs to the run that read it, also when it is delivered after a resume
    private final ThreadLocal<@Nullable Read> lastReadOnThisThread = new ThreadLocal<>();
    // A delivery is current unless a current delivery started before it belongs to a later read, or a restart after
    // a lost history was written after it started. One that isn't comes from a run a pause closed after it read, and
    // the run that resumed has read and delivered past it, or restarted past it
    private long readOfLatestCurrentDelivery;
    private long latestCurrentDelivery;
    // No delivery has this number, since they are numbered from 1
    private static final long NO_DELIVERY = 0;
    // What the checkpoint stored is compared with. The checkpoint of the current delivery that ended last, or the
    // quiet position or restart position after a lost history stored after it, as Checkpoint.asString. Null when
    // that delivery stored none. An action a pause stopped waiting for can still return after a resume has delivered
    // later events, and its checkpoint says nothing about them. Written holding deliveryLock and saveLock
    private @Nullable String coveringPosition;
    // The count of resumes at a position when that delivery read its event or that position was stored. A resume
    // after it stored a position that covers it
    private long coveringRepositions;
    // The latest quiet position or restart position after a lost history whose write threw since that delivery
    // ended. It may have written, so it covers that delivery once it is found stored. Kept when a resume writes over
    // it, since a failed resume puts it back. Only one, since a quiet save reads what is stored before it writes
    // again. A restart position that threw replaces a quiet position that threw before it without a read, so a read
    // that then finds that quiet position stored holds quiet saves until a later write stores a position that
    // covers. A listener that throws fails the restart, so the run that lost the history saves no quiet position
    // before it restarts, which writes a restart position again. Written holding deliveryLock and saveLock
    private @Nullable String positionThatThrew;
    // Whether a position of the subscription was stored before the first delivery, until a current delivery ends or
    // a read finds nothing stored. Any stored checkpoint covers a subscribe that has delivered nothing. Written
    // holding deliveryLock and saveLock
    private boolean storedBeforeAnyDelivery;
    // What the last write or read of this registration left stored, as Checkpoint.asString. A write that threw,
    // refused or not, makes it unknown. Null and known until this registration writes or reads one. Written
    // holding saveLock
    private @Nullable String storedPosition;
    private boolean storedKnown = true;
    // The read and the delivery number of the latest save that started writing, NO_DELIVERY for a quiet position. A
    // save of an earlier read, or of the same read and started before it, comes from a run a later run has replaced,
    // and would move the checkpoint back. Written holding saveLock
    private long readOfLatestSave;
    private long latestSave;
    // Held while the checkpoint for an event, a quiet position, the position of a resume or of a restart after a lost
    // history, a first position an evaluation records outside the settled outcome, or the version an evaluation stores
    // with the checkpoint it read is written, while a resume of a paused subscription reads the checkpoint stored and
    // goes to it, by a cancel while it marks this registration cancelled and, unless the checkpoint is kept, deletes
    // it, and by a replacement or a shutdown while it marks this registration cancelled. No other lock of this model is
    // taken while it is held, apart from the saveLock of an untracked registration of the same id, which a resume and a
    // restart after a lost history take inside the tracked one
    final ReentrantLock saveLock = new ReentrantLock();
    private boolean cancelled;
    // Counts the resumes at a position, the resumes at the checkpoint stored, the put-backs after a resume at a
    // position failed and the restarts after a lost history, each of which sets where the subscription reads from
    // next. Moved before the write, or with the read of a resume at the checkpoint stored, and never back, so a count
    // taken before one never comes round again. Written while holding saveLock, so a save that started before one and
    // checks it under saveLock is refused, and a save already under way finishes first
    private volatile long repositions;
    // The latest resume at a position, and the one whose position that resume read as stored before it, which a
    // put-back writes again. Either covers a delivery read before it only while its position is the one stored.
    // Written holding saveLock
    private Reposition latestReposition = Reposition.NONE;
    private Reposition repositionOfPrevious = Reposition.NONE;
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
        underDeliveryLockAndSaveLock(() -> {
            if (deliveries == 0) {
                storedBeforeAnyDelivery = true;
                decide();
            }
        });
    }

    // A delivery by its number, the read its event came from, 0 when the wrapped model numbers no reads, and the count
    // of resumes at a position when that read was made
    record Delivery(long number, long read, long repositions) {
    }

    // Called before anything the delivery waits on, so no quiet position is saved until it has finished
    Delivery delivering() {
        Read lastRead = lastReadOnThisThread.get();
        long read = lastRead == null ? 0 : lastRead.number();
        long repositionsWhenRead = lastRead == null ? repositions : lastRead.repositions();
        deliveryLock.lock();
        try {
            quietTickAllowed = false;
            deliveriesUnderWay++;
            long delivery = ++deliveries;
            if (read >= readOfLatestCurrentDelivery) {
                readOfLatestCurrentDelivery = read;
                latestCurrentDelivery = delivery;
            }
            return new Delivery(delivery, read, repositionsWhenRead);
        } finally {
            deliveryLock.unlock();
        }
    }

    // checkpoint is the one the delivery tried to store, null when the persist predicate declined it or the action
    // threw. Decided from what is stored when the delivery ends, since a put-back may have written over what its
    // save stored
    void delivered(Delivery delivery, @Nullable Checkpoint checkpoint) {
        underDeliveryLockAndSaveLock(() -> {
            deliveriesUnderWay--;
            if (delivery.number() == latestCurrentDelivery) {
                coveringPosition = checkpoint == null ? null : checkpoint.asString();
                coveringRepositions = delivery.repositions();
                positionThatThrew = null;
                storedBeforeAnyDelivery = false;
            }
            decide();
        });
    }

    // Read without the lock only to skip reading the write condition for a quiet save that would do nothing
    boolean quietTickAllowed() {
        return quietTickAllowed;
    }

    // Called holding saveLock, before a resume at a position writes it, and by a resume at the checkpoint stored with
    // the read of it
    void repositioning() {
        repositions++;
    }

    // Called holding saveLock, once the write of a resume at a position has returned, or with the read of a resume at
    // the checkpoint stored. previous is what the resume read as stored before it
    void repositionStored(Checkpoint position, @Nullable Checkpoint previous) {
        repositionMayBeStored(position, previous);
        stored(position);
    }

    // Called holding saveLock, once the write of a resume at a position threw. The position counts as the resume's
    // if a read finds it stored
    void repositionMayBeStored(Checkpoint position, @Nullable Checkpoint previous) {
        repositionOfPrevious = previous == null ? Reposition.NONE : repositionAt(previous.asString());
        latestReposition = new Reposition(repositions, position.asString());
        storedUnknown();
    }

    private Reposition repositionAt(String position) {
        if (position.equals(latestReposition.position())) {
            return latestReposition;
        }
        return position.equals(repositionOfPrevious.position()) ? repositionOfPrevious : Reposition.NONE;
    }

    // Called holding deliveryLock and saveLock, before a resume at a position that failed puts back what was stored
    // before it
    void puttingBack() {
        repositions++;
    }

    // Called holding saveLock once a write of this registration's own has returned
    void stored(Checkpoint checkpoint) {
        storedPosition = checkpoint.asString();
        storedKnown = true;
    }

    // Called holding saveLock once a write threw, since it may have written or not
    void storedUnknown() {
        storedKnown = false;
    }

    // Called holding deliveryLock and saveLock with what a read found stored
    void storedRead(@Nullable Checkpoint checkpoint) {
        storedPosition = checkpoint == null ? null : checkpoint.asString();
        storedKnown = true;
        if (checkpoint == null) {
            storedBeforeAnyDelivery = false;
        }
    }

    // Called once what is stored or the delivery it is compared with may have changed. Takes deliveryLock and then
    // saveLock, which a put-back already holds
    void decideQuietTicks() {
        underDeliveryLockAndSaveLock(this::decide);
    }

    // Called holding deliveryLock and saveLock
    private void decide() {
        if (storedKnown && storedPosition != null && storedPosition.equals(positionThatThrew)) {
            // The write that threw had written. A resume after the delivery still covers it, so
            // coveringRepositions stays
            coveringPosition = positionThatThrew;
            positionThatThrew = null;
        }
        quietTickAllowed = deliveriesUnderWay == 0 && (!storedKnown || storedCovers());
    }

    // Called holding deliveryLock and saveLock, with what is stored known. Compared by asString, since a Checkpoint
    // promises no equals
    private boolean storedCovers() {
        if (storedBeforeAnyDelivery) {
            return true;
        }
        String stored = storedPosition;
        return stored != null && (stored.equals(coveringPosition) || storedByResumeAfter(stored, latestReposition) || storedByResumeAfter(stored, repositionOfPrevious));
    }

    private boolean storedByResumeAfter(String stored, Reposition reposition) {
        return stored.equals(reposition.position()) && reposition.count() > coveringRepositions;
    }

    // Taken inside deliveryLock, so it writes nothing once this registration has stopped writing, while a delivery is
    // under way, or when a save superseded the read it was handed out at. When a write that threw left what is
    // stored unknown, it reads what is stored before it writes, and saves the quiet position only if what the read
    // found covers the delivery. The save returns once it wrote. Whatever the read or the save throws is thrown again
    // once this registration has noted it, and the caller decides whether it goes further
    void saveQuietPositionIfStoredCovers(Read handedOutAt, Checkpoint quietPosition, Runnable save, Supplier<@Nullable Checkpoint> read) {
        underDeliveryLockAndSaveLock(() -> {
            if (cancelled || superseded(handedOutAt.number(), handedOutAt.repositions(), NO_DELIVERY) || deliveriesUnderWay > 0) {
                return;
            }
            if (!storedKnown) {
                try {
                    storedRead(read.get());
                } finally {
                    // Still unknown when the read threw, so the next tick reads again
                    decide();
                }
            }
            if (!storedCovers()) {
                return;
            }
            String position = quietPosition.asString();
            saving(handedOutAt.number(), NO_DELIVERY);
            try {
                save.run();
            } catch (CheckpointWriteConditionNotFulfilledException refused) {
                // The one failure the storage reports as having written nothing
                storedUnknown();
                decide();
                throw refused;
            } catch (Throwable failure) {
                // Any other failure, checked ones from a storage written in Kotlin included, may have written
                storedUnknown();
                positionThatThrew = position;
                decide();
                throw failure;
            }
            storedPosition = position;
            coveringPosition = position;
            coveringRepositions = repositions;
            positionThatThrew = null;
            decide();
        });
    }

    // Called holding deliveryLock and saveLock once the write of the position the wrapped model restarts from after
    // it lost the history has returned. The run that restarts reads and delivers only after that position, and every
    // event before it was delivered by an earlier run or lost with the history, so it covers the deliveries that
    // ended before it
    void restartPositionStored(Checkpoint restartedFrom) {
        restartedAfterHistoryLoss();
        stored(restartedFrom);
        storedACheckpoint();
        coveringPosition = restartedFrom.asString();
        coveringRepositions = repositions;
        positionThatThrew = null;
    }

    // Called holding deliveryLock and saveLock once that write threw. It may have written, so the position covers
    // the deliveries that ended before it once a read finds it stored
    void restartPositionMayBeStored(Checkpoint restartedFrom) {
        restartedAfterHistoryLoss();
        storedUnknown();
        positionThatThrew = restartedFrom.asString();
    }

    // The run that lost the history has stopped delivering by then, so a save of a read made before it comes from a
    // run a pause stopped and would move the checkpoint back past the restart position. Also when the write threw,
    // since it may have written. The listener may run on any thread, so the save of every read made before it is
    // refused, and only a read made after it saves. The MongoDB models end the run that lost the history and read
    // again first
    private void restartedAfterHistoryLoss() {
        repositions++;
        // No delivery started so far has a later read, nor a number past deliveries
        readOfLatestCurrentDelivery = reads.get();
        latestCurrentDelivery = NO_DELIVERY;
    }

    // For tests, the positions this registration compares what is stored with, apart from those of resumes
    int positionsRemembered() {
        deliveryLock.lock();
        try {
            return (coveringPosition == null ? 0 : 1) + (positionThatThrew == null ? 0 : 1);
        } finally {
            deliveryLock.unlock();
        }
    }

    private void underDeliveryLockAndSaveLock(Runnable action) {
        deliveryLock.lock();
        try {
            saveLock.lock();
            try {
                action.run();
            } finally {
                saveLock.unlock();
            }
        } finally {
            deliveryLock.unlock();
        }
    }

    // What the write returned, or the refusal without writing once this registration writes nothing more
    <T extends @Nullable Object> T writeUnlessCancelled(Supplier<T> write, Supplier<IllegalStateException> refusal) {
        saveLock.lock();
        try {
            if (cancelled) {
                throw refusal.get();
            }
            return write.get();
        } finally {
            saveLock.unlock();
        }
    }

    // Taken by an evaluation of the start position before it reads what is stored, as a read of the wrapped model takes
    // it, so the first position it records is refused once a resume, a put-back or a restart came after that read
    long repositions() {
        return repositions;
    }

    // What the save returned. Refused without saving once this registration writes nothing more, or once a resume, a
    // put-back or a restart after a lost history came after the evaluation read that nothing was stored. Each of those
    // stores where the subscription reads from next, and a first position resolved against it could replace it with
    // an earlier one. Both writes of a first position run inside save, so no write of those comes between them
    Checkpoint saveFirstPositionUnlessCancelledOrRepositioned(long repositionsWhenRead, Supplier<Checkpoint> save,
                                                              Supplier<IllegalStateException> cancelledRefusal,
                                                              Supplier<IllegalStateException> repositionedRefusal) {
        saveLock.lock();
        try {
            if (cancelled) {
                throw cancelledRefusal.get();
            }
            if (repositionedSince(repositionsWhenRead)) {
                throw repositionedRefusal.get();
            }
            return save.get();
        } finally {
            saveLock.unlock();
        }
    }

    // Saves nothing once this registration has stopped writing or the delivery is superseded
    void saveUnlessCancelledRepositionedOrSuperseded(Delivery delivery, Checkpoint checkpoint, Runnable save) {
        saveLock.lock();
        try {
            if (cancelled || superseded(delivery.read(), delivery.repositions(), delivery.number())) {
                return;
            }
            saving(delivery.read(), delivery.number());
            try {
                save.run();
            } catch (Throwable failure) {
                storedUnknown();
                throw failure;
            }
            stored(checkpoint);
            storedACheckpoint = true;
        } finally {
            saveLock.unlock();
        }
    }

    // The one rule for the save of a delivery and of a quiet position, checked holding saveLock. A resume at a
    // position, a put-back or a restart after a lost history that came after its read supersedes it, and so does a
    // save of a later read, or of the same read and a later delivery. A run reads and delivers one event at a time on
    // one thread, so only a replaced run saves second. A quiet position comes before every delivery of its read
    private boolean superseded(long read, long repositionsWhenRead, long delivery) {
        return repositionedSince(repositionsWhenRead) || read < readOfLatestSave || (read == readOfLatestSave && delivery < latestSave);
    }

    // Checked holding saveLock. The count is moved holding it too, before the write it stands for
    private boolean repositionedSince(long repositionsWhenRead) {
        return repositions != repositionsWhenRead;
    }

    // Called holding saveLock, by a save that isn't superseded, before it writes. A save of an earlier read is
    // superseded from then on, also when the write throws, since it may have written
    private void saving(long read, long delivery) {
        readOfLatestSave = read;
        latestSave = delivery;
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
