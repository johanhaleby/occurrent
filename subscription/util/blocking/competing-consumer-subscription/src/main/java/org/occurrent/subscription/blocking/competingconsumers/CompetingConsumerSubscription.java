package org.occurrent.subscription.blocking.competingconsumers;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

@NullMarked
public class CompetingConsumerSubscription implements Subscription {
    private final String subscriptionId;
    private final String subscriberId;
    @Nullable
    private final Subscription subscription;
    private final StartRefusal startRefusal;

    CompetingConsumerSubscription(String subscriptionId, String subscriberId, StartRefusal startRefusal, @Nullable Subscription subscription) {
        this.subscriptionId = subscriptionId;
        this.subscriberId = subscriberId;
        this.startRefusal = startRefusal;
        this.subscription = subscription;
    }

    String getSubscriberId() {
        return subscriberId;
    }

    Optional<Subscription> getSubscription() {
        return Optional.ofNullable(subscription);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof CompetingConsumerSubscription that)) return false;
        return Objects.equals(subscriptionId, that.subscriptionId) && Objects.equals(subscriberId, that.subscriberId) && Objects.equals(subscription, that.subscription);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriptionId, subscriberId, subscription);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CompetingConsumerSubscription.class.getSimpleName() + "[", "]")
                .add("subscriptionId='" + subscriptionId + "'")
                .add("subscriberId='" + subscriberId + "'")
                .add("subscription=" + subscription)
                .toString();
    }

    @Override
    public String id() {
        return subscriptionId;
    }

    @Override
    public boolean waitUntilStarted(Duration timeout) {
        startRefusal.throwIfRefused();
        long started = System.nanoTime();
        try {
            return getSubscription().map(s -> s.waitUntilStarted(timeout)).orElse(true);
        } catch (UnsupportedStartAtException refusedInTheWrappedModel) {
            // The wrapped handle throws as soon as the wrapped model refused, before the model here has given the lease
            // back and freed the id, so a subscribe of the id right after the throw could find it still in use. A wrapped
            // model that holds the subscription paused instead says so, which ends the wait once the model here has held
            // it paused too and given the lease back, or has found that the hold ended first
            startRefusal.awaitActedOn(refusedInTheWrappedModel, timeout, started);
            startRefusal.throwIfRefused();
            throw refusedInTheWrappedModel;
        }
    }

    // Shared by every handle made for one competing consumer, from its subscribe until the model forgets it. A refusal
    // the model acted on ends the wait of every handle, so after one it acted on without forgetting the consumer, a
    // handle whose wrapped handle a later refusal throws from no longer waits, and can throw before the id is free. That
    // includes a handle a resume returns. A wait per refusal could not tell which report a handle's refusal is, since
    // the wrapped model can throw before it reports, and waiting for the wrong one would hold the handle for its whole
    // timeout. A hold can be told apart. The wrapped model gives each copy of a held refusal, the one a handle throws and
    // the one each listener gets, the same cause, so a handle waits for the report of its own hold.
    static final class StartRefusal {
        // Reported by the wrapped model, until the competing consumer model has acted on it
        final AtomicReference<@Nullable UnsupportedStartAtException> reported = new AtomicReference<>();
        // When the wrapped model reported the refusal, set before reported
        volatile @Nullable Instant reportedAt;
        // Reported by the wrapped model each time it holds the consumer paused, with an UnsupportedStartAtException when
        // it got no position at the present and a CatchupCannotResumeException when the history its subscription would
        // go live from is gone, until the competing consumer model has acted on it. Each report is kept, since a late one
        // for an earlier hold can come after the one for the hold the wrapped model is in now
        final Set<RuntimeException> heldPaused = ConcurrentHashMap.newKeySet();
        private volatile @Nullable UnsupportedStartAtException refused;
        // Only waited for when the wrapped model reports its refusals, since nothing would act on one otherwise
        private final boolean reportedByTheWrappedModel;
        private final Object monitor = new Object();
        // Set once a refusal was acted on, and once the consumer is cancelled or the model shuts down
        private boolean actedOn;
        // The causes of the holds whose report the model acted on, or dropped because the hold had ended. A late report
        // of a subscribe cancelled since adds one no handle of this subscribe waits for
        private final Set<Throwable> settledHolds = new HashSet<>();

        StartRefusal(boolean reportedByTheWrappedModel) {
            this.reportedByTheWrappedModel = reportedByTheWrappedModel;
        }

        void refused(UnsupportedStartAtException refusal) {
            refused = refusal;
            actedOn();
        }

        // Once a refusal was acted on without forgetting the consumer, and once the consumer is cancelled or the model
        // shuts down
        void actedOn() {
            synchronized (monitor) {
                actedOn = true;
                monitor.notifyAll();
            }
        }

        // Once a report of a hold was acted on, or dropped because the hold had ended
        void holdSettled(UnsupportedStartAtException report) {
            synchronized (monitor) {
                settledHolds.add(heldBy(report));
                monitor.notifyAll();
            }
        }

        private void awaitActedOn(UnsupportedStartAtException thrown, Duration timeout, long started) {
            if (!reportedByTheWrappedModel) {
                return;
            }
            long timeoutNanos = timeout.compareTo(Duration.ofNanos(Long.MAX_VALUE)) >= 0 ? Long.MAX_VALUE : timeout.toNanos();
            Throwable hold = heldBy(thrown);
            synchronized (monitor) {
                try {
                    while (!actedOn && !settledHolds.contains(hold)) {
                        long left = timeoutNanos - (System.nanoTime() - started);
                        if (left <= 0) {
                            return;
                        }
                        TimeUnit.NANOSECONDS.timedWait(monitor, left);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // The cause every copy of one refusal has, or the refusal itself when it isn't a copy
        private static Throwable heldBy(UnsupportedStartAtException refusal) {
            @Nullable Throwable cause = refusal.getCause();
            return cause == null ? refusal : cause;
        }

        // A copy for every handle that throws it, so a caller adding to what it caught changes nothing another caller gets
        private void throwIfRefused() {
            UnsupportedStartAtException refusal = refused;
            if (refusal != null) {
                UnsupportedStartAtException copy = new UnsupportedStartAtException(refusal.startAt(), requireNonNull(refusal.getMessage()));
                copy.initCause(refusal);
                throw copy;
            }
        }
    }
}
