package org.occurrent.subscription.blocking.competingconsumers;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
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
            // model that holds the subscription paused instead says so, which ends the wait with nothing given back
            startRefusal.awaitActedOn(timeout, started);
            startRefusal.throwIfRefused();
            throw refusedInTheWrappedModel;
        }
    }

    // Shared by every handle made for one competing consumer, from its subscribe until the model forgets it. Counted down
    // once, so after a report the model acted on without forgetting the consumer, or after the wrapped model held the
    // subscription paused, a handle whose wrapped handle a later refusal throws from no longer waits, and can throw
    // before the id is free. That includes a handle a resume returns. A count per report could not tell which report a
    // handle's refusal is, since the wrapped model can throw before it reports, and waiting for the wrong one would hold
    // the handle for its whole timeout.
    static final class StartRefusal {
        // Reported by the wrapped model, until the competing consumer model has acted on it
        final AtomicReference<@Nullable UnsupportedStartAtException> reported = new AtomicReference<>();
        private volatile @Nullable UnsupportedStartAtException refused;
        // Only waited for when the wrapped model reports its refusals, since nothing would act on one otherwise
        private final boolean reportedByTheWrappedModel;
        private final CountDownLatch actedOn = new CountDownLatch(1);

        StartRefusal(boolean reportedByTheWrappedModel) {
            this.reportedByTheWrappedModel = reportedByTheWrappedModel;
        }

        void refused(UnsupportedStartAtException refusal) {
            refused = refusal;
            actedOn.countDown();
        }

        // Once a report was acted on without forgetting the consumer, and once the model shuts down
        void actedOn() {
            actedOn.countDown();
        }

        private void awaitActedOn(Duration timeout, long started) {
            if (!reportedByTheWrappedModel) {
                return;
            }
            long timeoutNanos = timeout.compareTo(Duration.ofNanos(Long.MAX_VALUE)) >= 0 ? Long.MAX_VALUE : timeout.toNanos();
            long left = timeoutNanos - (System.nanoTime() - started);
            try {
                //noinspection ResultOfMethodCallIgnored
                actedOn.await(Math.max(0, left), TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
