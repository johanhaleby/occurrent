package org.occurrent.subscription.reactor.durable.catchup;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.LongStream;

// Probe for 3a (dec-0229): what a subscribe, a cancel, a pause, a resume and isRunning do on the reactor stream catch-up
// model once it, or only the model it wraps, has shut down. Prints one line per call and asserts nothing.
class Probe3aShutdownTest {

    @Test
    void after_the_catchup_model_shut_down() {
        run("catch-up shut down", true);
    }

    @Test
    void after_only_the_wrapped_model_shut_down() {
        run("wrapped shut down", false);
    }

    private static void run(String label, boolean catchupShutsDown) {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new Reader());
        List<String> delivered = new CopyOnWriteArrayList<>();
        // live: straight to live, made before the shutdown. replayed: a catch-up that handed over before the shutdown
        catchup.subscribe("live", StreamSubscriptionFilter.filter(Filter.all()), StartAt.now(), e -> Mono.fromRunnable(() -> delivered.add("live:" + e.getId())));
        catchup.subscribe("replayed", StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                e -> Mono.fromRunnable(() -> delivered.add("replayed:" + e.getId()))).waitUntilStarted().block(Duration.ofSeconds(5));
        if (catchupShutsDown) {
            catchup.shutdown();
        } else {
            wrapped.shutdown();
        }
        System.out.println("PROBE3A[" + label + "] wrapped.shutdown called=" + wrapped.shutdown + ", wrapped holds=" + wrapped.subscribeCalls);
        report(label, "subscribe new id from now", () -> catchup.subscribe("new-live", StreamSubscriptionFilter.filter(Filter.all()), StartAt.now(), e -> Mono.empty()).waitUntilStarted().block(Duration.ofSeconds(5)));
        report(label, "subscribe new id with catch-up", () -> catchup.subscribe("new-replayed", StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), e -> Mono.empty()).waitUntilStarted().block(Duration.ofSeconds(5)));
        report(label, "subscribe live again", () -> catchup.subscribe("live", StreamSubscriptionFilter.filter(Filter.all()), StartAt.now(), e -> Mono.empty()).waitUntilStarted().block(Duration.ofSeconds(5)));
        report(label, "isRunning(live)", () -> catchup.isRunning("live"));
        report(label, "isPaused(live)", () -> catchup.isPaused("live"));
        report(label, "isRunning()", catchup::isRunning);
        report(label, "pause(live)", () -> { catchup.pauseSubscription("live"); return null; });
        report(label, "pause(replayed)", () -> { catchup.pauseSubscription("replayed"); return null; });
        report(label, "pause(unknown)", () -> { catchup.pauseSubscription("unknown"); return null; });
        report(label, "resume(live)", () -> catchup.resumeSubscription("live").waitUntilStarted().block(Duration.ofSeconds(5)));
        report(label, "cancel(live)", () -> catchup.cancelSubscription("live").block(Duration.ofSeconds(5)));
        report(label, "cancel(replayed)", () -> catchup.cancelSubscription("replayed").block(Duration.ofSeconds(5)));
        report(label, "cancel(unknown)", () -> catchup.cancelSubscription("unknown").block(Duration.ofSeconds(5)));
        report(label, "stop()", () -> { catchup.stop(); return null; });
        report(label, "start(true)", () -> { catchup.start(true); return null; });
        report(label, "shutdown() again", () -> { catchup.shutdown(); return null; });
        System.out.println("PROBE3A[" + label + "] wrapped calls after the shutdown=" + wrapped.callsAfterShutdown + ", delivered=" + delivered);
    }

    private static void report(String label, String call, Supplier<?> step) {
        String outcome;
        try {
            Object answer = step.get();
            outcome = "returned " + answer;
        } catch (Throwable t) {
            outcome = "threw " + t.getClass().getSimpleName() + ": " + t.getMessage();
        }
        System.out.println("PROBE3A[" + label + "] " + call + " -> " + outcome);
    }

    static final class Reader implements UnscopedPositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            long from = range.afterPosition().orElse(0L) + 1;
            long to = Math.min(range.upToPosition().orElse(3L), 3L);
            return Flux.fromStream(LongStream.rangeClosed(from, to).boxed()
                    .map(position -> CloudEventBuilder.v1().withId("e" + position).withSource(URI.create("urn:test")).withType("type").build()));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(3L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // Answers after shutdown() as ReactorMongoSubscriptionModel does: subscribe throws SubscriptionModelShutdownException,
    // pause and resume throw IllegalStateException, cancel answers an empty Mono, isRunning answers false.
    static final class WrappedModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        static final Checkpoint TOKEN = new StringBasedCheckpoint("token");
        final List<String> subscribeCalls = new CopyOnWriteArrayList<>();
        final Set<String> paused = ConcurrentHashMap.newKeySet();
        final List<String> callsAfterShutdown = new CopyOnWriteArrayList<>();
        volatile boolean running = true;
        volatile boolean shutdown = false;

        private void called(String call) {
            if (shutdown) {
                callsAfterShutdown.add(call);
            }
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(TOKEN);
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return Mono.just(TOKEN);
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("cold primitive"));
        }

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            called("subscribe:" + subscriptionId);
            if (subscribeCalls.contains(subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            if (shutdown) {
                throw new SubscriptionModelShutdownException();
            }
            subscribeCalls.add(subscriptionId);
            return handle(subscriptionId);
        }

        @Override
        public synchronized Mono<Void> cancelSubscription(String subscriptionId) {
            called("cancel:" + subscriptionId);
            subscribeCalls.remove(subscriptionId);
            paused.remove(subscriptionId);
            return Mono.empty();
        }

        @Override
        public synchronized void stop() {
            called("stop");
            if (shutdown) {
                return;
            }
            running = false;
            paused.addAll(subscribeCalls);
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            called("start");
            if (shutdown) {
                return;
            }
            running = true;
            if (resumeSubscriptionsAutomatically) {
                paused.clear();
            }
        }

        @Override
        public synchronized void shutdown() {
            called("shutdown");
            shutdown = true;
            running = false;
            subscribeCalls.clear();
            paused.clear();
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return !shutdown && subscribeCalls.contains(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return !shutdown && paused.contains(subscriptionId);
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            called("resume:" + subscriptionId);
            if (shutdown) {
                throw new IllegalStateException("WrappedModel is shutdown");
            }
            if (!subscribeCalls.contains(subscriptionId)) {
                throw new UnknownSubscriptionException(subscriptionId);
            }
            paused.remove(subscriptionId);
            return handle(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            called("pause:" + subscriptionId);
            if (shutdown) {
                throw new IllegalStateException("WrappedModel is shutdown");
            }
            if (!subscribeCalls.contains(subscriptionId)) {
                throw new UnknownSubscriptionException(subscriptionId);
            }
            paused.add(subscriptionId);
        }

        private static Subscription handle(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public Mono<Void> waitUntilStarted() {
                    return Mono.empty();
                }
            };
        }
    }
}
