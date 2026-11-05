package org.occurrent.subscription.reactor.durable;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// Probe for dec-0227 (Durable E1): an event written while a dynamic start function runs, which then answers now()
class ProbeE1DynamicNowTest {
    private static final String ID = "e1";

    @ParameterizedTest(name = "handsOver={0}")
    @ValueSource(booleans = {false, true})
    void an_event_written_while_a_dynamic_start_runs_that_answers_now_is_delivered(boolean handsOver) {
        Feed feed = handsOver ? new NamedFeed() : new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        List<String> delivered = new CopyOnWriteArrayList<>();
        try {
            Subscription s = model.subscribe(ID, null, StartAt.dynamic(() -> {
                feed.write();
                return StartAt.now();
            }), e -> Mono.fromRunnable(() -> delivered.add(e.getId())));
            s.waitUntilStarted().block(Duration.ofSeconds(5));
            feed.write();
            await().atMost(Duration.ofSeconds(5)).until(() -> delivered.contains("2"));
            assertThat(delivered).as("events delivered, 1 written while the dynamic start ran, 2 after the subscribe").containsExactly("1", "2");
        } finally {
            model.shutdown();
        }
    }

    // Control: StartAt.now() with the write after the call returns
    @ParameterizedTest(name = "handsOver={0}")
    @ValueSource(booleans = {false, true})
    void control_an_event_written_after_a_subscribe_from_now_is_delivered(boolean handsOver) {
        Feed feed = handsOver ? new NamedFeed() : new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        List<String> delivered = new CopyOnWriteArrayList<>();
        try {
            model.subscribe(ID, null, StartAt.now(), e -> Mono.fromRunnable(() -> delivered.add(e.getId()))).waitUntilStarted().block(Duration.ofSeconds(5));
            feed.write();
            await().atMost(Duration.ofSeconds(5)).until(() -> delivered.contains("1"));
            assertThat(delivered).containsExactly("1");
        } finally {
            model.shutdown();
        }
    }

    static CloudEvent eventAt(long position) {
        CloudEvent event = CloudEventBuilder.v1().withId(String.valueOf(position)).withSource(URI.create("urn:test")).withType("Something").build();
        return new CheckpointAwareCloudEvent(event, new StringBasedCheckpoint(String.valueOf(position)));
    }

    static long startOf(StartAt startAt, long present) {
        StartAt resolved = startAt;
        while (resolved != null && resolved.isDynamic()) {
            resolved = resolved.get(new SubscriptionModelContext(ProbeE1DynamicNowTest.class));
        }
        if (resolved == null || resolved.isNow() || resolved.isDefault()) {
            return present;
        }
        return Long.parseLong(resolved.toString());
    }

    static class Feed implements CheckpointAwareSubscriptionModel {
        final AtomicLong present = new AtomicLong();
        final Sinks.Many<CloudEvent> written = Sinks.many().replay().all();

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.defer(() -> {
                long start = startOf(startAt, present.get());
                return written.asFlux().filter(event -> Long.parseLong(event.getId()) > start);
            });
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.fromSupplier(() -> new StringBasedCheckpoint(String.valueOf(present.get())));
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            long atTheCall = present.get();
            return Mono.fromSupplier(() -> new StringBasedCheckpoint(String.valueOf(atTheCall)));
        }

        synchronized long write() {
            long position = present.incrementAndGet();
            written.tryEmitNext(eventAt(position));
            return position;
        }
    }

    static final class NamedFeed extends Feed implements SubscriptionModel {
        final Map<String, Named> subscriptions = new ConcurrentHashMap<>();
        private final List<Long> log = new CopyOnWriteArrayList<>();
        private volatile boolean running = true;

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            Named named = new Named(startAt, action);
            if (subscriptions.putIfAbsent(subscriptionId, named) != null) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            if (running) {
                begin(named);
            }
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

        @Override
        synchronized long write() {
            long position = present.incrementAndGet();
            log.add(position);
            subscriptions.values().stream().filter(named -> named.start >= 0 && position > named.start)
                    .forEach(named -> named.action.apply(eventAt(position)).block());
            return position;
        }

        private void begin(Named named) {
            named.start = startOf(named.startAt, present.get());
            log.stream().filter(position -> position > named.start).forEach(position -> named.action.apply(eventAt(position)).block());
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            subscriptions.remove(subscriptionId);
            return Mono.empty();
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
            subscriptions.values().stream().filter(named -> named.start < 0).forEach(this::begin);
        }

        @Override
        public synchronized void stop() {
            running = false;
        }

        @Override
        public void shutdown() {
            subscriptions.clear();
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return running && subscriptions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return !running && subscriptions.containsKey(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        static final class Named {
            final StartAt startAt;
            final Function<CloudEvent, Mono<Void>> action;
            volatile long start = -1;

            Named(StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
                this.startAt = startAt;
                this.action = action;
            }
        }
    }
}
