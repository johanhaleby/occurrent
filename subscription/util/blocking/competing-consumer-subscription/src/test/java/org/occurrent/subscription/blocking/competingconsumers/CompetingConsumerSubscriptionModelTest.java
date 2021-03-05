package org.occurrent.subscription.blocking.competingconsumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumersStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSubscriptionModelTest {

    private InMemoryEventStore eventStore;
    private CompetingConsumerSubscriptionModel tested;
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private ObjectMapper objectMapper;

    @BeforeEach
    void start() {
        BlockingQueue<CloudEvent> queue = new LinkedBlockingQueue<>();

        inMemorySubscriptionModel = new InMemorySubscriptionModel(Executors.newCachedThreadPool(), RetryStrategy.none(), () -> queue);
        InMemoryCompetingConsumerStrategy inMemoryCompetingConsumerStrategy = new InMemoryCompetingConsumerStrategy(queue);
        tested = new CompetingConsumerSubscriptionModel(inMemorySubscriptionModel, inMemoryCompetingConsumerStrategy);

        eventStore = new InMemoryEventStore(inMemoryCompetingConsumerStrategy);

        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void kk() throws InterruptedException {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        String subscriptionId = UUID.randomUUID().toString();
        tested.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();
        tested.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("eventId", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "my name");

        // When
        eventStore.write("streamId", serialize(nameDefined));

        // Then
        Thread.sleep(1000);
        assertThat(cloudEvents).hasSize(1);
    }


    @SuppressWarnings("ConstantConditions")
    private DomainEvent deserialize(CloudEvent e) {
        try {
            return (DomainEvent) objectMapper.readValue(e.getData().toBytes(), Class.forName(e.getType()));
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private Stream<CloudEvent> serialize(DomainEvent e) {
        return Stream.of(CloudEventBuilder.v1()
                .withId(e.getEventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.getTimestamp()).atOffset(UTC))
                .withSubject(e.getName())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }


    private class InMemoryQueueSubscriptionModel implements SubscriptionModel, SubscriptionModelLifeCycle {

        @Override
        public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEvent> action) {
            return null;
        }

        @Override
        public void cancelSubscription(String subscriptionId) {

        }

        @Override
        public void stop() {

        }

        @Override
        public void start() {

        }

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return null;
        }

        @Override
        public void pauseSubscription(String subscriptionId) {

        }
    }

    private static class InMemoryCompetingConsumerStrategy implements CompetingConsumersStrategy, Consumer<Stream<CloudEvent>> {
        private final Set<Consumer> consumers;
        private final CopyOnWriteArrayList<CompetingConsumerListener> listeners;
        private final BlockingQueue<CloudEvent> queue;

        private InMemoryCompetingConsumerStrategy(BlockingQueue<CloudEvent> queue) {
            this.queue = queue;
            this.consumers = Collections.newSetFromMap(new ConcurrentHashMap<>());
            this.listeners = new CopyOnWriteArrayList<>();
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            consumers.add(new Consumer(subscriptionId, subscriberId));
            listeners.forEach(cc -> cc.onConsumeGranted(subscriptionId, subscriberId));
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            consumers.remove(new Consumer(subscriptionId, subscriberId));
            listeners.forEach(cc -> cc.onConsumeProhibited(subscriptionId, subscriberId));
        }

        @Override
        public boolean isRegisteredCompetingConsumer(String subscriptionId, String subscriberId) {
            return consumers.contains(new Consumer(subscriptionId, subscriberId));
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return false;
        }

        @Override
        public void addListener(CompetingConsumerListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {

        }

        @Override
        public void accept(Stream<CloudEvent> cloudEventStream) {
            cloudEventStream.forEach(queue::offer);
        }

        private static class Consumer {
            private final String subscriptionId;
            private final String subscriberId;

            Consumer(String subscriptionId, String subscriberId) {
                this.subscriptionId = subscriptionId;
                this.subscriberId = subscriberId;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof Consumer)) return false;
                Consumer consumer = (Consumer) o;
                return Objects.equals(subscriptionId, consumer.subscriptionId) && Objects.equals(subscriberId, consumer.subscriberId);
            }

            @Override
            public int hashCode() {
                return Objects.hash(subscriptionId, subscriberId);
            }
        }
    }
}