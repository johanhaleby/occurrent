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
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSubscriptionModelTest {

    private InMemoryEventStore eventStore;
    private CompetingConsumerSubscriptionModel tested;
    private Lock lock;
    private InMemorySubscriptionModel inMemorySubscriptionModel1;
    private InMemorySubscriptionModel inMemorySubscriptionModel2;
    private ObjectMapper objectMapper;

    @BeforeEach
    void start() {
        inMemorySubscriptionModel1 = new InMemorySubscriptionModel(RetryStrategy.none());
        inMemorySubscriptionModel2 = new InMemorySubscriptionModel(RetryStrategy.none());

        InMemorySubscriptionModel composite = new InMemorySubscriptionModel(RetryStrategy.none()) {
            @Override
            public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEvent> action) {
                Subscription subscription1 = inMemorySubscriptionModel1.subscribe(subscriptionId, filter, startAtSupplier, action);
                Subscription subscription2 = inMemorySubscriptionModel2.subscribe(subscriptionId, filter, startAtSupplier, action);
                return subscription1;
            }
        };

        eventStore = new InMemoryEventStore(e -> {
            List<CloudEvent> cloudEvents = e.collect(Collectors.toList());
            inMemorySubscriptionModel1.accept(cloudEvents.stream());
            inMemorySubscriptionModel2.accept(cloudEvents.stream());
        });

        lock = new ReentrantLock();
        tested = new CompetingConsumerSubscriptionModel(composite, new InMemoryLockCompetingConsumerStrategy(lock));
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        lock.unlock();
        inMemorySubscriptionModel1.shutdown();
        inMemorySubscriptionModel2.shutdown();
    }

    @Test
    void kk() throws InterruptedException {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        String subscriptionId = UUID.randomUUID().toString();
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


    private static class InMemoryLockCompetingConsumerStrategy implements CompetingConsumersStrategy {
        private final Lock lock;

        InMemoryLockCompetingConsumerStrategy(Lock lock) {
            this.lock = lock;
        }


        @Override
        public void registerCompetingConsumer(String subscriptionId, String subscriberId) {
            new Thread(lock::lock).start();
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            new Thread(lock::unlock).start();
        }
    }
}