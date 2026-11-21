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

package org.occurrent.subscription.inmemory;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.retry.AttemptNotMade;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.time.TimeConversion;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

public class InMemorySubscriptionModelTest {

    private InMemoryEventStore inMemoryEventStore;
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private ObjectMapper objectMapper;

    @BeforeEach
    void event_store_and_subscription_model_are_initialized_before_each_test() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        inMemoryEventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void inmemory_subscription_model_refuses_a_subscription_id_that_already_exists() {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        inMemorySubscriptionModel.subscribe(subscriptionId, __ -> System.out.println("hello")).waitUntilStarted();

        // When
        Throwable throwable = catchThrowable(() -> inMemorySubscriptionModel.subscribe(subscriptionId, __ -> System.out.println("hello")).waitUntilStarted());

        // Then
        assertThat(throwable).isExactlyInstanceOf(DuplicateSubscriptionIdException.class).hasMessage("Subscription " + subscriptionId + " is already defined.");
    }

    @Test
    void refuses_a_start_position_it_does_not_support() {
        Throwable throwable = catchThrowable(() -> inMemorySubscriptionModel.subscribe("subscription1", StartAt.checkpoint(new StringBasedCheckpoint("343")), __ -> {
        }));

        assertThat(throwable).isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessage("InMemorySubscriptionModel only supports starting from 'now' and 'default' (StartAt.now() or StartAt.subscriptionModelDefault())");
    }
    
    @Test
    void inmemory_subscription_model_allows_cancelling_a_subscription() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CountDownLatch eventReceived = new CountDownLatch(1);
        String subscriberId = UUID.randomUUID().toString();
        inMemorySubscriptionModel.subscribe(subscriberId, __ -> eventReceived.countDown()).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");

        // When
        inMemoryEventStore.write("1", serialize(nameDefined1));
        // The subscription is async so we need to wait for it
        assertThat(eventReceived.await(1, SECONDS))
                .as("the event must have been delivered before the subscription is cancelled")
                .isTrue();

        inMemorySubscriptionModel.cancelSubscription(subscriberId);

        // Then
        assertAll(
                () -> assertThat(inMemorySubscriptionModel.isRunning(subscriberId)).isFalse(),
                () -> assertThat(inMemorySubscriptionModel.isPaused(subscriberId)).isFalse()
        );
    }

    @Test
    void a_cancelled_subscription_delivers_none_of_the_events_it_had_queued() throws InterruptedException {
        // Given: the action holds on to the first event, so the next two wait in the subscription's queue
        CountDownLatch handlingFirstEvent = new CountDownLatch(1);
        CountDownLatch releaseFirstEvent = new CountDownLatch(1);
        CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        inMemorySubscriptionModel.subscribe(subscriptionId, e -> {
            handled.add(e);
            handlingFirstEvent.countDown();
            try {
                releaseFirstEvent.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        LocalDateTime now = LocalDateTime.now();
        inMemoryEventStore.write("1", serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));
        inMemoryEventStore.write("2", serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name2")));
        inMemoryEventStore.write("3", serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name3")));
        assertThat(handlingFirstEvent.await(5, SECONDS)).isTrue();

        // When
        inMemorySubscriptionModel.cancelSubscription(subscriptionId);
        releaseFirstEvent.countDown();
        Thread.sleep(1000);

        // Then
        assertThat(handled).describedAs("events delivered to the cancelled subscription").hasSize(1);
    }

    @Test
    void a_subscription_goes_on_delivering_after_the_action_withdrew_an_event() throws InterruptedException {
        // Given
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        CountDownLatch laterEventsHandled = new CountDownLatch(2);
        String subscriptionId = UUID.randomUUID().toString();
        inMemorySubscriptionModel.subscribe(subscriptionId, cloudEvent -> {
            if (cloudEvent.getId().equals("withdrawn")) {
                throw new AttemptNotMade("withdrawn");
            }
            handled.add(cloudEvent.getId());
            laterEventsHandled.countDown();
        }).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        LocalDateTime now = LocalDateTime.now();

        // When
        inMemoryEventStore.write("1", serialize(new NameDefined("withdrawn", now, "name", "name1")));
        inMemoryEventStore.write("2", serialize(new NameDefined("later1", now, "name", "name2")));
        inMemoryEventStore.write("3", serialize(new NameDefined("later2", now, "name", "name3")));
        laterEventsHandled.await(5, SECONDS);

        // Then
        assertThat(handled).as("the events the action handled after the one it withdrew").containsExactly("later1", "later2");
    }

    @Test
    void a_subscription_that_is_cancelled_while_the_action_withdraws_an_event_ends_its_task_without_an_exception() throws InterruptedException {
        // Given
        ObservableDispatcher dispatcher = new ObservableDispatcher();
        InMemorySubscriptionModel model = new InMemorySubscriptionModel(dispatcher, RetryStrategy.fixed(200));
        InMemoryEventStore eventStore = new InMemoryEventStore(model);
        CountDownLatch handlingEvent = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        String subscriptionId = UUID.randomUUID().toString();

        try {
            model.subscribe(subscriptionId, cloudEvent -> {
                handlingEvent.countDown();
                try {
                    cancelled.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new AttemptNotMade("withdrawn");
            }).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            eventStore.write("1", serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));
            assertThat(handlingEvent.await(5, SECONDS)).as("the event is in the action").isTrue();

            // When
            model.cancelSubscription(subscriptionId);
            cancelled.countDown();
            boolean taskEnded = dispatcher.taskEnded.await(5, SECONDS);

            // Then
            assertAll(
                    () -> assertThat(taskEnded).as("the subscription's task ended").isTrue(),
                    () -> assertThat(dispatcher.thrownByATask).as("what left the subscription's task").isEmpty()
            );
        } finally {
            cancelled.countDown();
            model.shutdown();
        }
    }

    // Records what leaves each task it runs and when one ends
    private static final class ObservableDispatcher extends ThreadPoolExecutor {
        final CopyOnWriteArrayList<Throwable> thrownByATask = new CopyOnWriteArrayList<>();
        final CountDownLatch taskEnded = new CountDownLatch(1);

        ObservableDispatcher() {
            super(0, Integer.MAX_VALUE, 60, SECONDS, new SynchronousQueue<>());
        }

        @Override
        protected void afterExecute(Runnable task, Throwable thrown) {
            if (thrown != null) {
                thrownByATask.add(thrown);
            }
            taskEnded.countDown();
        }
    }

    private List<CloudEvent> serialize(DomainEvent e) {
        return List.of(io.cloudevents.core.builder.CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(TimeConversion.toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }
}