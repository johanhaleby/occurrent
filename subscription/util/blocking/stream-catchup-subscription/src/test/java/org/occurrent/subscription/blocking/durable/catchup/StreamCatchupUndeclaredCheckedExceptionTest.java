/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.subscription.blocking.durable.catchup;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A live model or a catch-up listener can throw a checked exception without declaring it, which a Kotlin
 * implementation does with a plain {@code throw}. The clean-up after a failed subscribe or a failed resume at a
 * position must run for it as for any other failure, and the failure that started it must reach the caller as it was
 * thrown.
 */
@DisplayName("StreamCatchupUndeclaredCheckedException")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupUndeclaredCheckedExceptionTest {

    private static final String ID = "subscription";
    private static final Checkpoint TOKEN = new StringBasedCheckpoint("token-1");
    private static final Checkpoint LATER_TOKEN = new StringBasedCheckpoint("later-token");

    private final CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
    private final AtomicReference<Throwable> catchupStartedThrows = new AtomicReference<>();
    private final CountDownLatch deliveryMayReturn = new CountDownLatch(1);
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private InMemoryEventStore eventStore;
    private List<DomainEvent> history;
    private HistoryLosingLiveModel liveModel;
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        LocalDateTime time = LocalDateTime.now();
        history = List.of(nameDefined(time, "event1"), nameDefined(time, "event2"), nameDefined(time, "event3"));
        history.forEach(this::write);
        liveModel = new HistoryLosingLiveModel();
    }

    private void create(CheckpointAwareSubscriptionModel live) {
        catchup = new StreamCatchupSubscriptionModel(live, eventStore, new CatchupSubscriptionModelConfig(100));
        catchup.listenForCatchup(ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                Throwable failure = catchupStartedThrows.get();
                if (failure != null) {
                    throw sneaky(failure);
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
    }

    @AfterEach
    void shutdown() {
        deliveryMayReturn.countDown();
        if (catchup != null) {
            catchup.shutdown();
        }
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void a_loss_for_an_id_whose_subscribe_threw_a_checked_exception_without_declaring_it_is_left_to_the_live_model_since_the_catch_up_owns_nothing() {
        // Given
        IOException liveFailure = new IOException("the live model lost its connection");
        liveModel.failingTheNextSubscribeWith(liveFailure);
        liveModel.withoutHistoryBack(LATER_TOKEN);

        // When
        create(liveModel);
        Throwable thrown = catchThrowable(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action()));
        liveModel.subscribe(ID, null, StartAt.checkpoint(LATER_TOKEN), cloudEvent -> {
        });

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown by the subscribe").isSameAs(liveFailure),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").containsExactly(ID),
                () -> assertThat(liveModel.isPaused(ID)).as("paused on the live model").isFalse()
        );
    }

    @Test
    void throws_what_the_live_model_throws_with_the_checked_exception_the_listener_threw_without_declaring_it_as_suppressed_when_a_resume_at_a_checkpoint_puts_the_held_replay_back() throws Exception {
        // Given a takeover replay that is inside the action
        LiveStartLosingSubscriptionModel takeoverModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false,
                () -> write(nameDefined(LocalDateTime.now(), "writtenInTheGap")));
        create(takeoverModel);
        CountDownLatch inTheTakeoverReplay = new CountDownLatch(1);
        AtomicInteger deliveries = new AtomicInteger();
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
            if (deliveries.incrementAndGet() == history.size() + 1) {
                inTheTakeoverReplay.countDown();
                awaitUnchecked(deliveryMayReturn);
            }
        });
        assertThat(inTheTakeoverReplay.await(10, TimeUnit.SECONDS)).as("whether the takeover replay is in the action").isTrue();
        IOException liveFailure = new IOException("the live model lost its connection");
        IOException listenerFailure = new IOException("the listener lost its connection");
        takeoverModel.beforeTheNextResumeAt(() -> {
            throw sneaky(liveFailure);
        });

        // When a resume at a checkpoint holds the replay and the live model then refuses, while the listener throws as the replay would start again
        CompletableFuture<Throwable> resuming = CompletableFuture.supplyAsync(
                () -> catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(TOKEN))), runnable -> Thread.ofPlatform().daemon().start(runnable));
        await().untilAsserted(() -> assertThat(catchup.isCatchingUp(ID)).as("whether the replay is held by the resume").isFalse());
        catchupStartedThrows.set(listenerFailure);
        deliveryMayReturn.countDown();
        Throwable thrown = resuming.get(20, TimeUnit.SECONDS);

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown by the resume at a checkpoint").isSameAs(liveFailure),
                () -> assertThat(thrown.getSuppressed()).as("suppressed").containsExactly(listenerFailure),
                () -> assertThat(catchup.isCatchingUp(ID)).as("catching up").isFalse()
        );
    }

    private static void awaitUnchecked(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> RuntimeException sneaky(Throwable throwable) throws E {
        throw (E) throwable;
    }

    private Consumer<CloudEvent> action() {
        return cloudEvent -> received.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    private static NameDefined nameDefined(LocalDateTime time, String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private void write(DomainEvent event) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event));
        eventStore.write(event.eventId(), cloudEvents);
    }
}
