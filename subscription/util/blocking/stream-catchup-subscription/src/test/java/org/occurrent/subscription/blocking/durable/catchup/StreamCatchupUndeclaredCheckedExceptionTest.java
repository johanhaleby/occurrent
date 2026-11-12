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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
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
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
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
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A live model or a catch-up listener can throw a checked exception without declaring it, which a Kotlin
 * implementation does with a plain {@code throw}. The clean-up after a failed subscribe or a failed resume at a
 * position must run for it as for any other failure, and the failure that started it must reach the caller as it was
 * thrown. A listener that throws one when a replay starts holds the replay, and a live model that throws one from a
 * cancel makes a later resume wait for the replay the cancel ended, both as a {@code RuntimeException} does.
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
    private final AtomicInteger deliveries = new AtomicInteger();
    private final AtomicInteger inAction = new AtomicInteger();
    private final AtomicInteger mostInAction = new AtomicInteger();
    private final Logger catchupLogger = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
    private final LoggedEvents logged = new LoggedEvents();
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
        logged.start();
        catchupLogger.addAppender(logged);
    }

    private void create(CheckpointAwareSubscriptionModel live) {
        create(live, new CatchupSubscriptionModelConfig(100));
    }

    private void create(CheckpointAwareSubscriptionModel live, CatchupSubscriptionModelConfig config) {
        catchup = new StreamCatchupSubscriptionModel(live, eventStore, config);
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
        catchupLogger.detachAppender(logged);
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
    void holds_the_replay_and_logs_the_checked_exception_the_listener_threw_without_declaring_it_when_a_subscribe_starts_its_replay() {
        // Given
        IOException listenerFailure = new IOException("the listener lost its connection");
        catchupStartedThrows.set(listenerFailure);
        create(liveModel);

        // When
        Throwable thrown = catchThrowable(() -> catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action()));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown by the subscribe").isNull(),
                () -> assertThat(catchup.hasParkedReplay(ID)).as("the replay is held").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(catchup.isCatchingUp(ID)).as("catching up").isFalse(),
                () -> assertThat(received).as("received").isEmpty(),
                () -> assertThat(listenerFailuresLogged()).as("listener failures logged at ERROR").containsExactly("java.io.IOException: the listener lost its connection")
        );
    }

    @Test
    void runs_the_replay_a_subscribe_held_when_the_listener_threw_a_checked_exception_without_declaring_it_once_the_subscription_is_resumed() {
        // Given
        catchupStartedThrows.set(new IOException("the listener lost its connection"));
        create(liveModel);
        Throwable thrownBySubscribe = catchThrowable(() -> catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action()));
        catchupStartedThrows.set(null);

        // When
        Throwable thrownByResume = catchThrowable(() -> catchup.resumeSubscription(ID).waitUntilStarted(Duration.ofSeconds(15)));

        // Then
        assertAll(
                () -> assertThat(thrownBySubscribe).as("thrown by the subscribe").isNull(),
                () -> assertThat(thrownByResume).as("thrown by the resume").isNull(),
                () -> assertThat(received).as("received").containsExactlyElementsOf(history),
                () -> assertThat(catchup.hasParkedReplay(ID)).as("the replay is held").isFalse()
        );
    }

    @Test
    void throws_only_what_the_live_model_throws_and_logs_the_checked_exception_the_listener_threw_without_declaring_it_when_a_resume_at_a_checkpoint_puts_the_held_replay_back() throws Exception {
        // Given
        IOException liveFailure = new IOException("the live model lost its connection");
        IOException listenerFailure = new IOException("the listener lost its connection");

        // When
        Throwable thrown = resumeAtACheckpointWhileTheListenerThrowsAsTheHeldReplayIsPutBack(listenerFailure, liveFailure);

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown by the resume at a checkpoint").isSameAs(liveFailure),
                () -> assertThat(thrown.getSuppressed()).as("suppressed").isEmpty(),
                () -> assertThat(listenerFailuresLogged()).as("listener failures logged at ERROR").containsExactly("java.io.IOException: the listener lost its connection"),
                () -> assertThat(catchup.hasParkedReplay(ID)).as("the replay is held").isTrue(),
                () -> assertThat(catchup.isCatchingUp(ID)).as("catching up").isFalse()
        );
    }

    @Test
    void runs_the_held_replay_again_once_the_subscription_is_resumed_when_the_listener_threw_a_checked_exception_without_declaring_it_as_a_resume_at_a_checkpoint_put_it_back() throws Exception {
        // Given
        resumeAtACheckpointWhileTheListenerThrowsAsTheHeldReplayIsPutBack(new IOException("the listener lost its connection"), new IOException("the live model lost its connection"));
        int deliveriesBeforeTheResume = deliveries.get();
        catchupStartedThrows.set(null);

        // When
        boolean started = catchup.resumeSubscription(ID).waitUntilStarted(Duration.ofSeconds(15));

        // Then
        assertAll(
                () -> assertThat(started).as("the resumed replay handed over").isTrue(),
                () -> assertThat(deliveries.get()).as("deliveries").isGreaterThan(deliveriesBeforeTheResume),
                () -> assertThat(catchup.hasParkedReplay(ID)).as("the replay is held").isFalse(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue()
        );
    }

    @Test
    void does_not_run_the_action_for_a_resume_while_the_replay_a_cancel_ended_is_still_in_it_when_the_live_model_cancel_throws_a_checked_exception_without_declaring_it() throws Exception {
        // Given a takeover replay that is inside the action, and a live model that fails the cancel while it still holds the subscription paused
        IOException cancelFailure = new IOException("the live model lost its connection");
        LiveStartLosingSubscriptionModel takeoverModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false,
                () -> write(nameDefined(LocalDateTime.now(), "writtenInTheGap")))
                .beforeTheNextCancel(() -> {
                    throw sneaky(cancelFailure);
                });
        create(takeoverModel, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(new InMemoryCheckpointStorage()).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CountDownLatch inTheTakeoverReplay = new CountDownLatch(1);
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
            mostInAction.accumulateAndGet(inAction.incrementAndGet(), Math::max);
            try {
                if (deliveries.incrementAndGet() == history.size() + 1) {
                    inTheTakeoverReplay.countDown();
                    awaitUnchecked(deliveryMayReturn);
                }
            } finally {
                inAction.decrementAndGet();
            }
        });
        assertThat(inTheTakeoverReplay.await(10, TimeUnit.SECONDS)).as("whether the takeover replay is in the action").isTrue();

        // When the cancel fails, the model is stopped, and the subscription is resumed
        Throwable thrown = catchThrowable(() -> catchup.cancelSubscription(ID));
        catchup.stop();
        catchup.resumeSubscription(ID);

        // Then the new replay stays out of the action while the cancelled replay is in it
        assertThat(thrown).as("thrown by the cancel").isSameAs(cancelFailure);
        await().during(1, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(mostInAction.get()).as("the most calls of the action in flight at once while the cancelled replay was in it").isEqualTo(1));
        // And it delivers once the cancelled replay has left the action
        deliveryMayReturn.countDown();
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(deliveries.get()).as("deliveries").isGreaterThan(history.size() + 1));
        assertThat(mostInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
    }

    // A takeover replay is inside the action when a resume at a checkpoint holds it, the live model then refuses the
    // resume, and the listener throws as the held replay starts again
    private Throwable resumeAtACheckpointWhileTheListenerThrowsAsTheHeldReplayIsPutBack(Throwable listenerFailure, Throwable liveFailure) throws Exception {
        LiveStartLosingSubscriptionModel takeoverModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false,
                () -> write(nameDefined(LocalDateTime.now(), "writtenInTheGap")));
        create(takeoverModel);
        CountDownLatch inTheTakeoverReplay = new CountDownLatch(1);
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
            if (deliveries.incrementAndGet() == history.size() + 1) {
                inTheTakeoverReplay.countDown();
                awaitUnchecked(deliveryMayReturn);
            }
        });
        assertThat(inTheTakeoverReplay.await(10, TimeUnit.SECONDS)).as("whether the takeover replay is in the action").isTrue();
        takeoverModel.beforeTheNextResumeAt(() -> {
            throw sneaky(liveFailure);
        });
        CompletableFuture<Throwable> resuming = CompletableFuture.supplyAsync(
                () -> catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(TOKEN))), runnable -> Thread.ofPlatform().daemon().start(runnable));
        await().untilAsserted(() -> assertThat(catchup.isCatchingUp(ID)).as("whether the replay is held by the resume").isFalse());
        catchupStartedThrows.set(listenerFailure);
        deliveryMayReturn.countDown();
        return resuming.get(20, TimeUnit.SECONDS);
    }

    private List<String> listenerFailuresLogged() {
        return logged.events.stream()
                .filter(event -> event.getLevel() == Level.ERROR && event.getFormattedMessage().contains("The catch-up listener for subscription " + ID + " failed"))
                .map(event -> event.getThrowableProxy().getClassName() + ": " + event.getThrowableProxy().getMessage())
                .toList();
    }

    private static final class LoggedEvents extends AppenderBase<ILoggingEvent> {
        private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            events.add(event);
        }
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
