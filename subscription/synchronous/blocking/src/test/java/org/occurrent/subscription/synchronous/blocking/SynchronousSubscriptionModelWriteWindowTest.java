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

package org.occurrent.subscription.synchronous.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.WriteRefusedException;
import org.occurrent.application.service.blocking.ExecuteOptions;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher.WriteCheck;
import org.occurrent.application.service.blocking.dcb.DcbExecuteOptions;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StreamSubscriptionFilter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * The window a write holds open on the model, which a pause or a stop waits for. The services must close it after a
 * failed or refused write too. The model's own handling of a pause or a stop that waits for a window is driven
 * directly.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SynchronousSubscriptionModelWriteWindowTest {

    private static final Tag TAG = Tag.parse("name:1");
    private static final long WAIT_SECONDS = 10;

    private SynchronousSubscriptionModel model;
    private List<String> handled;

    @BeforeEach
    void initialize() {
        model = new SynchronousSubscriptionModel();
        handled = new CopyOnWriteArrayList<>();
    }

    // The window after a write that fails or is refused

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_generic_write_whose_store_failed() throws Exception {
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        var store = failingStore();
        var applicationService = GenericApplicationService.builder((EventStore) store, new TestEventConverter()).synchronousSubscriptions(model).build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).as("what the write threw").isInstanceOf(IllegalStateException.class).hasMessage("store failed");
        assertThatAPauseAndAStopApply("a write whose store failed");
    }

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_dcb_write_whose_store_failed() throws Exception {
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        var store = failingStore();
        var applicationService = GenericDcbApplicationService.builder((DcbEventStore) store, new TestEventConverter())
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(model)
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).as("what the write threw").isInstanceOf(IllegalStateException.class).hasMessage("store failed");
        assertThatAPauseAndAStopApply("a write whose store failed");
    }

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_generic_write_whose_handler_failed() throws Exception {
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> {
            throw new IllegalStateException("handler failed");
        });
        var applicationService = GenericApplicationService.builder((EventStore) new InMemoryEventStore(), new TestEventConverter()).synchronousSubscriptions(model).build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).as("what the write threw").hasStackTraceContaining("handler failed");
        assertThatAPauseAndAStopApply("a write whose handler failed");
    }

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_dcb_write_whose_handler_failed() throws Exception {
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> {
            throw new IllegalStateException("handler failed");
        });
        var applicationService = GenericDcbApplicationService.builder((DcbEventStore) new InMemoryEventStore(), new TestEventConverter())
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(model)
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).as("what the write threw").hasStackTraceContaining("handler failed");
        assertThatAPauseAndAStopApply("a write whose handler failed");
    }

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_generic_write_that_a_paused_subscription_refused() throws Exception {
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameWasChanged")), event -> handled.add(event.getType()));
        model.subscribe("paused", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        model.pauseSubscription("paused");
        var applicationService = GenericApplicationService.builder((EventStore) new InMemoryEventStore(), new TestEventConverter()).synchronousSubscriptions(model).build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).as("what the write threw").isInstanceOf(WriteRefusedException.class).hasMessageContaining("paused");
        assertThatAPauseAndAStopApply("a write that a paused subscription refused");
    }

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_dcb_write_that_a_paused_subscription_refused() throws Exception {
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameWasChanged")), event -> handled.add(event.getType()));
        model.subscribe("paused", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        model.pauseSubscription("paused");
        var applicationService = GenericDcbApplicationService.builder((DcbEventStore) new InMemoryEventStore(), new TestEventConverter())
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(model)
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).as("what the write threw").isInstanceOf(WriteRefusedException.class).hasMessageContaining("paused");
        assertThatAPauseAndAStopApply("a write that a paused subscription refused");
    }

    // A side effect that pauses or stops the model on another thread and waits for that. The write's window is closed
    // before the side effect runs, or the pause or the stop waits for a write that waits for the side effect.

    @Test
    void a_generic_write_whose_side_effect_stops_the_model_on_another_thread_and_waits_completes_with_its_events_stored() throws Exception {
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        var store = new InMemoryEventStore();
        var applicationService = GenericApplicationService.builder((EventStore) store, new TestEventConverter()).synchronousSubscriptions(model).build();
        String streamId = UUID.randomUUID().toString();
        AtomicBoolean sideEffectRan = new AtomicBoolean();

        var written = executeWithinTheTimeout(() -> applicationService.execute(streamId,
                ExecuteOptions.<TestEvent>options().sideEffect(newEvents -> {
                    sideEffectRan.set(true);
                    runOnAnotherThreadAndWait(model::stop);
                }), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(written).as("what execute returned").isNotNull();
        assertThat(store.read(streamId).events().map(CloudEvent::getType).toList()).as("the events stored").containsExactly("NameDefined");
        assertThat(handled).as("the events the handler got before the side effect").containsExactly("NameDefined");
        assertThat(sideEffectRan).as("the side effect ran").isTrue();
        assertThat(model.isRunning()).as("running once the side effect stopped the model").isFalse();
    }

    @Test
    void a_generic_write_whose_side_effect_pauses_a_subscription_on_another_thread_and_waits_completes_with_its_events_stored() throws Exception {
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        var store = new InMemoryEventStore();
        var applicationService = GenericApplicationService.builder((EventStore) store, new TestEventConverter()).synchronousSubscriptions(model).build();
        String streamId = UUID.randomUUID().toString();
        AtomicBoolean sideEffectRan = new AtomicBoolean();

        var written = executeWithinTheTimeout(() -> applicationService.execute(streamId,
                ExecuteOptions.<TestEvent>options().sideEffect(newEvents -> {
                    sideEffectRan.set(true);
                    runOnAnotherThreadAndWait(() -> model.pauseSubscription("p"));
                }), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(written).as("what execute returned").isNotNull();
        assertThat(store.read(streamId).events().map(CloudEvent::getType).toList()).as("the events stored").containsExactly("NameDefined");
        assertThat(handled).as("the events the handler got before the side effect").containsExactly("NameDefined");
        assertThat(sideEffectRan).as("the side effect ran").isTrue();
        assertThat(model.isPaused("p")).as("paused once the side effect paused it").isTrue();
    }

    @Test
    void a_dcb_write_whose_side_effect_stops_the_model_on_another_thread_and_waits_completes_with_its_events_stored() throws Exception {
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        var store = new InMemoryEventStore();
        var applicationService = GenericDcbApplicationService.builder((DcbEventStore) store, new TestEventConverter())
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(model)
                .build();
        AtomicBoolean sideEffectRan = new AtomicBoolean();

        var written = executeWithinTheTimeout(() -> applicationService.execute(tags(TAG),
                DcbExecuteOptions.<TestEvent>options().sideEffect(newEvents -> {
                    sideEffectRan.set(true);
                    runOnAnotherThreadAndWait(model::stop);
                }), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(written).as("what execute returned").isPresent();
        assertThat(store.read(tags(TAG)).events().stream().map(CloudEvent::getType).toList()).as("the events stored").containsExactly("NameDefined");
        assertThat(handled).as("the events the handler got before the side effect").containsExactly("NameDefined");
        assertThat(sideEffectRan).as("the side effect ran").isTrue();
        assertThat(model.isRunning()).as("running once the side effect stopped the model").isFalse();
    }

    @Test
    void a_dcb_write_whose_side_effect_pauses_a_subscription_on_another_thread_and_waits_completes_with_its_events_stored() throws Exception {
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        var store = new InMemoryEventStore();
        var applicationService = GenericDcbApplicationService.builder((DcbEventStore) store, new TestEventConverter())
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(model)
                .build();
        AtomicBoolean sideEffectRan = new AtomicBoolean();

        var written = executeWithinTheTimeout(() -> applicationService.execute(tags(TAG),
                DcbExecuteOptions.<TestEvent>options().sideEffect(newEvents -> {
                    sideEffectRan.set(true);
                    runOnAnotherThreadAndWait(() -> model.pauseSubscription("p"));
                }), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(written).as("what execute returned").isPresent();
        assertThat(store.read(tags(TAG)).events().stream().map(CloudEvent::getType).toList()).as("the events stored").containsExactly("NameDefined");
        assertThat(handled).as("the events the handler got before the side effect").containsExactly("NameDefined");
        assertThat(sideEffectRan).as("the side effect ran").isTrue();
        assertThat(model.isPaused("p")).as("paused once the side effect paused it").isTrue();
    }

    // The call runs on a thread of its own, so a side effect that never returns fails the test, with the thread
    // interrupted, rather than hanging it. The thread is a daemon one.
    private static <T> T executeWithinTheTimeout(Callable<T> execute) throws Exception {
        ExecutorService caller = Executors.newSingleThreadExecutor(action -> {
            Thread thread = new Thread(action, "execute-caller");
            thread.setDaemon(true);
            return thread;
        });
        try {
            Future<T> future = caller.submit(execute);
            try {
                return future.get(WAIT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                return fail("execute didn't complete within " + WAIT_SECONDS + " seconds, since its side effect waited for a pause or a stop that waited for the write");
            }
        } finally {
            caller.shutdownNow();
        }
    }

    private static void runOnAnotherThreadAndWait(Runnable action) {
        Thread thread = start(action);
        try {
            thread.join();
        } catch (InterruptedException e) {
            thread.interrupt();
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private void assertThatAPauseAndAStopApply(String after) throws InterruptedException {
        Thread pauser = start(() -> model.pauseSubscription("p"));
        assertThat(joinedOrInterrupted(pauser)).as("the pause from another thread returned after " + after).isTrue();
        assertThat(model.isPaused("p")).as("paused once the pause returned after " + after).isTrue();
        Thread stopper = start(model::stop);
        assertThat(joinedOrInterrupted(stopper)).as("the stop from another thread returned after " + after).isTrue();
        assertThat(model.isRunning()).as("running once the stop returned after " + after).isFalse();
    }

    // The pause and stop branches that wait for a window, driven by windows the test opens and closes itself. The
    // model's beforeWrite opens a window on the calling thread, so a pause or a stop made on that thread doesn't wait.

    @Test
    void a_pause_applies_once_the_windows_opened_before_it_close_even_when_a_window_opened_after_it_is_still_open() {
        model.subscribe("p", event -> {
        });
        WriteCheck openedBefore = model.beforeWrite();
        model.pauseSubscription("p");
        WriteCheck openedAfter = model.beforeWrite();
        assertThat(model.isPaused("p")).as("paused while a window opened before the pause is open").isFalse();

        openedBefore.close();

        assertThat(model.isPaused("p")).as("paused once the window opened before the pause closed, with a later one still open").isTrue();
        openedAfter.close();
    }

    @Test
    void a_resume_while_a_pause_waits_calls_the_pause_off() {
        model.subscribe("p", event -> {
        });
        WriteCheck open = model.beforeWrite();
        model.pauseSubscription("p");

        Throwable thrown = catchThrowable(() -> model.resumeSubscription("p"));
        open.close();

        assertThat(thrown).as("what resuming the subscription threw").isNull();
        assertThat(model.isPaused("p")).as("paused after the window closed").isFalse();
        assertThat(model.isRunning("p")).as("running after the window closed").isTrue();
    }

    @Test
    void a_resume_while_a_stop_waits_calls_the_stop_off_and_pauses_the_other_subscriptions() {
        model.subscribe("p", event -> {
        });
        model.subscribe("other", event -> {
        });
        WriteCheck open = model.beforeWrite();
        model.stop();

        Throwable thrown = catchThrowable(() -> model.resumeSubscription("p"));
        open.close();

        assertThat(thrown).as("what resuming the subscription threw").isNull();
        assertThat(model.isRunning("p")).as("the resumed subscription runs after the window closed").isTrue();
        assertThat(model.isPaused("other")).as("the other subscription is paused after the window closed").isTrue();
        assertThat(model.isRunning()).as("the model runs after the window closed").isTrue();
    }

    @Test
    void a_start_without_resuming_while_a_stop_waits_turns_the_stop_into_pauses() {
        model.subscribe("p", event -> {
        });
        model.subscribe("other", event -> {
        });
        WriteCheck open = model.beforeWrite();
        model.stop();

        model.start(false);
        open.close();

        assertThat(model.isRunning()).as("the model runs after the window closed").isTrue();
        assertThat(model.isPaused("p")).as("p is paused after the window closed").isTrue();
        assertThat(model.isPaused("other")).as("other is paused after the window closed").isTrue();
    }

    // Delegates to an in-memory store except that every write throws
    private static Object failingStore() {
        InMemoryEventStore delegate = new InMemoryEventStore();
        return Proxy.newProxyInstance(SynchronousSubscriptionModelWriteWindowTest.class.getClassLoader(), new Class<?>[]{EventStore.class, DcbEventStore.class}, (proxy, method, args) -> {
            if (method.getName().equals("write") || method.getName().equals("append")) {
                throw new IllegalStateException("store failed");
            }
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }

    private static Thread start(Runnable action) {
        Thread thread = new Thread(action);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static boolean joinedOrInterrupted(Thread thread) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        if (thread.isAlive()) {
            thread.interrupt();
            thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            return false;
        }
        return true;
    }

    private record TestEvent(String type) {
    }

    private static final class TestEventConverter implements CloudEventConverter<TestEvent> {
        @Override
        public CloudEvent toCloudEvent(TestEvent domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(URI.create("urn:occurrent:test"))
                    .withType(domainEvent.type())
                    .build();
        }

        @Override
        public TestEvent toDomainEvent(CloudEvent cloudEvent) {
            return new TestEvent(cloudEvent.getType());
        }

        @Override
        public String getCloudEventType(Class<? extends TestEvent> type) {
            throw new UnsupportedOperationException();
        }
    }
}
