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

package org.occurrent.dsl.saga.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The timer poller goes on with a release that stopped, and leaves one that is still running to the call running it.
 * An instance written within the last {@code timerPollInterval} has a release running, so a poll in that time must not
 * replay the record that release is on. Driven with a test clock and a latch in the command dispatcher, so each
 * interleaving is deterministic.
 */
@DisplayName("The timer poller and a quarantined saga instance that is releasing")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleasePollerOverlapTest {

    private static final String ORDER = "o1";
    private static final Duration BUDGET = Duration.ofMinutes(1);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(15);
    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET)
            .withTimerPollInterval(POLL_INTERVAL)
            .withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);

    sealed interface OrderEvent permits OrderPlaced, ItemAdded {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record ItemAdded(String eventId, String orderId, String item) implements OrderEvent {
    }

    record Pack(String orderId, String item) {
    }

    record Order(String orderId, List<String> items) {
        Order with(String item) {
            List<String> added = new ArrayList<>(items);
            added.add(item);
            return new Order(orderId, List.copyOf(added));
        }
    }

    private volatile boolean poisonFails = false;
    private volatile boolean aFails = false;
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final List<Object> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private final InMemorySagaStateStore<Order> store = new InMemorySagaStateStore<>();
    private final AtomicBoolean blockOnA = new AtomicBoolean(false);
    private final CountDownLatch dispatchingA = new CountDownLatch(1);
    private final CountDownLatch mayFinishA = new CountDownLatch(1);

    private Saga<OrderEvent, Order, Object> saga() {
        return Saga.<OrderEvent, Order, Object>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(e.orderId(), List.of()))
                .evolve(ItemAdded.class, (state, e) -> state.with(e.item()))
                .react(ItemAdded.class, (state, e) -> {
                    if (poisonFails && e.item().equals("poison") || aFails && e.item().equals("a")) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.orderId(), e.item())));
                })
                .build();
    }

    private SagaExecution<OrderEvent, Order, Object> execution() {
        return new SagaExecution<>("orders", saga(), store, command -> {
            dispatched.add(command);
            if (command.equals(new Pack(ORDER, "a")) && blockOnA.compareAndSet(true, false)) {
                dispatchingA.countDown();
                await(mayFinishA);
            }
        }, converter, CONFIG, event -> true, now::get, eventStore);
    }

    private CloudEvent append(OrderEvent event) {
        eventStore.write(ORDER, converter.toCloudEvents(List.of(event)));
        return eventStore.read(ORDER).eventList().getLast();
    }

    private String nextEventId() {
        return "e" + eventIds.incrementAndGet();
    }

    private SagaEnvelope<Order> instance() {
        return store.find(ORDER).orElseThrow();
    }

    // Quarantined on "poison", with "a" and "b" skipped after it, the way a saga that has not been released looks
    private SagaExecution<OrderEvent, Order, Object> quarantinedOnPoisonWithTwoSkipped() {
        SagaExecution<OrderEvent, Order, Object> execution = execution();
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(new ItemAdded(nextEventId(), ORDER, "poison"));
        catchThrowable(() -> execution.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(poison));
        assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED);
        poisonFails = false;
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "a")));
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "b")));
        return execution;
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("latch not released in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Test
    void a_poll_while_a_release_is_dispatching_a_record_leaves_the_record_to_the_release_so_each_record_is_dispatched_once() throws Exception {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped();
        dispatched.clear();
        blockOnA.set(true);

        CompletableFuture<ReleaseResult> release = CompletableFuture.supplyAsync(() -> execution.release(ORDER));
        await(dispatchingA);
        List<Object> beforeThePoll = List.copyOf(dispatched);
        execution.pollTimers();
        List<Object> afterThePoll = List.copyOf(dispatched);
        mayFinishA.countDown();
        ReleaseResult result = release.get(10, TimeUnit.SECONDS);

        assertAll(
                () -> assertThat(beforeThePoll).as("control: the release is in the middle of 'a', after 'poison'")
                        .containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a")),
                () -> assertThat(afterThePoll).as("the poll dispatched nothing").isEqualTo(beforeThePoll),
                () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(dispatched).as("each record once, overall")
                        .containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b")),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(instance().state().items()).containsExactly("poison", "a", "b")
        );
    }

    @Test
    void a_release_that_stopped_on_a_failed_input_is_gone_on_with_by_the_first_poll_a_poll_interval_after_its_last_write_and_not_before() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped();
        aFails = true;
        ReleaseResult stopped = execution.release(ORDER);
        aFails = false;
        Instant lastWrite = instance().updatedAt();

        now.set(lastWrite);
        execution.pollTimers();
        List<String> afterAPollAtTheLastWrite = instance().state().items();
        now.set(lastWrite.plus(POLL_INTERVAL).minusMillis(1));
        execution.pollTimers();
        List<String> afterAPollJustInsideTheInterval = instance().state().items();
        boolean stillReleasing = instance().isReleasing();
        now.set(lastWrite.plus(POLL_INTERVAL).plusMillis(1));
        execution.pollTimers();

        assertAll(
                () -> assertThat(stopped).as("control: the release stopped on 'a'").isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(afterAPollAtTheLastWrite).as("a poll when the release wrote last").containsExactly("poison"),
                () -> assertThat(afterAPollJustInsideTheInterval).as("a poll just inside the interval").containsExactly("poison"),
                () -> assertThat(stillReleasing).isTrue(),
                () -> assertThat(instance().status()).as("a poll after the interval").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(instance().state().items()).containsExactly("poison", "a", "b"),
                () -> assertThat(dispatched).filteredOn(c -> c.equals(new Pack(ORDER, "a"))).as("'a' is dispatched once, by the poll").hasSize(1)
        );
    }

    @Test
    void a_release_that_stopped_on_a_failed_input_is_gone_on_with_by_a_poll_exactly_a_poll_interval_after_its_last_write() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped();
        aFails = true;
        execution.release(ORDER);
        aFails = false;
        Instant lastWrite = instance().updatedAt();

        now.set(lastWrite.plus(POLL_INTERVAL));
        execution.pollTimers();

        assertAll(
                () -> assertThat(instance().status()).as("a poll exactly one interval after the last write").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(instance().state().items()).containsExactly("poison", "a", "b")
        );
    }
}
