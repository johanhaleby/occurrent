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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Issue #1003. A poll fires at most {@code timerBatchLimit} instances. When more instances than that keep failing on
 * their timer and the store keeps handing the same ones back first, an instance behind them never gets a turn.
 * The store here is the in-memory one, so the order {@code findWithDueTimers} answers in and the failed-attempt marker
 * the executor writes are both under test, together.
 */
@DisplayName("A saga whose timer reactions keep failing")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaTimerFairnessTest {

    private static final String TIMER = "expiry";
    private static final int BATCH_LIMIT = 3;
    private static final int FAILING_INSTANCES = 7;
    private static final long FIRST_POLL = 6_000;
    private static final long POLL_STEP = 1_000;

    private final CloudEventConverter<OrderEvent> converter =
            new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:occurrent:saga-timer-fairness-test")).build();

    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.ofEpochMilli(FIRST_POLL));
    private final SagaStateStore<String> store = SagaStateStore.inMemory();
    private final List<CancelOrder> dispatched = new ArrayList<>();
    private final Map<String, Integer> attempts = new HashMap<>();

    @Test
    void still_lets_a_healthy_instance_that_became_due_after_all_of_them_fire_within_the_polls_the_instances_ahead_of_it_need() {
        seedFailing(FAILING_INSTANCES);
        seed("healthy", 5_000);
        SagaExecution<OrderEvent, String, CancelOrder> execution = execution();
        int ahead = FAILING_INSTANCES;

        int polls = pollsAllowed(ahead);
        pollRepeatedly(execution, polls);

        assertThat(dispatched).as("the healthy timer fired within %d polls of %d failing instances ahead of it at a batch limit of %d", polls, ahead, BATCH_LIMIT)
                .containsExactly(new CancelOrder("healthy"));
    }

    @Test
    void still_lets_a_healthy_instance_that_became_due_between_two_polls_fire_after_the_first_failures_were_marked() {
        seedFailing(FAILING_INSTANCES);
        seed("healthy", 2_500);
        SagaExecution<OrderEvent, String, CancelOrder> execution = execution();
        now.set(Instant.ofEpochMilli(2_000));

        execution.pollTimers();

        assertThat(dispatched).as("the healthy timer is not due at the first poll").isEmpty();

        now.set(Instant.ofEpochMilli(3_000));
        int ahead = FAILING_INSTANCES;
        int polls = pollsAllowed(ahead);
        pollRepeatedly(execution, polls);

        assertThat(dispatched).as("the healthy timer fired within %d polls of becoming due, with %d failing instances ahead of it at a batch limit of %d", polls, ahead, BATCH_LIMIT)
                .containsExactly(new CancelOrder("healthy"));
    }

    @Test
    void keeps_trying_every_failing_instance_rather_than_dropping_the_ones_it_has_already_tried() {
        seedFailing(FAILING_INSTANCES);
        seed("healthy", 5_000);
        SagaExecution<OrderEvent, String, CancelOrder> execution = execution();

        pollRepeatedly(execution, 3 * pollsAllowed(FAILING_INSTANCES));

        assertAll(
                IntStream.range(0, FAILING_INSTANCES)
                        .mapToObj(i -> (Executable) () ->
                                assertThat(attempts.getOrDefault("failing-" + i, 0)).as("attempts at failing-" + i).isGreaterThanOrEqualTo(2))
        );
    }

    private static int pollsAllowed(int ahead) {
        return (int) Math.ceil(ahead / (double) BATCH_LIMIT) + 1;
    }

    private void pollRepeatedly(SagaExecution<OrderEvent, String, CancelOrder> execution, int polls) {
        for (int i = 0; i < polls; i++) {
            execution.pollTimers();
            now.updateAndGet(current -> current.plusMillis(POLL_STEP));
        }
    }

    private void seedFailing(int count) {
        // Distinct firing times, so the order the store must answer in is fixed by when each became due
        IntStream.range(0, count).forEach(i -> seed("failing-" + i, 1_000 + i));
    }

    private void seed(String sagaId, long firesAt) {
        store.compareAndSave(sagaId, new SagaEnvelope<>(sagaId, "waiting", SagaStatus.ACTIVE, 1, List.of(new TimerEntry(TIMER, firesAt)),
                Map.of(), null, Instant.ofEpochMilli(1), Instant.ofEpochMilli(1), null, null), 0);
    }

    private SagaExecution<OrderEvent, String, CancelOrder> execution() {
        Saga<OrderEvent, String, CancelOrder> saga = Saga.<OrderEvent, String, CancelOrder>builder("waiting")
                .correlate(OrderPlaced.class, OrderPlaced::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, event) -> "waiting")
                .evolveOnTimeout(TIMER, (state, timeout) -> "cancelled")
                .reactOnTimeout(TIMER, (state, timeout) -> {
                    if (timeout.sagaId().startsWith("failing-")) {
                        attempts.merge(timeout.sagaId(), 1, Integer::sum);
                        throw new IllegalStateException("this instance always fails");
                    }
                    return List.of(SagaEffect.issue(new CancelOrder(timeout.sagaId())));
                })
                .build();
        CommandDispatcher<CancelOrder> dispatcher = dispatched::add;
        SagaRunnerConfig config = new SagaRunnerConfig(Duration.ofSeconds(15), BATCH_LIMIT, 50);
        return new SagaExecution<>("saga-timer-fairness", saga, store, dispatcher, converter, config, event -> true, now::get);
    }

    sealed interface OrderEvent permits OrderPlaced {
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record CancelOrder(String orderId) {
    }
}
