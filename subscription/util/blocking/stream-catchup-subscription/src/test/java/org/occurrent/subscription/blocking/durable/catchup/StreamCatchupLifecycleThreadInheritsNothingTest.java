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

import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The lifecycle thread runs the calls that actions handed over. It is created from the thread of a replay, so it must
 * not inherit that thread's inheritable thread-locals, which would pin the caller's context to a thread that outlives it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupLifecycleThreadInheritsNothingTest {

    private static final InheritableThreadLocal<String> CALLERS_CONTEXT = new InheritableThreadLocal<>();

    private InMemorySubscriptionModel inMemory;
    private HookedSubscriptionModel wrapped;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_model_whose_wrapped_stop_reads_an_inheritable_thread_local() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new HookedSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new StreamCatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void clear_the_context_and_shut_down() {
        try {
            CALLERS_CONTEXT.remove();
        } finally {
            model.shutdown();
            inMemory.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_stop_that_an_action_handed_over_runs_on_a_thread_that_did_not_inherit_the_context_of_the_thread_that_subscribed() {
        // Given
        AtomicReference<String> contextOfTheThreadThatRanTheWrappedStop = new AtomicReference<>("not yet run");
        wrapped.beforeStop = () -> contextOfTheThreadThatRanTheWrappedStop.set(String.valueOf(CALLERS_CONTEXT.get()));
        eventStore.write(UUID.randomUUID().toString(), List.of(CloudEventBuilder.v1().withId("1").withSource(URI.create("urn:test")).withType("test").withTime(OffsetDateTime.now()).build()));
        CALLERS_CONTEXT.set("the context of the thread that subscribes");

        // When
        model.subscribe("subscription", StartAtTime.beginningOfTime(), event -> model.stop());

        // Then
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(contextOfTheThreadThatRanTheWrappedStop.get()).as("the inheritable thread-local as the thread that ran the handed stop saw it").isEqualTo("null"));
    }
}
