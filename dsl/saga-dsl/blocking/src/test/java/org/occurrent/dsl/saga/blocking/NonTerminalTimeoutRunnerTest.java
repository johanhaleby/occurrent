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
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayName("A non-terminal timeout whose reaction does not re-arm it")
class NonTerminalTimeoutRunnerTest {

    sealed interface Ev {
        String eventId();

        String id();
    }

    record Started(String eventId, String id) implements Ev {
    }

    sealed interface Cmd {
    }

    record Ping(String id) implements Cmd {
    }

    @Test
    @DisplayName("fires exactly once and does not re-fire every poll")
    void firesExactlyOnce() throws Exception {
        InMemorySubscriptionModel subscriptionModel = new InMemorySubscriptionModel();
        InMemoryEventStore eventStore = new InMemoryEventStore(subscriptionModel);
        CloudEventConverter<Ev> converter = new JacksonCloudEventConverter.Builder<Ev>(new ObjectMapper(), URI.create("urn:test")).idMapper(Ev::eventId).build();

        Saga<Ev, String, Cmd> saga = Saga.<Ev, String, Cmd>builder("new")
                .correlate(Started.class, Started::id)
                .startsOn(Started.class)
                .evolve(Started.class, (state, event) -> "active")
                .react(Started.class, (state, event) -> List.of(SagaEffect.startTimeout("t", Duration.ofMillis(100))))
                .evolveOnTimeout("t", (state, timeout) -> "active")   // stays active, never terminal, never re-arms
                .reactOnTimeout("t", (state, timeout) -> List.of(SagaEffect.issue(new Ping(timeout.sagaId()))))
                .build();

        List<Cmd> issued = new CopyOnWriteArrayList<>();
        SagaRunnerConfig config = SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofMillis(50));
        SagaSubscription subscription = SagaRunner.<Ev, Cmd>agnostic(subscriptionModel, converter)
                .run("non-terminal-timeout", saga, SagaStateStore.inMemory(), issued::add, null, config);
        try {
            eventStore.write("s1", converter.toCloudEvents(List.of(new Started(UUID.randomUUID().toString(), "s1"))));

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertThat(issued).containsExactly(new Ping("s1")));
            // Let several poll cycles pass; a consumed timer must not re-fire.
            await().pollDelay(Duration.ofMillis(400)).atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(issued).containsExactly(new Ping("s1")));
        } finally {
            subscription.close();
        }
    }
}
