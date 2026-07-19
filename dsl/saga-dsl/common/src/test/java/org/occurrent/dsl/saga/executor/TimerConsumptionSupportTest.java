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

package org.occurrent.dsl.saga.executor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.SagaTimeout;
import org.occurrent.dsl.saga.executor.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.executor.SagaExecutionSupport.Outcome;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("A fired timer is one-shot")
class TimerConsumptionSupportTest {

    sealed interface Ev {
    }

    record Started(String id) implements Ev {
    }

    sealed interface Cmd {
    }

    record Ping(String id) implements Cmd {
    }

    private static final Instant NOW = Instant.parse("2026-07-19T12:00:00Z");

    @Test
    @DisplayName("is consumed even when its reaction neither cancels it nor completes the saga, so it does not re-fire")
    void firedTimerIsConsumed() {
        Saga<Ev, String, Cmd> saga = Saga.<Ev, String, Cmd>builder("new")
                .correlate(Started.class, Started::id)
                .startsOn(Started.class)
                .evolve(Started.class, (state, event) -> "active")
                .react(Started.class, (state, event) -> List.of(SagaEffect.startTimeout("t", Duration.ofMinutes(5))))
                .evolveOnTimeout("t", (state, timeout) -> "active")   // stays active: NOT terminal
                .reactOnTimeout("t", (state, timeout) -> List.of(SagaEffect.issue(new Ping(timeout.sagaId()))))
                .build();

        Outcome<String, Cmd> start = SagaExecutionSupport.process(saga, "s1", null, SagaInput.event(new Started("s1")), new EventMeta("s1", 1L, null), NOW);
        assertThat(start.envelope().timers()).extracting("name").containsExactly("t");

        Outcome<String, Cmd> fired = SagaExecutionSupport.process(saga, "s1", start.envelope(), SagaInput.timeout(new SagaTimeout("s1", "t")), EventMeta.NONE, NOW.plusSeconds(1));

        assertThat(fired.processed()).isTrue();
        assertThat(fired.commands()).containsExactly(new Ping("s1"));
        assertThat(fired.envelope().timers()).as("the fired timer must be consumed so it does not re-fire every poll").isEmpty();
        assertThat(fired.envelope().isCompleted()).isFalse();
    }

    @Test
    @DisplayName("but a reaction that explicitly re-arms it keeps the timer, with a fresh firing time")
    void reArmedTimerIsKept() {
        Saga<Ev, String, Cmd> saga = Saga.<Ev, String, Cmd>builder("new")
                .correlate(Started.class, Started::id)
                .startsOn(Started.class)
                .evolve(Started.class, (state, event) -> "active")
                .react(Started.class, (state, event) -> List.of(SagaEffect.startTimeout("t", Duration.ofMinutes(5))))
                .evolveOnTimeout("t", (state, timeout) -> "active")
                .reactOnTimeout("t", (state, timeout) -> List.of(SagaEffect.issue(new Ping(timeout.sagaId())), SagaEffect.startTimeout("t", Duration.ofMinutes(5))))
                .build();

        Outcome<String, Cmd> start = SagaExecutionSupport.process(saga, "s1", null, SagaInput.event(new Started("s1")), new EventMeta("s1", 1L, null), NOW);
        Outcome<String, Cmd> fired = SagaExecutionSupport.process(saga, "s1", start.envelope(), SagaInput.timeout(new SagaTimeout("s1", "t")), EventMeta.NONE, NOW.plusSeconds(60));

        assertThat(fired.envelope().timers()).extracting("name").containsExactly("t");
        assertThat(fired.envelope().timers().getFirst().firesAtEpochMilli()).isEqualTo(NOW.plusSeconds(60).plus(Duration.ofMinutes(5)).toEpochMilli());
    }
}
