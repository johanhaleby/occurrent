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

package org.occurrent.dsl.saga;

import java.time.Duration;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * What a saga wants to happen once an input has been applied, expressed as data. A {@link Saga#react(Object, SagaInput)}
 * returns a list of these, and an executor interprets them in order. The saga itself never performs an effect, which is what
 * keeps it a pure, unit-testable function.
 * <p>
 * The hierarchy is a two-way partition: an effect either issues a command ({@link IssueCommand}) or touches a timer
 * ({@link TimerEffect}). {@link Saga.Step#issuedCommands()} and {@link Saga.Step#timerEffects()} read the two halves,
 * and their return types say so.
 *
 * @param <C> the command type the saga issues
 */
public sealed interface SagaEffect<C> permits SagaEffect.IssueCommand, SagaEffect.TimerEffect {

    /**
     * Issue {@code command}. An executor hands it to a command dispatcher, typically a lambda over an
     * {@code ApplicationService} (with or without a decider). The effect carries no routing information on purpose: a
     * command already carries the id of whatever it targets.
     */
    record IssueCommand<C>(C command) implements SagaEffect<C> {
        public IssueCommand {
            requireNonNull(command, "command cannot be null");
        }
    }

    /**
     * An effect that starts or cancels a timer rather than issuing a command, the other half of the partition from
     * {@link IssueCommand}. It carries no {@code C}, so {@link Saga.Step#timerEffects()} can return these without
     * statically admitting a command, and a caller matching on one has three cases rather than four.
     * <p>
     * The {@code permits} clause is explicit so a new effect has to be classified here, as a timer effect or as
     * something else, rather than joining the union by merely being declared in this file.
     */
    sealed interface TimerEffect<C> extends SagaEffect<C> permits StartTimeout, StartTimeoutAt, CancelTimeout {
    }

    /**
     * Start (or, if one with the same {@code timerName} already runs, restart) a timer that fires {@code after} the given
     * duration. The duration is relative and resolved against the clock by the executor when it stores the timer, not
     * here: building an absolute time inside the pure {@code react} would read the clock and make the same reaction
     * produce different effect values on each call.
     */
    record StartTimeout<C>(TimerName timerName, Duration after) implements TimerEffect<C> {
        public StartTimeout {
            requireNonNull(timerName, "timerName cannot be null");
            requireNonNull(after, "after cannot be null");
        }
    }

    /**
     * Start (or restart) a timer that fires at the absolute instant {@code at}. Use this for a timer whose firing time is
     * derived from data (for example an auction's end time). Compute {@code at} from event data, never from the current
     * clock inside {@code react}.
     */
    record StartTimeoutAt<C>(TimerName timerName, Instant at) implements TimerEffect<C> {
        public StartTimeoutAt {
            requireNonNull(timerName, "timerName cannot be null");
            requireNonNull(at, "at cannot be null");
        }
    }

    /** Cancel the timer named {@code timerName} if it is running, a no-op otherwise. */
    record CancelTimeout<C>(TimerName timerName) implements TimerEffect<C> {
        public CancelTimeout {
            requireNonNull(timerName, "timerName cannot be null");
        }
    }

    /** Issue {@code command}. */
    static <C> SagaEffect<C> issue(C command) {
        return new IssueCommand<>(command);
    }

    // The three timer factories are typed as TimerEffect rather than SagaEffect so a value built here can be compared
    // straight against Saga.Step.timerEffects() without a cast. Every use as a plain effect still works, since a
    // reaction's List<SagaEffect<C>> is the target type the elements are inferred against.

    /** Start (or restart) a timer firing once the duration {@code after} has elapsed. */
    static <C> TimerEffect<C> startTimeout(TimerName timerName, Duration after) {
        return new StartTimeout<>(timerName, after);
    }

    /**
     * Start (or restart) a timer firing once the duration {@code after} has elapsed, naming it with a string that
     * {@link TimerName#parse(String)} reads.
     */
    static <C> TimerEffect<C> startTimeout(String timerName, Duration after) {
        return new StartTimeout<>(TimerName.parse(timerName), after);
    }

    /** Start (or restart) a timer firing at {@code at}. */
    static <C> TimerEffect<C> startTimeoutAt(TimerName timerName, Instant at) {
        return new StartTimeoutAt<>(timerName, at);
    }

    /**
     * Start (or restart) a timer firing at {@code at}, naming it with a string that {@link TimerName#parse(String)}
     * reads.
     */
    static <C> TimerEffect<C> startTimeoutAt(String timerName, Instant at) {
        return new StartTimeoutAt<>(TimerName.parse(timerName), at);
    }

    /** Cancel the timer named {@code timerName}. */
    static <C> TimerEffect<C> cancelTimeout(TimerName timerName) {
        return new CancelTimeout<>(timerName);
    }

    /** Cancel the timer named by a string that {@link TimerName#parse(String)} reads. */
    static <C> TimerEffect<C> cancelTimeout(String timerName) {
        return new CancelTimeout<>(TimerName.parse(timerName));
    }
}
