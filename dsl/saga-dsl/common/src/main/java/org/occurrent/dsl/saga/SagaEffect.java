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
 *
 * @param <C> the command type the saga issues
 */
public sealed interface SagaEffect<C> {

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
     * Start (or, if one with the same {@code timerName} already runs, restart) a timer that fires {@code after} the given
     * duration. The duration is relative and resolved against the clock by the executor when it stores the timer, not
     * here: building an absolute time inside the pure {@code react} would read the clock and make the same reaction
     * produce different effect values on each call.
     */
    record StartTimeout<C>(String timerName, Duration after) implements SagaEffect<C> {
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
    record StartTimeoutAt<C>(String timerName, Instant at) implements SagaEffect<C> {
        public StartTimeoutAt {
            requireNonNull(timerName, "timerName cannot be null");
            requireNonNull(at, "at cannot be null");
        }
    }

    /** Cancel the timer named {@code timerName} if it is running, a no-op otherwise. */
    record CancelTimeout<C>(String timerName) implements SagaEffect<C> {
        public CancelTimeout {
            requireNonNull(timerName, "timerName cannot be null");
        }
    }

    /** Issue {@code command}. */
    static <C> SagaEffect<C> issue(C command) {
        return new IssueCommand<>(command);
    }

    /** Start (or restart) a timer firing once the duration {@code after} has elapsed. */
    static <C> SagaEffect<C> startTimeout(String timerName, Duration after) {
        return new StartTimeout<>(timerName, after);
    }

    /** Start (or restart) a timer firing at {@code at}. */
    static <C> SagaEffect<C> startTimeoutAt(String timerName, Instant at) {
        return new StartTimeoutAt<>(timerName, at);
    }

    /** Cancel the timer named {@code timerName}. */
    static <C> SagaEffect<C> cancelTimeout(String timerName) {
        return new CancelTimeout<>(timerName);
    }
}
