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

import static java.util.Objects.requireNonNull;

/**
 * A saga timer that has fired, delivered back into the saga as {@link SagaInput#timeout(SagaTimeout)}. It carries exactly
 * what routing needs and nothing more: the saga instance id the timer belongs to and the timer name. There is no user
 * payload, because the saga's persisted state already holds everything {@link Saga#react(Object, SagaInput)} needs when a
 * timer fires.
 * <p>
 * The instance id is a {@code String} so it round-trips losslessly through whatever the executor persists timers in.
 * The timer name is a {@link TimerName}, and {@link TimerName#encode()} gives the string it is stored under when you
 * need one.
 *
 * @param sagaId    the id of the saga instance the timer belongs to
 * @param timerName the name of the timer that fired, as given to {@link SagaEffect#startTimeout(TimerName, java.time.Duration)}
 */
public record SagaTimeout(String sagaId, TimerName timerName) {
    public SagaTimeout {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(timerName, "timerName cannot be null");
    }
}
