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

/**
 * What a {@link SagaRunner} does with an event it cannot recognise a redelivery of. A saga tells a redelivered event
 * from a new one by its {@code streamid} together with its {@code streamversion}, or by its {@code position}. An event
 * carrying none of those leaves nothing to compare against, so the reaction runs again on every redelivery and issues
 * its commands again.
 * <p>
 * Only a feed handing the runner events it did not read from the event store can produce such an event. Occurrent's
 * own stored events always carry the metadata, and so does a catch-up replay.
 *
 * @see SagaRunnerConfig#redeliveryDetection()
 */
public enum RedeliveryDetection {
    /**
     * The default. An event with no {@code streamid}/{@code streamversion} and no {@code position} is refused with a
     * {@link SagaRedeliveryDetectionException} rather than reacted to, so the feed that dropped the metadata announces
     * itself instead of quietly costing the saga its redelivery protection. The exception propagates to the
     * subscription model, so the event is not acknowledged and a push feed offers it again.
     */
    REQUIRED,
    /**
     * Take the event anyway, logging a warning the first time. Choose this for a feed that genuinely carries no such
     * metadata, and only when every command the saga issues is safe to receive more than once, since a redelivery runs
     * every reaction again.
     */
    BEST_EFFORT
}
