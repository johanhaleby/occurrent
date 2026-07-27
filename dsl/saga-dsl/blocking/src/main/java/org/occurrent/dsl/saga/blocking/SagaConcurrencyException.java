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
 * Thrown when a saga's state cannot be saved because a concurrent writer kept winning the compare-and-set, exhausting the
 * configured retries. For an event this propagates to the subscription model, which will redeliver and retry. Because that
 * subscription is one ordered channel shared by every instance the saga handles, an event that keeps failing here blocks
 * the events queued behind it until it succeeds. For a timeout the poller catches it, logs it, and leaves the timer due
 * for the next poll, so a failing timeout does not block other instances.
 */
public class SagaConcurrencyException extends RuntimeException {
    public SagaConcurrencyException(String message) {
        super(message);
    }
}
