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

package org.occurrent.subscription.reactor.durable.catchup;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;

import java.net.URI;

/**
 * The replay-to-live overlap cache's key. CloudEvents only promises that {@code id} and {@code source} together are
 * unique, so an event store can hold two events with the same id from different producers. Keying the cache by id
 * alone would let a replayed event from one producer suppress a live event from another.
 */
@NullMarked
record CatchupEventKey(String id, URI source) {
    static CatchupEventKey of(CloudEvent event) {
        return new CatchupEventKey(event.getId(), event.getSource());
    }
}
