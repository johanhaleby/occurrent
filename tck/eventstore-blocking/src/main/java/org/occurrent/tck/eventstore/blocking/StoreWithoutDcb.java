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

package org.occurrent.tck.eventstore.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStore;

import static java.util.Objects.requireNonNull;

/**
 * A store built with the stream capability alone, presented as the DCB interface it must refuse and the stream
 * interface it must still serve.
 * <p>
 * A store implements {@link DcbEventStore} whether or not
 * {@link org.occurrent.eventstore.api.EventStoreCapability#DCB} was enabled on it, so there is always an object to
 * call. {@link CapabilityGuardConformance} asserts that every method on it refuses, and writes through
 * {@code eventStore} to show the store is otherwise alive rather than closed or broken.
 * <p>
 * Both views are handed over separately because neither interface extends the other, exactly as
 * {@link StoreWithoutPosition} does. An implementation where one object is both passes the same instance twice.
 *
 * @param eventStore    writes and reads a stream, which this store must still do
 * @param dcbEventStore the DCB view, every method of which must refuse
 */
@NullMarked
public record StoreWithoutDcb(EventStore eventStore, DcbEventStore dcbEventStore) {

    public StoreWithoutDcb {
        requireNonNull(eventStore, "eventStore cannot be null");
        requireNonNull(dcbEventStore, "dcbEventStore cannot be null");
    }
}
