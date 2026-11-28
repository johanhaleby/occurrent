/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.blocking;

import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.dcb.internal.DcbCatchupReadScope;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A Mockito mock of the Mongo event store for a wiring test that never reaches MongoDB. The starter reads the live
 * start of the event store when it creates the store bean, so a mock has to open a scope for it. Both scopes here
 * report no live start, which is what a store with no events reports.
 */
final class MockedMongoEventStore {

    private MockedMongoEventStore() {
    }

    static SpringMongoEventStore withoutLiveStart() {
        SpringMongoEventStore eventStore = mock(SpringMongoEventStore.class);

        CatchupReadScope streamScope = mock(CatchupReadScope.class);
        when(streamScope.liveStart()).thenReturn(null);
        when(eventStore.openCatchupReadScope(any())).thenReturn(streamScope);

        DcbCatchupReadScope dcbScope = mock(DcbCatchupReadScope.class);
        when(dcbScope.liveStart()).thenReturn(null);
        when(eventStore.openDcbCatchupReadScope(any())).thenReturn(dcbScope);

        return eventStore;
    }
}
