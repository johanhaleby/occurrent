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

package org.occurrent.application.service.internal;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.util.ArrayList;
import java.util.List;

/**
 * The events of a stream write as a synchronous subscription filter will see them, before they are written. A filter
 * on the stream id or the stream version has to match the events the paused-subscription check is asked about, or a
 * paused handler with such a filter misses the refusal and the event with it. The store sets both when it writes, and
 * the application service knows both before it writes. The position and the append id are known only once the store
 * has written the events, so a filter on either can't be checked before the write.
 */
public final class EventsAsWritten {

    private EventsAsWritten() {
    }

    /**
     * @param streamId            The stream the events are written to
     * @param streamVersionBefore The version of the stream the write expects, the one the events follow
     * @param events              The events to write
     * @return The events with the stream id and stream version the write gives them
     */
    public static List<CloudEvent> inStream(String streamId, long streamVersionBefore, List<CloudEvent> events) {
        List<CloudEvent> asWritten = new ArrayList<>(events.size());
        for (int i = 0; i < events.size(); i++) {
            asWritten.add(CloudEventBuilder.v1(events.get(i)).withExtension(OccurrentCloudEventExtension.occurrent(streamId, streamVersionBefore + i + 1)).build());
        }
        return asWritten;
    }
}
