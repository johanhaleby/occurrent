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

package org.occurrent.dsl.dcb.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;

import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * DCB metadata of a delivered event, namely the DCB sequence position and the DCB tags.
 * <p>
 * Read straight from the {@link CloudEvent} extensions. The reactive subscription stack has no generic
 * {@code EventMetadata} type, so unlike the blocking DCB DSL this reads the CloudEvent directly rather than wrapping a
 * generic metadata object.
 */
@NullMarked
public final class DcbEventMetadata {

    private final CloudEvent cloudEvent;

    public DcbEventMetadata(CloudEvent cloudEvent) {
        this.cloudEvent = requireNonNull(cloudEvent, "CloudEvent cannot be null");
    }

    public static DcbEventMetadata from(CloudEvent cloudEvent) {
        return new DcbEventMetadata(cloudEvent);
    }

    /**
     * The DCB sequence position of the event, or empty when the event has no DCB position (for example a regular
     * stream-written event).
     */
    public OptionalLong dcbPosition() {
        long position = DcbCloudEvents.getPosition(cloudEvent);
        return position > 0 ? OptionalLong.of(position) : OptionalLong.empty();
    }

    /**
     * The canonical DCB tags of the event, or an empty set when the event has no DCB tags.
     */
    public Set<String> dcbTags() {
        return DcbCloudEvents.getTags(cloudEvent);
    }

    /**
     * The delivered {@link CloudEvent}, for any metadata not specific to DCB.
     */
    public CloudEvent cloudEvent() {
        return cloudEvent;
    }
}
