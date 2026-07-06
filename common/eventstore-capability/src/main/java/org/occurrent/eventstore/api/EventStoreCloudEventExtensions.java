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

package org.occurrent.eventstore.api;

/**
 * CloudEvent extension names that identify which {@link EventStoreCapability} wrote an event. These are the names of
 * extensions on the live {@link io.cloudevents.CloudEvent}, the single source of truth shared by anything that needs to
 * tell capability-scoped events apart, so the literal string is not duplicated across modules.
 * <p>
 * Note that a CloudEvent extension name is not necessarily identical to the field name a particular storage engine uses
 * to persist that extension. For example, MongoDB additionally maintains a separate indexed {@code dcbTags} array field
 * for tag-containment queries, which is a different field than the {@value #DCB_TAGS} extension carried on the CloudEvent
 * itself.
 */
public final class EventStoreCloudEventExtensions {

    /**
     * CloudEvent extension that a Dynamic Consistency Boundary append always stamps (even for an empty tag set), holding
     * the newline-separated DCB tags. A stream-written event never carries it, so its presence is the reliable
     * discriminator between a DCB-written event and a stream-written one.
     */
    public static final String DCB_TAGS = "dcbtags";

    private EventStoreCloudEventExtensions() {
    }
}
