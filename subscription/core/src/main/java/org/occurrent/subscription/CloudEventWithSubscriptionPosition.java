/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription;

import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.lang.Nullable;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Set;

/**
 * A wrapper around a {@link CloudEvent} that also includes a {@link SubscriptionPosition} so that
 * it's possible to resume the stream from a particular state.
 */
public class CloudEventWithSubscriptionPosition implements CloudEvent {
    private final CloudEvent cloudEvent;
    private final SubscriptionPosition changeStreamPosition;

    public CloudEventWithSubscriptionPosition(CloudEvent cloudEvent, SubscriptionPosition changeStreamPosition) {
        this.cloudEvent = cloudEvent;
        this.changeStreamPosition = changeStreamPosition;
    }

    @Nullable
    public byte[] getData() {
        return cloudEvent.getData();
    }

    public SpecVersion getSpecVersion() {
        return cloudEvent.getSpecVersion();
    }

    public String getId() {
        return cloudEvent.getId();
    }

    public String getType() {
        return cloudEvent.getType();
    }

    public URI getSource() {
        return cloudEvent.getSource();
    }

    @Nullable
    public String getDataContentType() {
        return cloudEvent.getDataContentType();
    }

    @Nullable
    public URI getDataSchema() {
        return cloudEvent.getDataSchema();
    }

    @Nullable
    public String getSubject() {
        return cloudEvent.getSubject();
    }

    @Nullable
    public OffsetDateTime getTime() {
        return cloudEvent.getTime();
    }

    @Nullable
    public Object getAttribute(String attributeName) throws IllegalArgumentException {
        return cloudEvent.getAttribute(attributeName);
    }

    public Set<String> getAttributeNames() {
        return cloudEvent.getAttributeNames();
    }

    @Nullable
    public Object getExtension(String extensionName) {
        return cloudEvent.getExtension(extensionName);
    }

    public Set<String> getExtensionNames() {
        return cloudEvent.getExtensionNames();
    }

    public SubscriptionPosition getStreamPosition() {
        return changeStreamPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CloudEventWithSubscriptionPosition)) return false;
        CloudEventWithSubscriptionPosition that = (CloudEventWithSubscriptionPosition) o;
        return Objects.equals(cloudEvent, that.cloudEvent) &&
                Objects.equals(changeStreamPosition, that.changeStreamPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cloudEvent, changeStreamPosition);
    }

    @Override
    public String toString() {
        return "CloudEventWithStreamPosition{" +
                "cloudEvent=" + cloudEvent +
                ", subscriptionPosition=" + changeStreamPosition +
                '}';
    }
}