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
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.lang.Nullable;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A wrapper around a {@link CloudEvent} that also includes a {@link SubscriptionPosition} so that
 * it's possible to resume the stream from a particular state. You can treat this cloud event implementation
 * as a regular cloud event.
 */
public final class PositionAwareCloudEvent implements CloudEvent {
    private final CloudEvent cloudEvent;
    private final SubscriptionPosition subscriptionPosition;

    public PositionAwareCloudEvent(CloudEvent cloudEvent, SubscriptionPosition subscriptionPosition) {
        Objects.requireNonNull(cloudEvent, CloudEvent.class.getSimpleName() + "cannot be null");
        Objects.requireNonNull(subscriptionPosition, SubscriptionPosition.class.getSimpleName() + "cannot be null");
        this.cloudEvent = cloudEvent;
        this.subscriptionPosition = subscriptionPosition;
    }

    @Nullable
    public CloudEventData getData() {
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

    public SubscriptionPosition getSubscriptionPosition() {
        return subscriptionPosition;
    }

    public CloudEvent getOriginalCloudEvent() {
        return cloudEvent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PositionAwareCloudEvent)) return false;
        PositionAwareCloudEvent that = (PositionAwareCloudEvent) o;
        return Objects.equals(cloudEvent, that.cloudEvent) &&
                Objects.equals(subscriptionPosition, that.subscriptionPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cloudEvent, subscriptionPosition);
    }

    @Override
    public String toString() {
        return "SubscriptionCloudEvent{" +
                "cloudEvent=" + cloudEvent +
                ", changeStreamPosition=" + subscriptionPosition +
                '}';
    }

    public static boolean hasSubscriptionPosition(CloudEvent cloudEvent) {
        return cloudEvent instanceof PositionAwareCloudEvent;
    }

    public static SubscriptionPosition getSubscriptionPositionOrThrowIAE(CloudEvent cloudEvent) {
        return getSubscriptionPosition(cloudEvent).orElseThrow(() -> new IllegalArgumentException(CloudEvent.class.getSimpleName() + " doesn't contain a subscription position"));
    }

    public static Optional<SubscriptionPosition> getSubscriptionPosition(CloudEvent cloudEvent) {
        if (cloudEvent instanceof PositionAwareCloudEvent) {
            return Optional.ofNullable(((PositionAwareCloudEvent) cloudEvent).getSubscriptionPosition());
        }
        return Optional.empty();
    }
}