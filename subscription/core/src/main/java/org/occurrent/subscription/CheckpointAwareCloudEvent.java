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
 * A wrapper around a {@link CloudEvent} that also includes a {@link Checkpoint} so that
 * it's possible to resume the stream from a particular state. You can treat this cloud event implementation
 * as a regular cloud event.
 */
public final class CheckpointAwareCloudEvent implements CloudEvent {
    private final CloudEvent cloudEvent;
    private final Checkpoint checkpoint;

    public CheckpointAwareCloudEvent(CloudEvent cloudEvent, Checkpoint checkpoint) {
        Objects.requireNonNull(cloudEvent, CloudEvent.class.getSimpleName() + "cannot be null");
        Objects.requireNonNull(checkpoint, Checkpoint.class.getSimpleName() + "cannot be null");
        this.cloudEvent = cloudEvent;
        this.checkpoint = checkpoint;
    }

    public @Nullable CloudEventData getData() {
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

    public @Nullable String getDataContentType() {
        return cloudEvent.getDataContentType();
    }

    public @Nullable URI getDataSchema() {
        return cloudEvent.getDataSchema();
    }

    public @Nullable String getSubject() {
        return cloudEvent.getSubject();
    }

    public @Nullable OffsetDateTime getTime() {
        return cloudEvent.getTime();
    }

    public @Nullable Object getAttribute(String attributeName) throws IllegalArgumentException {
        return cloudEvent.getAttribute(attributeName);
    }

    public Set<String> getAttributeNames() {
        return cloudEvent.getAttributeNames();
    }

    public @Nullable Object getExtension(String extensionName) {
        return cloudEvent.getExtension(extensionName);
    }

    public Set<String> getExtensionNames() {
        return cloudEvent.getExtensionNames();
    }

    public Checkpoint getCheckpoint() {
        return checkpoint;
    }

    public CloudEvent getOriginalCloudEvent() {
        return cloudEvent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CheckpointAwareCloudEvent)) return false;
        CheckpointAwareCloudEvent that = (CheckpointAwareCloudEvent) o;
        return Objects.equals(cloudEvent, that.cloudEvent) &&
                Objects.equals(checkpoint, that.checkpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cloudEvent, checkpoint);
    }

    @Override
    public String toString() {
        return "SubscriptionCloudEvent{" +
                "cloudEvent=" + cloudEvent +
                ", changeStreamPosition=" + checkpoint +
                '}';
    }

    public static boolean hasCheckpoint(CloudEvent cloudEvent) {
        return cloudEvent instanceof CheckpointAwareCloudEvent;
    }

    public static Checkpoint getCheckpointOrThrowIAE(CloudEvent cloudEvent) {
        return getCheckpoint(cloudEvent).orElseThrow(() -> new IllegalArgumentException(CloudEvent.class.getSimpleName() + " doesn't contain a checkpoint"));
    }

    public static Optional<Checkpoint> getCheckpoint(CloudEvent cloudEvent) {
        if (cloudEvent instanceof CheckpointAwareCloudEvent) {
            return Optional.ofNullable(((CheckpointAwareCloudEvent) cloudEvent).getCheckpoint());
        }
        return Optional.empty();
    }
}