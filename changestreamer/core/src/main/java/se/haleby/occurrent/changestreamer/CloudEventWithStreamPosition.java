package se.haleby.occurrent.changestreamer;

import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.lang.Nullable;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Set;

public class CloudEventWithStreamPosition<T> implements CloudEvent {
    private final CloudEvent cloudEvent;
    private final T streamPosition;

    public CloudEventWithStreamPosition(CloudEvent cloudEvent, T streamPosition) {
        this.cloudEvent = cloudEvent;
        this.streamPosition = streamPosition;
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
    public ZonedDateTime getTime() {
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

    public T getStreamPosition() {
        return streamPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CloudEventWithStreamPosition)) return false;
        CloudEventWithStreamPosition<?> that = (CloudEventWithStreamPosition<?>) o;
        return Objects.equals(cloudEvent, that.cloudEvent) &&
                Objects.equals(streamPosition, that.streamPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cloudEvent, streamPosition);
    }

    @Override
    public String toString() {
        return "CloudEventWithStreamPosition{" +
                "cloudEvent=" + cloudEvent +
                ", streamPosition=" + streamPosition +
                '}';
    }
}