package se.haleby.occurrent.cloudevents;

import io.cloudevents.CloudEventExtensions;
import io.cloudevents.Extension;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class OccurrentCloudEventExtension implements Extension {
    public static final String STREAM_ID = "streamId";

    private static final Set<String> KEY_SET = Collections.unmodifiableSet(new HashSet<>(Collections.singletonList(STREAM_ID)));

    private String streamId;

    public OccurrentCloudEventExtension(String streamId) {
        Objects.requireNonNull(streamId, "StreamId cannot be null");
        this.streamId = streamId;
    }

    public static OccurrentCloudEventExtension occurrent(String streamId) {
        return new OccurrentCloudEventExtension(streamId);
    }

    @Override
    public void readFrom(CloudEventExtensions extensions) {
        Object streamId = extensions.getExtension(STREAM_ID);
        if (streamId != null) {
            this.streamId = streamId.toString();
        }
    }

    @Override
    public Object getValue(String key) throws IllegalArgumentException {
        if (STREAM_ID.equals(key)) {
            return this.streamId;
        }
        throw new IllegalArgumentException(this.getClass().getSimpleName() + " doesn't expect the attribute key \"" + key + "\"");
    }

    @Override
    public Set<String> getKeys() {
        return KEY_SET;
    }
}