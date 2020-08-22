package se.haleby.occurrent.cloudevents;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventExtensions;
import io.cloudevents.Extension;

import java.util.*;

/**
 * A {@link CloudEvent} {@link Extension} that adds required extensions for Occurrent. These are:<br><br>
 *
 * <table>
 *     <tr><th>Key</th><th>Description</th></tr>
 *     <tr><td>{@value #STREAM_ID}</td><td>The id of a particular event stream</td></tr>
 *     <tr><td>{@value #STREAM_VERSION}</td><td>The version of an event in a particular event stream</td></tr>
 * </table>
 */
public class OccurrentCloudEventExtension implements Extension {
    public static final String STREAM_ID = "streamId";
    public static final String STREAM_VERSION = "streamVersion";

    private static final Set<String> KEY_SET = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(STREAM_ID, STREAM_VERSION)));
    private String streamId;
    private long streamVersion;

    public OccurrentCloudEventExtension(String streamId, long streamVersion) {
        Objects.requireNonNull(streamId, "StreamId cannot be null");
        if (streamVersion < 1) {
            throw new IllegalArgumentException("Stream version cannot be less than 1");
        }
        this.streamId = streamId;
        this.streamVersion = streamVersion;
    }

    public static OccurrentCloudEventExtension occurrent(String streamId, long streamVersion) {
        return new OccurrentCloudEventExtension(streamId, streamVersion);
    }

    @Override
    public void readFrom(CloudEventExtensions extensions) {
        Object streamId = extensions.getExtension(STREAM_ID);
        if (streamId != null) {
            this.streamId = streamId.toString();
        }

        Object streamVersion = extensions.getExtension(STREAM_VERSION);
        if (streamVersion != null) {
            this.streamVersion = (long) streamVersion;
        }
    }

    @Override
    public Object getValue(String key) throws IllegalArgumentException {
        if (STREAM_ID.equals(key)) {
            return this.streamId;
        } else if (STREAM_VERSION.equals(key)) {
            return this.streamVersion;
        }
        throw new IllegalArgumentException(this.getClass().getSimpleName() + " doesn't expect the attribute key \"" + key + "\"");
    }

    @Override
    public Set<String> getKeys() {
        return KEY_SET;
    }
}