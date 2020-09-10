package org.occurrent.cloudevents;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;

import java.util.Objects;


/**
 * Utility class that removes all Occurrent extensions from a {@link CloudEvent}.
 */
public class OccurrentExtensionRemover {

    /**
     * Remove all occurrent extensions from a cloud event. This is useful, for example,
     * if you want to forward a {@link CloudEvent} that has been persisted into an Occurrent
     * event store but you don't want to occurrent extensions (metadata) to be included.
     *
     * @param cloudEvent The cloud event to remove Occurrent's {@code CloudEvent} extensions from.
     * @return A {@code CloudEvent} without Occurrent's extension
     */
    public static CloudEvent removeOccurrentExtensions(CloudEvent cloudEvent) {
        Objects.requireNonNull(cloudEvent, CloudEvent.class.getSimpleName() + " cannot be null");
        CloudEventBuilder builder = CloudEventBuilder.v1(cloudEvent);
        OccurrentCloudEventExtension.KEYS.forEach(builder::withoutExtension);
        return builder.build();
    }
}