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
     * @return A {@code CloudEvent} without Occurrents extension
     */
    public static CloudEvent removeOccurrentExtensions(CloudEvent cloudEvent) {
        Objects.requireNonNull(cloudEvent, CloudEvent.class.getSimpleName() + " cannot be null");
        io.cloudevents.core.v1.CloudEventBuilder b = CloudEventBuilder.v1(cloudEvent);
        OccurrentCloudEventExtension.KEYS.forEach(occurrentExtensionKey -> {
            b.setExtension(occurrentExtensionKey, (String) null);
        });
        return b.build();
    }
}