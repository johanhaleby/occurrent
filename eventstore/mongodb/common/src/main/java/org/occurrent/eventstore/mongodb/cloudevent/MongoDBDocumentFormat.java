package org.occurrent.eventstore.mongodb.cloudevent;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventDeserializationException;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.format.EventSerializationException;
import io.cloudevents.rw.CloudEventDataMapper;

public class MongoDBDocumentFormat implements EventFormat {

    public static final String CONTENT_TYPE = "application/cloudevents+json";

    @Override
    public byte[] serialize(CloudEvent event) throws EventSerializationException {
        return new byte[0];
    }

    @Override
    public CloudEvent deserialize(byte[] bytes, CloudEventDataMapper mapper) throws EventDeserializationException {
        return null;
    }

    @Override
    public String serializedContentType() {
        return CONTENT_TYPE;
    }
}
