package org.occurrent.eventstore.mongodb.cloudevent;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.rw.*;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.occurrent.eventstore.mongodb.cloudevent.JsonContentType.isJson;

public class DocumentCloudEventReader implements CloudEventReader {
    private static final String CONTENT_TYPE_ATTRIBUTE_NAME = "datacontenttype";
    private static final String DATA_ATTRIBUTE_NAME = "data";
    private static final String SPEC_VERSION_ATTRIBUTE_NAME = "specversion";

    private final Document document;

    public DocumentCloudEventReader(Document document) {
        this.document = document;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V extends CloudEventWriter<R>, R> R read(CloudEventWriterFactory<V, R> cloudEventWriterFactory, CloudEventDataMapper mapper) throws CloudEventRWException {
        SpecVersion specVersion = SpecVersion.parse((String) document.get(SPEC_VERSION_ATTRIBUTE_NAME));

        V writer = cloudEventWriterFactory.create(specVersion);

        // Let's deal with attributes first
        Set<String> allCloudEventAttributes = specVersion.getAllAttributes();
        for (String attributeName : allCloudEventAttributes) {
            if (Objects.equals(attributeName, SPEC_VERSION_ATTRIBUTE_NAME)) {
                continue;
            }
            Object attributeValue = document.get(attributeName);

            if (attributeValue != null) {
                writer.withAttribute(attributeName, attributeValue.toString());
            }
        }

        // Let's deal with extensions
        for (Map.Entry<String, Object> extension : document.entrySet()) {
            if (allCloudEventAttributes.contains(extension.getKey())) {
                continue;
            }
            if (extension.getKey().equals("data")) {
                // data handled later
                continue;
            }

            // Switch on types document support
            if (extension.getValue() instanceof Number) {
                writer.withExtension(extension.getKey(), (Number) extension.getValue());
            } else if (extension.getValue() instanceof Boolean) {
                writer.withExtension(extension.getKey(), (Boolean) extension.getValue());
            } else {
                writer.withExtension(extension.getKey(), extension.getValue().toString());
            }
        }

        // Let's handle data
        Object data = document.get(DATA_ATTRIBUTE_NAME);
        if (data != null) {
            Object contentType = document.get(CONTENT_TYPE_ATTRIBUTE_NAME);
            final CloudEventData ceData;
            if (isJson(contentType)) {
                if (data instanceof Document) {
                    // Best case, it's a document
                    ceData = new DocumentCloudEventData((Document) data);
                } else if (data instanceof String) {
                    ceData = new DocumentCloudEventData(Document.parse((String) data));
                } else if (data instanceof Map) {
                    ceData = new DocumentCloudEventData((Map<String, Object>) data);
                } else if (data instanceof byte[]) {
                    String json = new String((byte[]) data, UTF_8);
                    ceData = new DocumentCloudEventData(Document.parse(json));
                } else {
                    //TODO next cloudevents-sdk version will add proper exception kind here
                    throw CloudEventRWException.newOther(new IllegalArgumentException("Data type is unknown: " + data.getClass().toString() + ". Only " + Document.class.getName() + ", String, byte[], and Map are supported."));
                }
            } else if (data instanceof byte[]) {
                ceData = new BytesCloudEventData((byte[]) data);
            } else if (data instanceof String) {
                ceData = new BytesCloudEventData(((String) data).getBytes(UTF_8));
            } else if (data instanceof Binary) {
                ceData = new BytesCloudEventData(((Binary) data).getData());
            } else {
                //TODO next cloudevents-sdk version will add proper exception kind here
                throw CloudEventRWException.newOther(new IllegalArgumentException(String.format("Data type is unknown: %s for %s %s. Only String and byte[] are supported.", data.getClass().getName(), CONTENT_TYPE_ATTRIBUTE_NAME, contentType)));
            }

            writer.end(mapper != null ? mapper.map(ceData) : ceData);
        }
        return writer.end();
    }

    @Override
    public void readAttributes(CloudEventAttributesWriter cloudEventAttributesWriter) throws CloudEventRWException {
        // That's fine, this method is optional and only used when performing direct transcoding
        throw new UnsupportedOperationException();
    }

    @Override
    public void readExtensions(CloudEventExtensionsWriter cloudEventExtensionsWriter) throws CloudEventRWException {
        // That's fine, this method is optional and only used when performing direct transcoding
        throw new UnsupportedOperationException();
    }

    // Example method for Document -> Event
    public static CloudEvent toCloudEvent(Document document) {
        DocumentCloudEventReader reader = new DocumentCloudEventReader(document);
        try {
            return reader.read(CloudEventBuilder::fromSpecVersion);
        } catch (CloudEventRWException e) {
            // TODO Something went wrong when deserializing the event, deal with it
            throw e;
        }
    }
}
