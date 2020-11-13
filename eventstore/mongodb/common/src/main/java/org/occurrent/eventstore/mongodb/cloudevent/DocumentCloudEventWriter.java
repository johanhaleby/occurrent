package org.occurrent.eventstore.mongodb.cloudevent;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.impl.CloudEventUtils;
import io.cloudevents.rw.*;
import org.bson.Document;

import java.time.OffsetDateTime;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.occurrent.eventstore.mongodb.cloudevent.JsonContentType.isJson;

public class DocumentCloudEventWriter implements CloudEventWriterFactory<DocumentCloudEventWriter, Document>, CloudEventWriter<Document> {

    private final Document document;

    public DocumentCloudEventWriter(Document document) {
        this.document = document;
    }

    @Override
    public DocumentCloudEventWriter create(SpecVersion specVersion) {
        document.append("specversion", specVersion.toString());
        return this;
    }

    @Override
    public CloudEventAttributesWriter withAttribute(String s, String s1) throws CloudEventRWException {
        document.append(s, s1);
        return this;
    }

    @Override
    public CloudEventAttributesWriter withAttribute(String name, OffsetDateTime value) throws CloudEventRWException {
        if (value != null) {
            document.append(name, value.toString());
        }
        return this;
    }

    @Override
    public CloudEventExtensionsWriter withExtension(String s, String s1) throws CloudEventRWException {
        document.append(s, s1);
        return this;
    }

    @Override
    public CloudEventExtensionsWriter withExtension(String name, Number value) throws CloudEventRWException {
        document.append(name, value);
        return this;
    }

    @Override
    public CloudEventExtensionsWriter withExtension(String name, Boolean value) throws CloudEventRWException {
        document.append(name, value);
        return this;
    }

    @Override
    public Document end(CloudEventData cloudEventData) throws CloudEventRWException {
        if (cloudEventData instanceof DocumentCloudEventData) {
            document.put("data", ((DocumentCloudEventData) cloudEventData).document);
        } else if (isJson(document.get("datacontenttype"))) {
            String json = new String(cloudEventData.toBytes(), UTF_8);
            document.put("data", Document.parse(json));
        } else {
            document.put("data", cloudEventData.toBytes());
        }
        return document;
    }

    @Override
    public Document end() {
        return document;
    }

    // Example method for Event -> Document
    public static Document toDocument(CloudEvent event) {
        DocumentCloudEventWriter writer = new DocumentCloudEventWriter(new Document());
        try {
            return CloudEventUtils.toVisitable(event).read(writer);
        } catch (CloudEventRWException e) {
            // TODO Something went wrong when serializing the event, deal with it
            throw e;
        }
    }

}
