package org.occurrent.eventstore.mongodb.cloudevent;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.impl.CloudEventUtils;
import io.cloudevents.rw.*;
import org.bson.Document;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.time.OffsetDateTime;

public class DocumentCloudEventWriter implements CloudEventWriterFactory<DocumentCloudEventWriter, Document>, CloudEventWriter<Document> {

  private final Document document;
  private final TimeRepresentation representation;

  public DocumentCloudEventWriter(Document document, TimeRepresentation representation) {
    this.representation = representation;
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
    // TODO do stuff with time representation
    document.append(name, value.toString());
    return this;
  }

  @Override
  public CloudEventExtensionsWriter withExtension(String s, String s1) throws CloudEventRWException {
    document.append(s, s1);
    return this;
  }

  @Override
  public CloudEventExtensionsWriter withExtension(String name, Number value) throws CloudEventRWException {
    document.append(name, value); // Document accept numbers, right?
    return this;
  }

  @Override
  public CloudEventExtensionsWriter withExtension(String name, Boolean value) throws CloudEventRWException {
    document.append(name, value); // Document accept booleans, right?
    return this;
  }

  @Override
  public Document end(CloudEventData cloudEventData) throws CloudEventRWException {
    if (cloudEventData instanceof MongoDBCloudEventData) {
      document.put("data", ((MongoDBCloudEventData) cloudEventData).document);
    }
    document.put("data", cloudEventData.toBytes());
    return document;
  }

  @Override
  public Document end() {
    return document;
  }

  // Example method for Event -> Document
  public static Document toDocument(CloudEvent event, TimeRepresentation representation) {
    DocumentCloudEventWriter writer = new DocumentCloudEventWriter(new Document(), representation);
    try {
      return CloudEventUtils.toVisitable(event).read(writer);
    } catch (CloudEventRWException e) {
      // Something went wrong when serializing the event, deal with it
      return null;
    }
  }

}
