package org.occurrent.eventstore.mongodb.cloudevent;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.rw.*;
import org.bson.Document;

import java.util.Map;

public class DocumentCloudEventReader implements CloudEventReader {

  private final Document document;

  public DocumentCloudEventReader(Document document) {
    this.document = document;
  }

  @Override
  public <V extends CloudEventWriter<R>, R> R read(CloudEventWriterFactory<V, R> cloudEventWriterFactory, CloudEventDataMapper mapper) throws CloudEventRWException {
    SpecVersion specVersion = SpecVersion.parse((String)document.remove("specversion"));

    V writer = cloudEventWriterFactory.create(specVersion);

    // Let's deal with attributes first
    for (String attributeName : specVersion.getAllAttributes()) {
      Object attributeValue = document.remove(attributeName);

      if (attributeValue != null) {
        writer.withAttribute(attributeName, attributeValue.toString());
      }
    }

    // Let's deal with extensions
    for (Map.Entry<String, Object> extension : document.entrySet()) {
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
    Object data = document.remove("data");
    if (data != null) {
      CloudEventData ceData;
      if (data instanceof Document) {
        // Best case, it's a document
        ceData = new MongoDBCloudEventData((Document) data);
      } else if (data instanceof byte[]) {
        // Worst case, convert to bytes
        ceData = new BytesCloudEventData((byte[]) data);
      } else {
        //TODO next cloudevents-sdk version will add proper exception kind here
        throw CloudEventRWException.newOther(new IllegalStateException("Data type is unknown: " + data.getClass().toString() + ". Only Document and byte[] accepted."));
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
  public static CloudEvent toEvent(Document document) {
    DocumentCloudEventReader reader = new DocumentCloudEventReader(document);
    try {
      return reader.read(CloudEventBuilder::fromSpecVersion);
    } catch (CloudEventRWException e) {
      // TODO Something went wrong when deserializing the event, deal with it
      throw e;
    }
  }
}
