/*
 * Copyright 2020 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.eventstore.mongodb.cloudevent;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.rw.*;
import org.bson.Document;
import org.bson.types.Binary;
import org.jspecify.annotations.NullMarked;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.occurrent.eventstore.mongodb.cloudevent.ContentType.isJson;

@NullMarked
public class DocumentCloudEventReader implements CloudEventReader {
    private static final String CONTENT_TYPE_ATTRIBUTE_NAME = "datacontenttype";
    private static final String DATA_ATTRIBUTE_NAME = "data";
    private static final String SPEC_VERSION_ATTRIBUTE_NAME = "specversion";
    private static final String JSON_OBJECT_PREFIX = "{";

    private final Document document;

    public DocumentCloudEventReader(Document document) {
        this.document = document;
    }

    @Override
    public <V extends CloudEventWriter<R>, R> R read(CloudEventWriterFactory<V, R> cloudEventWriterFactory, CloudEventDataMapper<? extends CloudEventData> mapper) throws CloudEventRWException {
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
                writer.withContextAttribute(attributeName, attributeValue.toString());
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
            if (extension.getValue() instanceof Integer) {
                writer.withContextAttribute(extension.getKey(), (Integer) extension.getValue());
            } else if (extension.getValue() instanceof Boolean) {
                writer.withContextAttribute(extension.getKey(), (Boolean) extension.getValue());
            } else {
                writer.withContextAttribute(extension.getKey(), extension.getValue().toString());
            }
        }

        // Let's handle data
        Object data = document.get(DATA_ATTRIBUTE_NAME);
        if (data != null) {
            Object contentType = document.get(CONTENT_TYPE_ATTRIBUTE_NAME);
            final CloudEventData ceData;
            if (isJson(contentType)) {
                ceData = convertJsonData(data);
            } else if (data instanceof byte[]) {
                ceData = BytesCloudEventData.wrap((byte[]) data);
            } else if (data instanceof String) {
                ceData = BytesCloudEventData.wrap(((String) data).getBytes(UTF_8));
            } else if (data instanceof Binary) {
                ceData = BytesCloudEventData.wrap(((Binary) data).getData());
            } else {
                throw CloudEventRWException.newInvalidDataType(data.getClass().getName(), String.class.getName(), byte[].class.getName(), Binary.class.getName());
            }

            writer.end(mapper != null ? mapper.map(ceData) : ceData);
        }
        return writer.end();
    }

    @SuppressWarnings("unchecked")
    private static CloudEventData convertJsonData(Object data) {
        final CloudEventData ceData;
        if (data instanceof Document document) {
            // Best case, it's a document
            ceData = PojoCloudEventData.wrap(document, DocumentCloudEventReader::convertDocumentToBytes);
        } else if (data instanceof String json) {
            if (json.trim().startsWith(JSON_OBJECT_PREFIX)) {
                ceData = PojoCloudEventData.wrap(Document.parse((String) data), DocumentCloudEventReader::convertDocumentToBytes);
            } else {
                ceData = BytesCloudEventData.wrap(json.getBytes(UTF_8));
            }
        } else if (data instanceof Map) {
            ceData = PojoCloudEventData.wrap(new Document((Map<String, Object>) data), DocumentCloudEventReader::convertDocumentToBytes);
        } else if (data instanceof byte[] byteArray) {
            String json = new String(byteArray, UTF_8);
            if (json.trim().startsWith(JSON_OBJECT_PREFIX)) {
                ceData = PojoCloudEventData.wrap(Document.parse(json), DocumentCloudEventReader::convertDocumentToBytes);
            } else {
                ceData = BytesCloudEventData.wrap(json.getBytes(UTF_8));
            }
        } else if (data instanceof Binary) {
            ceData = BytesCloudEventData.wrap(((Binary) data).getData());
        } else {
            throw CloudEventRWException.newInvalidDataType(data.getClass().getName(), String.class.getName(), byte[].class.getName(), Map.class.getName(), Binary.class.getName());
        }
        return ceData;
    }

    public static CloudEvent toCloudEvent(Document document) {
        DocumentCloudEventReader reader = new DocumentCloudEventReader(document);
        return reader.read(CloudEventBuilder::fromSpecVersion);
    }

    private static byte[] convertDocumentToBytes(Document document) {
        return document.toJson().getBytes(UTF_8);
    }
}
