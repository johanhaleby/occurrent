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
import io.cloudevents.rw.*;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.occurrent.eventstore.mongodb.cloudevent.ContentType.isJson;

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
        if (data instanceof Document) {
            // Best case, it's a document
            ceData = new DocumentCloudEventData((Document) data);
        } else if (data instanceof String) {
            String json = ((String) data);
            if (json.trim().startsWith(JSON_OBJECT_PREFIX)) {
                ceData = new DocumentCloudEventData(Document.parse((String) data));
            } else {
                ceData = BytesCloudEventData.wrap(json.getBytes(UTF_8));
            }
        } else if (data instanceof Map) {
            ceData = new DocumentCloudEventData((Map<String, Object>) data);
        } else if (data instanceof byte[]) {
            byte[] byteArray = (byte[]) data;
            String json = new String(byteArray, UTF_8);
            if (json.trim().startsWith(JSON_OBJECT_PREFIX)) {
                ceData = new DocumentCloudEventData(Document.parse(json));
            } else {
                ceData = new DocumentCloudEventData(byteArray);
            }
        } else if (data instanceof Binary) {
            ceData = new DocumentCloudEventData(((Binary) data).getData());
        } else {
            throw CloudEventRWException.newInvalidDataType(data.getClass().getName(), String.class.getName(), byte[].class.getName(), Map.class.getName(), DocumentCloudEventData.class.getName(), Binary.class.getName());
        }
        return ceData;
    }

    public static CloudEvent toCloudEvent(Document document) {
        DocumentCloudEventReader reader = new DocumentCloudEventReader(document);
        return reader.read(CloudEventBuilder::fromSpecVersion);
    }
}
