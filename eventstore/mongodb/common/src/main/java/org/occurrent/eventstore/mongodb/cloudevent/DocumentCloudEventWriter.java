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
import io.cloudevents.core.CloudEventUtils;
import io.cloudevents.rw.*;
import org.bson.Document;

import java.time.OffsetDateTime;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.occurrent.eventstore.mongodb.cloudevent.ContentType.isJson;
import static org.occurrent.eventstore.mongodb.cloudevent.ContentType.isText;

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
        Object contentType = document.get("datacontenttype");
        if (cloudEventData instanceof DocumentCloudEventData) {
            document.put("data", ((DocumentCloudEventData) cloudEventData).document);
        } else if (isJson(contentType)) {
            String json = convertToString(cloudEventData);
            if (json.trim().startsWith("{")) {
                document.put("data", Document.parse(json));
            } else {
                document.put("data", json);
            }
        } else if (isText(contentType)) {
            String text = convertToString(cloudEventData);
            document.put("data", text);
        } else {
            // Note that we cannot convert the data to DocumentCloudEventData even if content-type is json.
            // This is because json data can be an array (or just a string) and this thus
            // not necessarily representable as a "map" (and thus not as a org.bson.Document)
            document.put("data", cloudEventData.toBytes());
        }
        return document;
    }

    private static String convertToString(CloudEventData cloudEventData) {
        byte[] bytes = cloudEventData.toBytes();
        return new String(bytes, UTF_8);
    }

    @Override
    public Document end() {
        return document;
    }

    // Example method for Event -> Document
    public static Document toDocument(CloudEvent event) {
        DocumentCloudEventWriter writer = new DocumentCloudEventWriter(new Document());
        return CloudEventUtils.toReader(event).read(writer);
    }

}
