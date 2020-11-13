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

import io.cloudevents.CloudEventData;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An implementation of {@link CloudEventData} that holds a {@link Document}. Use this class if you know you're writing to MongoDB to avoid double mapping from
 * JSON bytes into a {@code Document}.
 */
public class DocumentCloudEventData implements CloudEventData {

    public final Document document;

    public DocumentCloudEventData(byte[] json) {
        this(Document.parse(new String(json, UTF_8)));
    }


    public DocumentCloudEventData(String json) {
        this(Document.parse(json));
    }

    public DocumentCloudEventData(Map<String, Object> map) {
        this(new Document(map));
    }

    public DocumentCloudEventData(Document document) {
        Objects.requireNonNull(document, Document.class.getSimpleName() + " cannot be null");
        this.document = document;
    }

    @Override
    public byte[] toBytes() {
        return document.toJson().getBytes(UTF_8);
    }

    public Document getDocument() {
        return document;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DocumentCloudEventData)) return false;
        DocumentCloudEventData that = (DocumentCloudEventData) o;
        return Objects.equals(document, that.document);
    }

    @Override
    public int hashCode() {
        return Objects.hash(document);
    }

    @Override
    public String toString() {
        return "DocumentCloudEventData{" +
                "document=" + document +
                '}';
    }

}