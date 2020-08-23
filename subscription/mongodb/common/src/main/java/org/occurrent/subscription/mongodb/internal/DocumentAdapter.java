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

package org.occurrent.subscription.mongodb.internal;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;

public class DocumentAdapter {
    private final CodecRegistry registry;
    private final Codec<Document> codec;

    public DocumentAdapter(CodecRegistry registry) {
        this.registry = registry;
        this.codec = registry.get(Document.class);
    }

    public Document fromBson(BsonDocument bson) {
        return codec.decode(bson.asBsonReader(), DecoderContext.builder().build());
    }

    public BsonDocument toBson(Document document) {
        return document.toBsonDocument(BsonDocument.class, registry);
    }
}