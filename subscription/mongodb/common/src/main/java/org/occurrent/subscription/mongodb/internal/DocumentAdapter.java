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