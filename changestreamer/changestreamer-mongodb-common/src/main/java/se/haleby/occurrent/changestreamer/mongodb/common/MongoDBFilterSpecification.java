package se.haleby.occurrent.changestreamer.mongodb.common;

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.function.BiFunction;

import static com.mongodb.client.model.Filters.elemMatch;

/**
 * Add filters when subscribing to a MongoDB change streamer if you're only interested in specify changes.
 */
public class MongoDBFilterSpecification {

    public static class JsonMongoDBFilterSpecification extends MongoDBFilterSpecification {
        private final String json;

        public JsonMongoDBFilterSpecification(String json) {
            this.json = json;
        }

        public String getJson() {
            return json;
        }

        public static JsonMongoDBFilterSpecification filter(String json) {
            return new JsonMongoDBFilterSpecification(json);
        }
    }

    /**
     * Supply a document filter. For example if using Spring you can do:
     * <pre>
     * filter(where("type").is("MyEventType"))
     * </pre>
     * <p>
     * Where <code>where</code> is imported from the <code>org.springframework.data.mongodb.core.query.Criteria</code> api.
     */
    public static class DocumentMongoDBFilterSpecification extends MongoDBFilterSpecification {
        private final Document[] documents;

        public DocumentMongoDBFilterSpecification(Document document, Document... documents) {
            this.documents = new Document[1 + documents.length];
            documents[0] = document;
            System.arraycopy(documents, 0, this.documents, 1, documents.length);
        }

        public Document[] getDocuments() {
            return documents;
        }

        public static DocumentMongoDBFilterSpecification filter(Document document, Document... documents) {
            return new DocumentMongoDBFilterSpecification(document, documents);
        }
    }

    /**
     * Use e.g. {@link Filters} to create a bson filter. For example:
     *
     * <pre>
     * filter(and(eq("x", 1), lt("y", 3)))
     * </pre>
     */
    public static class BsonMongoDBFilterSpecification extends MongoDBFilterSpecification {

        private final Bson bson;

        private BsonMongoDBFilterSpecification() {
            this.bson = null;
        }

        public BsonMongoDBFilterSpecification(Bson bson) {
            this.bson = bson;
        }

        public static BsonMongoDBFilterSpecification filter(Bson bson) {
            return new BsonMongoDBFilterSpecification(bson);
        }

        public static BsonMongoDBFilterSpecification filter() {
            return new BsonMongoDBFilterSpecification();
        }

        public BsonMongoDBFilterSpecification type(BiFunction<String, String, Bson> filter, String item) {
            return new BsonMongoDBFilterSpecification(elemMatch("events", filter.apply("type", item)));
        }

        public Bson getBson() {
            return bson;
        }
    }
}