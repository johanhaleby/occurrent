package se.haleby.occurrent.subscription.mongodb.spring.internal;

import com.mongodb.MongoClientSettings;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.query.Criteria;
import se.haleby.occurrent.filter.Filter;
import se.haleby.occurrent.mongodb.timerepresentation.TimeRepresentation;
import se.haleby.occurrent.subscription.OccurrentSubscriptionFilter;
import se.haleby.occurrent.subscription.SubscriptionFilter;
import se.haleby.occurrent.subscription.mongodb.MongoDBFilterSpecification.BsonMongoDBFilterSpecification;
import se.haleby.occurrent.subscription.mongodb.MongoDBFilterSpecification.JsonMongoDBFilterSpecification;
import se.haleby.occurrent.subscription.mongodb.internal.DocumentAdapter;

import java.util.stream.Stream;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static se.haleby.occurrent.mongodb.spring.filterqueryconversion.internal.FilterConverter.convertFilterToCriteria;
import static se.haleby.occurrent.subscription.mongodb.MongoDBFilterSpecification.FULL_DOCUMENT;

public class ApplyFilterToChangeStreamOptionsBuilder {

    public static ChangeStreamOptions applyFilter(TimeRepresentation timeRepresentation, SubscriptionFilter filter, ChangeStreamOptionsBuilder changeStreamOptionsBuilder) {
        final ChangeStreamOptions changeStreamOptions;
        if (filter == null) {
            changeStreamOptions = changeStreamOptionsBuilder.build();
        } else if (filter instanceof OccurrentSubscriptionFilter) {
            Filter occurrentFilter = ((OccurrentSubscriptionFilter) filter).filter;
            Criteria criteria = convertFilterToCriteria(FULL_DOCUMENT, timeRepresentation, occurrentFilter);
            changeStreamOptions = changeStreamOptionsBuilder.filter(newAggregation(match(criteria))).build();
        } else if (filter instanceof JsonMongoDBFilterSpecification) {
            changeStreamOptions = changeStreamOptionsBuilder.filter(Document.parse(((JsonMongoDBFilterSpecification) filter).getJson())).build();
        } else if (filter instanceof BsonMongoDBFilterSpecification) {
            Bson[] aggregationStages = ((BsonMongoDBFilterSpecification) filter).getAggregationStages();
            DocumentAdapter documentAdapter = new DocumentAdapter(MongoClientSettings.getDefaultCodecRegistry());
            Document[] documents = Stream.of(aggregationStages).map(aggregationStage -> {
                final Document result;
                if (aggregationStage instanceof Document) {
                    result = (Document) aggregationStage;
                } else if (aggregationStage instanceof BsonDocument) {
                    result = documentAdapter.fromBson((BsonDocument) aggregationStage);
                } else {
                    BsonDocument bsonDocument = aggregationStage.toBsonDocument(null, MongoClientSettings.getDefaultCodecRegistry());
                    result = documentAdapter.fromBson(bsonDocument);
                }
                return result;
            }).toArray(Document[]::new);

            changeStreamOptions = changeStreamOptionsBuilder.filter(documents).build();
        } else {
            throw new IllegalArgumentException("Unrecognized " + SubscriptionFilter.class.getSimpleName() + " for MongoDB subscription");
        }
        return changeStreamOptions;
    }
}
