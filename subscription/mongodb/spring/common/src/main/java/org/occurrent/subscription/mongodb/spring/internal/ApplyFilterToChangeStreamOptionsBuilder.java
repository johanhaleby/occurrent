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

package org.occurrent.subscription.mongodb.spring.internal;

import com.mongodb.MongoClientSettings;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnsupportedSubscriptionFilterException;
import org.occurrent.subscription.mongodb.MongoFilterSpecification.MongoJsonFilterSpecification;
import org.occurrent.subscription.mongodb.MongoFilterSpecification;
import org.occurrent.subscription.mongodb.internal.DcbSubscriptionFilterConverter;
import org.occurrent.subscription.mongodb.internal.DocumentAdapter;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.stream.Stream;

import static org.occurrent.mongodb.spring.filterqueryconversion.internal.FilterConverter.convertFilterToCriteria;
import static org.occurrent.subscription.mongodb.MongoFilterSpecification.FULL_DOCUMENT;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;

@NullMarked
public class ApplyFilterToChangeStreamOptionsBuilder {

    public static ChangeStreamOptions applyFilter(TimeRepresentation timeRepresentation, @Nullable SubscriptionFilter filter, ChangeStreamOptionsBuilder changeStreamOptionsBuilder) {
        final ChangeStreamOptions changeStreamOptions;
        if (filter == null) {
            changeStreamOptions = changeStreamOptionsBuilder.build();
        } else if (filter instanceof StreamSubscriptionFilter streamSubscriptionFilter) {
            Filter streamFilter = streamSubscriptionFilter.filter();
            Criteria criteria = convertFilterToCriteria(FULL_DOCUMENT, timeRepresentation, streamFilter);
            changeStreamOptions = changeStreamOptionsBuilder.filter(newAggregation(match(criteria))).build();
        } else if (filter instanceof AgnosticSubscriptionFilter agnosticSubscriptionFilter) {
            // Capability-agnostic: the change stream applies the plain Filter, the same as a stream filter. The stream
            // versus DCB scoping lives in the catch-up layer, not here.
            Filter agnosticFilter = agnosticSubscriptionFilter.filter();
            Criteria criteria = convertFilterToCriteria(FULL_DOCUMENT, timeRepresentation, agnosticFilter);
            changeStreamOptions = changeStreamOptionsBuilder.filter(newAggregation(match(criteria))).build();
        } else if (filter instanceof DcbSubscriptionFilter dcbSubscriptionFilter) {
            Document matchStage = DcbSubscriptionFilterConverter.toChangeStreamMatchStage(dcbSubscriptionFilter.criteria());
            changeStreamOptions = changeStreamOptionsBuilder.filter(matchStage).build();
        } else if (filter instanceof MongoJsonFilterSpecification jsonFilterSpecification) {
            changeStreamOptions = changeStreamOptionsBuilder.filter(Document.parse(jsonFilterSpecification.getJson())).build();
        } else if (filter instanceof MongoFilterSpecification.MongoBsonFilterSpecification bsonFilterSpecification) {
            Bson[] aggregationStages = bsonFilterSpecification.getAggregationStages();
            DocumentAdapter documentAdapter = new DocumentAdapter(MongoClientSettings.getDefaultCodecRegistry());
            Document[] documents = Stream.of(aggregationStages).map(aggregationStage -> {
                return switch (aggregationStage) {
                    case Document document -> document;
                    case BsonDocument bsonDocument -> documentAdapter.fromBson(bsonDocument);
                    default -> {
                        BsonDocument bsonDocument = aggregationStage.toBsonDocument(null, MongoClientSettings.getDefaultCodecRegistry());
                        yield documentAdapter.fromBson(bsonDocument);
                    }
                };
            }).toArray(Document[]::new);

            changeStreamOptions = changeStreamOptionsBuilder.filter(documents).build();
        } else {
            throw new UnsupportedSubscriptionFilterException(filter.getClass(), "Unrecognized " + SubscriptionFilter.class.getSimpleName() + " for MongoDB subscription");
        }
        return changeStreamOptions;
    }
}
