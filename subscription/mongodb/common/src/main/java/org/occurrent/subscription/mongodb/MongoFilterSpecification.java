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

package org.occurrent.subscription.mongodb;

import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.function.BiFunction;

import static com.mongodb.client.model.Aggregates.match;

/**
 * Add filters when subscribing to a MongoDB subscription if you're only interested in specify changes.
 * These filters will be applied at the database level so they're efficient.
 */
public class MongoFilterSpecification implements SubscriptionFilter {

    public static final String FULL_DOCUMENT = "fullDocument";

    public static class MongoJsonFilterSpecification extends MongoFilterSpecification {
        private final String json;

        public MongoJsonFilterSpecification(String json) {
            this.json = json;
        }

        public String getJson() {
            return json;
        }

        public static MongoJsonFilterSpecification filter(String json) {
            return new MongoJsonFilterSpecification(json);
        }
    }

    /**
     * Use e.g. {@link Filters} to create a bson filter. Note that MongoDB wraps the cloud event in a document called {@value FULL_DOCUMENT}
     * so you need to take this into account when creating custom filters. Note also that each filter entry must be a valid
     * <a href="https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/">aggregation stage</a>.
     * For more simple filters use the predefined filter methods such as {@link #id(BiFunction, String)} and {@link #type(BiFunction, String)}.
     * <br/>
     * <br/>
     * <p>
     * Examples:
     *
     * <pre>
     * filter().type(Filters::eq, "12345").and().data("someInt", Filters::lt, 3))
     * </pre>
     * <p>
     * which can be written like this if created manually:
     *
     * <pre>
     * filter(match(eq("fullDocument.id", "12345")), matches(lt("fullDocument.data.someInt", 3)))))
     * </pre>
     * </p>
     */
    public static class MongoBsonFilterSpecification extends MongoFilterSpecification {

        private final Bson[] aggregationStages;

        private MongoBsonFilterSpecification() {
            this.aggregationStages = new Bson[0];
        }

        public MongoBsonFilterSpecification(Bson firstAggregationStage, Bson... additionalStages) {
            this(new Bson[]{firstAggregationStage}, additionalStages);
        }

        private MongoBsonFilterSpecification(Bson[] firstAggregationStage, Bson... additionalStages) {
            this.aggregationStages = new Bson[firstAggregationStage.length + additionalStages.length];
            System.arraycopy(firstAggregationStage, 0, this.aggregationStages, 0, firstAggregationStage.length);
            System.arraycopy(additionalStages, 0, this.aggregationStages, firstAggregationStage.length, additionalStages.length);
        }

        public static MongoBsonFilterSpecification filter(Bson firstAggregationStage, Bson... additionalStages) {
            return new MongoBsonFilterSpecification(firstAggregationStage, additionalStages);
        }

        public static MongoBsonFilterSpecification filter() {
            return new MongoBsonFilterSpecification();

        }

        public MongoBsonFilterSpecification and() {
            return this;
        }

        public MongoBsonFilterSpecification id(BiFunction<String, String, Bson> filter, String value) {
            return new MongoBsonFilterSpecification(aggregationStages, matchStage(filter, "id", value));
        }

        public MongoBsonFilterSpecification type(BiFunction<String, String, Bson> filter, String value) {
            return new MongoBsonFilterSpecification(aggregationStages, matchStage(filter, "type", value));
        }

        public MongoBsonFilterSpecification source(BiFunction<String, String, Bson> filter, String value) {
            return new MongoBsonFilterSpecification(aggregationStages, matchStage(filter, "source", value));
        }

        public MongoBsonFilterSpecification subject(BiFunction<String, String, Bson> filter, String value) {
            return new MongoBsonFilterSpecification(aggregationStages, matchStage(filter, "subject", value));
        }

        public MongoBsonFilterSpecification dataSchema(BiFunction<String, String, Bson> filter, String value) {
            return new MongoBsonFilterSpecification(aggregationStages, matchStage(filter, "dataschema", value));
        }

        public MongoBsonFilterSpecification specVersion(BiFunction<String, String, Bson> filter, String value) {
            return new MongoBsonFilterSpecification(aggregationStages, matchStage(filter, "specversion", value));
        }

        public MongoBsonFilterSpecification dataContentType(BiFunction<String, String, Bson> filter, String value) {
            return new MongoBsonFilterSpecification(aggregationStages, matchStage(filter, "datacontenttype", value));
        }

        // TODO Take TimeRepresentation into account
        public MongoBsonFilterSpecification time(BiFunction<String, String, Bson> filter, String value) {
            return new MongoBsonFilterSpecification(aggregationStages, matchStage(filter, "time", value));
        }

        public MongoBsonFilterSpecification occurrentStreamId(BiFunction<String, String, Bson> filter, String value) {
            return new MongoBsonFilterSpecification(aggregationStages, matchStage(filter, "occurrentStreamId", value));
        }

        public MongoBsonFilterSpecification extension(BiFunction<String, String, Bson> filter, String propertyName, String value) {
            return new MongoBsonFilterSpecification(aggregationStages, matchStage(filter, propertyName, value));
        }

        public MongoBsonFilterSpecification data(BiFunction<String, String, Bson> filter, String propertyName, String value) {
            return new MongoBsonFilterSpecification(aggregationStages, matchStage(filter, "data." + propertyName, value));
        }

        private static Bson matchStage(BiFunction<String, String, Bson> filter, String propertyName, String value) {
            return match(filter.apply(FULL_DOCUMENT + "." + propertyName, value));
        }

        public Bson[] getAggregationStages() {
            return aggregationStages;
        }
    }
}