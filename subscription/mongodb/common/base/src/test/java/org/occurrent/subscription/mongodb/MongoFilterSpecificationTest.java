/*
 * Copyright 2026 Johan Haleby
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
import org.bson.Document;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.mongodb.MongoFilterSpecification.MongoBsonFilterSpecification;
import org.occurrent.subscription.mongodb.MongoFilterSpecification.MongoJsonFilterSpecification;

import static com.mongodb.client.model.Aggregates.match;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoFilterSpecificationTest {

    @Test
    void two_json_filters_built_from_the_same_json_are_equal() {
        MongoJsonFilterSpecification filter = MongoJsonFilterSpecification.filter("{ $match : { \"fullDocument.type\" : \"t1\" } }");
        MongoJsonFilterSpecification sameJson = MongoJsonFilterSpecification.filter("{ $match : { \"fullDocument.type\" : \"t1\" } }");
        MongoJsonFilterSpecification anotherJson = MongoJsonFilterSpecification.filter("{ $match : { \"fullDocument.type\" : \"t2\" } }");

        assertThat(filter).isEqualTo(sameJson).hasSameHashCodeAs(sameJson).isNotEqualTo(anotherJson);
    }

    @Test
    void two_bson_filters_built_the_same_way_are_equal() {
        MongoBsonFilterSpecification filter = MongoBsonFilterSpecification.filter().type(Filters::eq, "t1").and().data(Filters::lt, "someInt", "3");
        MongoBsonFilterSpecification builtTheSameWay = MongoBsonFilterSpecification.filter().type(Filters::eq, "t1").and().data(Filters::lt, "someInt", "3");
        MongoBsonFilterSpecification anotherType = MongoBsonFilterSpecification.filter().type(Filters::eq, "t2").and().data(Filters::lt, "someInt", "3");
        MongoBsonFilterSpecification oneStageLess = MongoBsonFilterSpecification.filter().type(Filters::eq, "t1");

        assertThat(filter).isEqualTo(builtTheSameWay).hasSameHashCodeAs(builtTheSameWay).isNotEqualTo(anotherType).isNotEqualTo(oneStageLess);
    }

    @Test
    void two_bson_filters_built_from_equal_stages_of_your_own_are_equal() {
        MongoBsonFilterSpecification filter = MongoBsonFilterSpecification.filter(match(new Document("fullDocument.type", "t1")));
        MongoBsonFilterSpecification equalStages = MongoBsonFilterSpecification.filter(match(new Document("fullDocument.type", "t1")));

        assertThat(filter).isEqualTo(equalStages).hasSameHashCodeAs(equalStages);
    }

    @Test
    void a_json_filter_and_a_bson_filter_are_not_equal() {
        assertThat((Object) MongoJsonFilterSpecification.filter("{}")).isNotEqualTo(MongoBsonFilterSpecification.filter());
    }
}
