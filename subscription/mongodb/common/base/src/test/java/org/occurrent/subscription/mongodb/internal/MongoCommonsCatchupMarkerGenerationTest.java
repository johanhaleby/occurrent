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

package org.occurrent.subscription.mongodb.internal;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.random.RandomGenerator;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoCommonsCatchupMarkerGenerationTest {

    @Test
    void a_generation_is_the_first_value_the_random_generator_gives_when_it_is_not_0() {
        assertThat(MongoCommons.newCatchupMarkerGeneration(returning(Long.MIN_VALUE))).isEqualTo(Long.MIN_VALUE);
        assertThat(MongoCommons.newCatchupMarkerGeneration(returning(-1))).isEqualTo(-1);
    }

    @Test
    void a_generation_skips_every_0_the_random_generator_gives() {
        assertThat(MongoCommons.newCatchupMarkerGeneration(returning(0, 0, 7, 9))).isEqualTo(7);
    }

    @Test
    void setting_a_generation_sets_the_generation_field_to_it_as_a_long() {
        BsonDocument update = MongoCommons.setCatchupMarkerGeneration(7).toBsonDocument();

        assertThat(update).isEqualTo(new BsonDocument("$set", new BsonDocument(MongoCommons.CATCHUP_MARKER_GENERATION, new BsonInt64(7))));
    }

    private static RandomGenerator returning(long... values) {
        Iterator<Long> iterator = Arrays.stream(values).boxed().iterator();
        return iterator::next;
    }
}
