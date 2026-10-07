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

package org.occurrent.subscription.mongodb.spring.reactor;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel.TokenWatch;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("TokenWatch")
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelTokenWatchTest {

    @Test
    void confirms_nothing_when_a_look_reads_the_instance_the_look_before_read() {
        // Given
        TokenWatch watch = new TokenWatch();
        BsonDocument token = token("a");

        // When
        List<BsonDocument> confirmed = looks(watch, token, token, token);

        // Then
        assertThat(confirmed).containsOnlyNulls();
    }

    @Test
    void confirms_the_value_read_before_when_a_look_reads_another_instance_with_an_equal_value() {
        // Given
        TokenWatch watch = new TokenWatch();
        BsonDocument first = token("a");
        BsonDocument second = token("a");

        // When
        List<BsonDocument> confirmed = looks(watch, first, second);

        // Then
        assertThat(confirmed).containsExactly(null, token("a"));
    }

    @Test
    void confirms_the_value_read_before_when_a_look_reads_another_value() {
        // Given
        TokenWatch watch = new TokenWatch();

        // When
        List<BsonDocument> confirmed = looks(watch, token("a"), token("b"), token("c"));

        // Then
        assertThat(confirmed).containsExactly(null, token("a"), token("b"));
    }

    @Test
    void confirms_a_value_once_however_many_instances_of_it_are_read() {
        // Given
        TokenWatch watch = new TokenWatch();

        // When
        List<BsonDocument> confirmed = looks(watch, token("a"), token("a"), token("a"), token("a"));

        // Then
        assertThat(confirmed).containsExactly(null, token("a"), null, null);
    }

    @Test
    void never_confirms_the_token_read_by_the_last_look() {
        // Given
        TokenWatch watch = new TokenWatch();

        // When
        List<BsonDocument> confirmed = looks(watch, token("a"), token("b"));

        // Then
        assertThat(confirmed).doesNotContain(token("b"));
    }

    @Test
    void ignores_a_look_that_reads_no_token_and_keeps_what_it_has_seen() {
        // Given
        TokenWatch watch = new TokenWatch();

        // When
        List<BsonDocument> confirmed = looks(watch, token("a"), null, token("b"));

        // Then
        assertThat(confirmed).containsExactly(null, null, token("a"));
    }

    @Test
    void ignores_a_look_that_reads_no_token_and_keeps_what_it_has_confirmed() {
        // Given
        TokenWatch watch = new TokenWatch();

        // When
        List<BsonDocument> confirmed = looks(watch, token("a"), token("a"), null, token("a"));

        // Then
        assertThat(confirmed).containsExactly(null, token("a"), null, null);
    }

    private static List<BsonDocument> looks(TokenWatch watch, BsonDocument... tokens) {
        List<BsonDocument> confirmed = new ArrayList<>();
        for (BsonDocument token : tokens) {
            confirmed.add(watch.confirmed(token));
        }
        return confirmed;
    }

    // A new instance every time, as the driver decodes one from every reply
    private static BsonDocument token(String data) {
        return new BsonDocument("_data", new BsonString(data));
    }
}
