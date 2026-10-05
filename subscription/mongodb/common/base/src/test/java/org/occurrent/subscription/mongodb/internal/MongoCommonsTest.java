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
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;

import java.util.Date;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoCommonsTest {

    @Test
    void extract_write_version_returns_empty_when_document_is_null() {
        OptionalLong result = MongoCommons.extractWriteVersion(null);

        assertThat(result).isEmpty();
    }

    @Test
    void extract_write_version_returns_empty_when_key_is_absent() {
        Document document = new Document("_id", "subscription-1");

        OptionalLong result = MongoCommons.extractWriteVersion(document);

        assertThat(result).isEmpty();
    }

    @Test
    void extract_write_version_returns_empty_when_value_is_null() {
        Document document = new Document("_id", "subscription-1")
                .append(MongoCommons.WRITE_VERSION, null);

        OptionalLong result = MongoCommons.extractWriteVersion(document);

        assertThat(result).isEmpty();
    }

    @Test
    void extract_write_version_returns_the_value_when_present() {
        Document document = new Document("_id", "subscription-1")
                .append(MongoCommons.WRITE_VERSION, 42L);

        OptionalLong result = MongoCommons.extractWriteVersion(document);

        assertThat(result).hasValue(42L);
    }

    private static final SubscriptionModelContext CONTEXT = new SubscriptionModelContext(MongoCommonsTest.class);
    private static final BsonTimestamp OPERATION_TIME = new BsonTimestamp(1_700_000_000, 7);
    private static final StartAt RESUME_TOKEN_POSITION = StartAt.checkpoint(new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("8263"))));

    @Test
    void operation_time_after_is_one_increment_past_the_reply_operation_time() {
        Document reply = new Document("ok", 1.0).append(MongoCommons.OPERATION_TIME, OPERATION_TIME);

        BsonTimestamp result = MongoCommons.operationTimeAfter(reply);

        assertThat(result).isEqualTo(new BsonTimestamp(1_700_000_000, 8));
    }

    @Test
    void operation_time_after_is_null_when_the_reply_carries_none() {
        BsonTimestamp result = MongoCommons.operationTimeAfter(new Document("ok", 1.0));

        assertThat(result).isNull();
    }

    @Test
    void operation_time_as_of_is_the_start_of_the_second_the_server_clock_showed_the_elapsed_time_before_the_reply() {
        // 1_700_000_001.200 at the reply, 150 ms after the moment, so 1_700_000_001.049 at the moment
        Document reply = new Document("ok", 1.0).append("localTime", new Date(1_700_000_001_200L));

        BsonTimestamp result = MongoCommons.operationTimeAsOf(reply, TimeUnit.MILLISECONDS.toNanos(150));

        assertThat(result).isEqualTo(new BsonTimestamp(1_700_000_001, 0));
    }

    @Test
    void operation_time_as_of_falls_in_the_previous_second_when_the_elapsed_time_crosses_a_second() {
        Document reply = new Document("ok", 1.0).append("localTime", new Date(1_700_000_001_200L));

        BsonTimestamp result = MongoCommons.operationTimeAsOf(reply, TimeUnit.MILLISECONDS.toNanos(200));

        assertThat(result).isEqualTo(new BsonTimestamp(1_700_000_000, 0));
    }

    @Test
    void operation_time_as_of_is_null_when_the_reply_has_no_local_time() {
        BsonTimestamp result = MongoCommons.operationTimeAsOf(new Document("ok", 1.0), 0);

        assertThat(result).isNull();
    }

    @Test
    void resolving_the_present_pins_it_to_the_operation_time_before_the_stream_opens() {
        AtomicReference<StartAt> currentStartAt = new AtomicReference<>(StartAt.now());

        StartAt opening = MongoCommons.resolveOpeningPosition(currentStartAt, CONTEXT, () -> OPERATION_TIME);

        assertThat(checkpointOf(opening)).isEqualTo(new MongoOperationTimeCheckpoint(OPERATION_TIME));
        assertThat(checkpointOf(currentStartAt.get())).isEqualTo(new MongoOperationTimeCheckpoint(OPERATION_TIME));
    }

    @Test
    void resolving_a_pinned_position_again_asks_the_server_nothing_and_opens_where_it_first_opened() {
        AtomicReference<StartAt> currentStartAt = new AtomicReference<>(StartAt.subscriptionModelDefault());
        StartAt firstOpening = MongoCommons.resolveOpeningPosition(currentStartAt, CONTEXT, () -> OPERATION_TIME);
        AtomicInteger asked = new AtomicInteger();

        StartAt secondOpening = MongoCommons.resolveOpeningPosition(currentStartAt, CONTEXT, () -> {
            asked.incrementAndGet();
            return new BsonTimestamp(1_800_000_000, 1);
        });

        assertThat(checkpointOf(secondOpening)).isEqualTo(checkpointOf(firstOpening));
        assertThat(asked).hasValue(0);
    }

    @Test
    void resolving_leaves_a_resume_token_alone_and_asks_the_server_nothing() {
        AtomicReference<StartAt> currentStartAt = new AtomicReference<>(RESUME_TOKEN_POSITION);
        AtomicInteger asked = new AtomicInteger();

        StartAt opening = MongoCommons.resolveOpeningPosition(currentStartAt, CONTEXT, () -> {
            asked.incrementAndGet();
            return OPERATION_TIME;
        });

        assertThat(opening).isSameAs(RESUME_TOKEN_POSITION);
        assertThat(currentStartAt.get()).isSameAs(RESUME_TOKEN_POSITION);
        assertThat(asked).hasValue(0);
    }

    @Test
    void a_checkpoint_set_while_the_server_is_asked_wins_over_the_pin() {
        AtomicReference<StartAt> currentStartAt = new AtomicReference<>(StartAt.now());

        StartAt opening = MongoCommons.resolveOpeningPosition(currentStartAt, CONTEXT, () -> {
            currentStartAt.set(RESUME_TOKEN_POSITION);
            return OPERATION_TIME;
        });

        assertThat(opening).isSameAs(RESUME_TOKEN_POSITION);
        assertThat(currentStartAt.get()).isSameAs(RESUME_TOKEN_POSITION);
    }

    @Test
    void a_checkpoint_this_model_does_not_recognize_is_pinned_like_the_present() {
        AtomicReference<StartAt> currentStartAt = new AtomicReference<>(StartAt.checkpoint(new StringBasedCheckpoint("somewhere-else")));

        StartAt opening = MongoCommons.resolveOpeningPosition(currentStartAt, CONTEXT, () -> OPERATION_TIME);

        assertThat(checkpointOf(opening)).isEqualTo(new MongoOperationTimeCheckpoint(OPERATION_TIME));
    }

    @Test
    void no_operation_time_leaves_the_position_unpinned_and_opens_at_the_present() {
        StartAt now = StartAt.now();
        AtomicReference<StartAt> currentStartAt = new AtomicReference<>(now);

        StartAt opening = MongoCommons.resolveOpeningPosition(currentStartAt, CONTEXT, () -> null);

        assertThat(opening.isNow()).isTrue();
        assertThat(currentStartAt.get()).isSameAs(now);
    }

    @Test
    void a_pinned_dynamic_position_still_answers_its_own_checkpoint_once_it_has_one() {
        AtomicReference<StartAt> answer = new AtomicReference<>(StartAt.subscriptionModelDefault());
        AtomicReference<StartAt> currentStartAt = new AtomicReference<>(StartAt.dynamic(answer::get));
        StartAt firstOpening = MongoCommons.resolveOpeningPosition(currentStartAt, CONTEXT, () -> OPERATION_TIME);

        answer.set(RESUME_TOKEN_POSITION);
        StartAt secondOpening = MongoCommons.resolveOpeningPosition(currentStartAt, CONTEXT, () -> OPERATION_TIME);

        assertThat(checkpointOf(firstOpening)).isEqualTo(new MongoOperationTimeCheckpoint(OPERATION_TIME));
        assertThat(secondOpening).isSameAs(RESUME_TOKEN_POSITION);
    }

    @Test
    void a_pinned_dynamic_position_answers_the_pin_where_it_would_have_answered_the_present() {
        AtomicReference<StartAt> currentStartAt = new AtomicReference<>(StartAt.dynamic(StartAt::now));
        StartAt firstOpening = MongoCommons.resolveOpeningPosition(currentStartAt, CONTEXT, () -> OPERATION_TIME);

        StartAt secondOpening = MongoCommons.resolveOpeningPosition(currentStartAt, CONTEXT, () -> new BsonTimestamp(1_800_000_000, 1));

        assertThat(checkpointOf(secondOpening)).isEqualTo(checkpointOf(firstOpening));
    }

    private static Checkpoint checkpointOf(StartAt startAt) {
        return ((StartAt.StartAtCheckpoint) startAt).checkpoint;
    }
}
