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
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;

import java.time.Duration;
import java.util.Date;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    private static final BsonTimestamp HELLO_START = new BsonTimestamp(1_700_000_020, 0);
    private static final Duration MAX_AGE = Duration.ofSeconds(15);

    @Test
    void start_of_is_the_hello_start_when_no_cluster_time_is_known() {
        BsonTimestamp result = MongoCommons.startOf(HELLO_START, null, MAX_AGE);

        assertThat(result).isEqualTo(HELLO_START);
    }

    @Test
    void start_of_is_the_hello_start_when_the_known_cluster_time_is_more_than_max_age_older() {
        BsonTimestamp result = MongoCommons.startOf(HELLO_START, new BsonTimestamp(1_700_000_004, 9), MAX_AGE);

        assertThat(result).isEqualTo(HELLO_START);
    }

    @Test
    void start_of_is_just_after_the_known_cluster_time_when_it_is_exactly_max_age_older() {
        BsonTimestamp result = MongoCommons.startOf(HELLO_START, new BsonTimestamp(1_700_000_005, 9), MAX_AGE);

        assertThat(result).isEqualTo(new BsonTimestamp(1_700_000_005, 10));
    }

    @Test
    void start_of_is_just_after_a_fresh_known_cluster_time_that_is_before_the_hello_start() {
        BsonTimestamp result = MongoCommons.startOf(HELLO_START, new BsonTimestamp(1_700_000_017, 3), MAX_AGE);

        assertThat(result).isEqualTo(new BsonTimestamp(1_700_000_017, 4));
    }

    @Test
    void start_of_moves_to_the_next_second_when_the_known_increment_is_at_its_maximum() {
        BsonTimestamp result = MongoCommons.startOf(HELLO_START, new BsonTimestamp(1_700_000_017, Integer.MAX_VALUE), MAX_AGE);

        assertThat(result).isEqualTo(new BsonTimestamp(1_700_000_018, 0));
    }

    @Test
    void start_of_is_never_later_than_the_hello_start_when_the_known_cluster_time_is_ahead_of_it() {
        BsonTimestamp result = MongoCommons.startOf(HELLO_START, new BsonTimestamp(1_700_000_025, 1), MAX_AGE);

        assertThat(result).isEqualTo(HELLO_START);
    }

    @Test
    void start_of_is_the_hello_start_when_just_after_the_known_cluster_time_is_the_hello_start() {
        BsonTimestamp result = MongoCommons.startOf(new BsonTimestamp(1_700_000_020, 5), new BsonTimestamp(1_700_000_020, 4), MAX_AGE);

        assertThat(result).isEqualTo(new BsonTimestamp(1_700_000_020, 5));
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

    @Test
    void a_catch_up_position_keeps_its_live_start_out_of_the_fields_a_change_stream_position_is_read_from() {
        MongoOperationTimeCheckpoint liveStart = new MongoOperationTimeCheckpoint(new BsonTimestamp(1735689600, 1));
        GlobalCheckpoint checkpoint = GlobalCheckpoint.of(42, liveStart, 7, 40);

        Document document = MongoCommons.generateCheckpointDocument("subscription-1", checkpoint);

        assertThat(document.getString(MongoCommons.GENERIC_CHECKPOINT)).isEqualTo("position:42");
        assertThat(document).doesNotContainKeys(MongoCommons.OPERATION_TIME, MongoCommons.RESUME_TOKEN);
        assertThat(document.get(MongoCommons.CATCHUP_LIVE_FROM, Document.class)).isEqualTo(new Document(MongoCommons.OPERATION_TIME, liveStart.operationTime));
        assertThat(document.get(MongoCommons.CATCHUP_REPLAY_ORIGIN)).isEqualTo(7L);
        assertThat(document.get(MongoCommons.CATCHUP_REPLAY_TO)).isEqualTo(40L);
        assertThat(MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document))).isEqualTo(checkpoint);
    }

    @Test
    void a_catch_up_position_stored_with_a_live_start_but_no_replay_end_is_read_back_as_a_plain_position() {
        Document document = MongoCommons.generateCheckpointDocument("subscription-1", GlobalCheckpoint.of(42, new MongoOperationTimeCheckpoint(new BsonTimestamp(1735689600, 1)), 7, 40));
        document.remove(MongoCommons.CATCHUP_REPLAY_TO);

        assertThat(MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document))).isEqualTo(new StringBasedCheckpoint("position:42"));
    }

    @Test
    void a_catch_up_position_read_back_as_a_string_is_stored_the_same_way() {
        MongoResumeTokenCheckpoint liveStart = new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("82ABCDEF")));
        GlobalCheckpoint checkpoint = GlobalCheckpoint.of(42, liveStart, 7, 40);

        Document document = MongoCommons.generateCheckpointDocument("subscription-1", new StringBasedCheckpoint(checkpoint.asString()));

        assertThat(document).doesNotContainKeys(MongoCommons.OPERATION_TIME, MongoCommons.RESUME_TOKEN);
        Checkpoint readBack = MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document));
        assertThat(readBack).isEqualTo(checkpoint);
        assertThat(((GlobalCheckpoint) readBack).liveFrom()).containsInstanceOf(MongoResumeTokenCheckpoint.class);
    }

    @Test
    void a_catch_up_position_without_a_live_start_is_stored_as_before() {
        Document document = MongoCommons.generateCheckpointDocument("subscription-1", GlobalCheckpoint.of(42));

        assertThat(document.getString(MongoCommons.GENERIC_CHECKPOINT)).isEqualTo("position:42");
        assertThat(document).doesNotContainKeys(MongoCommons.CATCHUP_LIVE_FROM, MongoCommons.CATCHUP_REPLAY_ORIGIN, MongoCommons.CATCHUP_REPLAY_TO);
        assertThat(MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document))).isEqualTo(new StringBasedCheckpoint("position:42"));
    }

    @Test
    void a_change_stream_given_a_catch_up_position_opens_at_the_present_whether_or_not_it_has_a_live_start() {
        GlobalCheckpoint withLiveStart = GlobalCheckpoint.of(42, new MongoOperationTimeCheckpoint(new BsonTimestamp(1735689600, 1)), 7, 40);

        assertThat(MongoCommons.opensAtThePresent(StartAt.checkpoint(GlobalCheckpoint.of(42)))).isTrue();
        assertThat(MongoCommons.opensAtThePresent(StartAt.checkpoint(withLiveStart))).isTrue();
        assertThat(MongoCommons.opensAtThePresent(StartAt.checkpoint(new StringBasedCheckpoint(withLiveStart.asString())))).isTrue();
    }

    @Test
    void a_catch_up_time_with_a_resume_token_live_start_keeps_the_live_start_out_of_the_fields_a_change_stream_position_is_read_from() {
        MongoResumeTokenCheckpoint liveStart = new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("82ABCDEF")));
        CatchupTimeCheckpoint checkpoint = CatchupTimeCheckpoint.of("2026-01-01T10:00:05.123Z", liveStart, "2026-01-01T10:00:00Z");

        Document document = MongoCommons.generateCheckpointDocument("subscription-1", checkpoint);

        assertThat(document.getString(MongoCommons.GENERIC_CHECKPOINT)).isEqualTo("2026-01-01T10:00:05.123Z");
        assertThat(document).doesNotContainKeys(MongoCommons.OPERATION_TIME, MongoCommons.RESUME_TOKEN, MongoCommons.CATCHUP_REPLAY_TO);
        Document liveFrom = document.get(MongoCommons.CATCHUP_LIVE_FROM, Document.class);
        assertThat(liveFrom).isEqualTo(new Document(MongoCommons.RESUME_TOKEN, liveStart.resumeToken));
        assertThat(liveFrom).doesNotContainKey("_id");
        assertThat(document.get(MongoCommons.CATCHUP_REPLAY_ORIGIN)).isEqualTo("2026-01-01T10:00:00Z");
        Checkpoint readBack = MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document));
        assertThat(readBack).isEqualTo(checkpoint);
        assertThat(((CatchupTimeCheckpoint) readBack).liveFrom()).isInstanceOf(MongoResumeTokenCheckpoint.class);
    }

    @Test
    void a_catch_up_time_with_an_operation_time_live_start_keeps_the_live_start_out_of_the_fields_a_change_stream_position_is_read_from() {
        MongoOperationTimeCheckpoint liveStart = new MongoOperationTimeCheckpoint(new BsonTimestamp(1735689600, 1));
        CatchupTimeCheckpoint checkpoint = CatchupTimeCheckpoint.of("2026-01-01T10:00:05.123Z", liveStart, "1970-01-01T00:00:00Z");

        Document document = MongoCommons.generateCheckpointDocument("subscription-1", checkpoint);

        assertThat(document.getString(MongoCommons.GENERIC_CHECKPOINT)).isEqualTo("2026-01-01T10:00:05.123Z");
        assertThat(document).doesNotContainKeys(MongoCommons.OPERATION_TIME, MongoCommons.RESUME_TOKEN, MongoCommons.CATCHUP_REPLAY_TO);
        Document liveFrom = document.get(MongoCommons.CATCHUP_LIVE_FROM, Document.class);
        assertThat(liveFrom).isEqualTo(new Document(MongoCommons.OPERATION_TIME, liveStart.operationTime));
        assertThat(liveFrom).doesNotContainKey("_id");
        assertThat(document.get(MongoCommons.CATCHUP_REPLAY_ORIGIN)).isEqualTo("1970-01-01T00:00:00Z");
        Checkpoint readBack = MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document));
        assertThat(readBack).isEqualTo(checkpoint);
        assertThat(((CatchupTimeCheckpoint) readBack).liveFrom()).isInstanceOf(MongoOperationTimeCheckpoint.class);
    }

    @Test
    void a_catch_up_time_read_back_as_a_string_is_stored_the_same_way() {
        MongoResumeTokenCheckpoint liveStart = new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("82ABCDEF")));
        CatchupTimeCheckpoint checkpoint = CatchupTimeCheckpoint.of("2026-01-01T10:00:05.123Z", liveStart, "2026-01-01T10:00:00Z");

        Document document = MongoCommons.generateCheckpointDocument("subscription-1", new StringBasedCheckpoint(checkpoint.asString()));

        assertThat(document).doesNotContainKeys(MongoCommons.OPERATION_TIME, MongoCommons.RESUME_TOKEN, MongoCommons.CATCHUP_REPLAY_TO);
        assertThat(document.getString(MongoCommons.GENERIC_CHECKPOINT)).isEqualTo("2026-01-01T10:00:05.123Z");
        Checkpoint readBack = MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document));
        assertThat(readBack).isEqualTo(checkpoint);
        assertThat(((CatchupTimeCheckpoint) readBack).liveFrom()).isInstanceOf(MongoResumeTokenCheckpoint.class);
    }

    @Test
    void a_time_without_a_live_start_is_stored_as_before() {
        Document document = MongoCommons.generateCheckpointDocument("subscription-1", new StringBasedCheckpoint("2026-01-01T10:00:05.123Z"));

        assertThat(document.getString(MongoCommons.GENERIC_CHECKPOINT)).isEqualTo("2026-01-01T10:00:05.123Z");
        assertThat(document).doesNotContainKeys(MongoCommons.CATCHUP_LIVE_FROM, MongoCommons.CATCHUP_REPLAY_ORIGIN, MongoCommons.CATCHUP_REPLAY_TO);
        assertThat(MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document))).isEqualTo(new StringBasedCheckpoint("2026-01-01T10:00:05.123Z"));
    }

    @Test
    void a_time_stored_with_a_live_start_and_a_replay_end_is_read_back_as_a_plain_time() {
        // Only a position catch-up stores a replay end, so a document holding both is read as the time it stores
        Document document = MongoCommons.generateCheckpointDocument("subscription-1",
                CatchupTimeCheckpoint.of("2026-01-01T10:00:05.123Z", new MongoOperationTimeCheckpoint(new BsonTimestamp(1735689600, 1)), "2026-01-01T10:00:00Z"));
        document.put(MongoCommons.CATCHUP_REPLAY_TO, 40L);

        assertThat(MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document))).isEqualTo(new StringBasedCheckpoint("2026-01-01T10:00:05.123Z"));
    }

    @Test
    void a_time_stored_with_a_live_start_but_a_numeric_replay_origin_is_read_back_as_a_plain_time() {
        Document document = MongoCommons.generateCheckpointDocument("subscription-1",
                CatchupTimeCheckpoint.of("2026-01-01T10:00:05.123Z", new MongoOperationTimeCheckpoint(new BsonTimestamp(1735689600, 1)), "2026-01-01T10:00:00Z"));
        document.put(MongoCommons.CATCHUP_REPLAY_ORIGIN, 7L);

        assertThat(MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document))).isEqualTo(new StringBasedCheckpoint("2026-01-01T10:00:05.123Z"));
    }

    @Test
    void a_time_stored_with_a_replay_origin_but_no_live_start_is_read_back_as_a_plain_time() {
        Document document = MongoCommons.generateCheckpointDocument("subscription-1",
                CatchupTimeCheckpoint.of("2026-01-01T10:00:05.123Z", new MongoOperationTimeCheckpoint(new BsonTimestamp(1735689600, 1)), "2026-01-01T10:00:00Z"));
        document.remove(MongoCommons.CATCHUP_LIVE_FROM);

        assertThat(MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document))).isEqualTo(new StringBasedCheckpoint("2026-01-01T10:00:05.123Z"));
    }

    @Test
    void a_position_stored_with_a_live_start_and_a_time_as_replay_origin_is_read_back_as_a_plain_position() {
        // The fields of a time catch-up beside a position, which no version writes
        Document document = MongoCommons.generateGenericCheckpointDocument("subscription-1", "position:42");
        document.put(MongoCommons.CATCHUP_LIVE_FROM, new Document(MongoCommons.OPERATION_TIME, new BsonTimestamp(1735689600, 1)));
        document.put(MongoCommons.CATCHUP_REPLAY_ORIGIN, "2026-01-01T10:00:00Z");

        assertThat(MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document))).isEqualTo(new StringBasedCheckpoint("position:42"));
    }

    @Test
    void a_position_stored_with_a_live_start_and_a_time_as_replay_origin_and_a_replay_end_is_read_back_as_a_plain_position() {
        Document document = MongoCommons.generateGenericCheckpointDocument("subscription-1", "position:42");
        document.put(MongoCommons.CATCHUP_LIVE_FROM, new Document(MongoCommons.OPERATION_TIME, new BsonTimestamp(1735689600, 1)));
        document.put(MongoCommons.CATCHUP_REPLAY_ORIGIN, "2026-01-01T10:00:00Z");
        document.put(MongoCommons.CATCHUP_REPLAY_TO, 40L);

        assertThat(MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document))).isEqualTo(new StringBasedCheckpoint("position:42"));
    }

    @Test
    void a_time_stored_with_a_live_start_and_a_replay_origin_but_a_value_that_is_not_a_time_fails_when_read() {
        Document document = MongoCommons.generateGenericCheckpointDocument("subscription-1", "garbage");
        document.put(MongoCommons.CATCHUP_LIVE_FROM, new Document(MongoCommons.OPERATION_TIME, new BsonTimestamp(1735689600, 1)));
        document.put(MongoCommons.CATCHUP_REPLAY_ORIGIN, "2026-01-01T10:00:00Z");

        assertThatThrownBy(() -> MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void a_time_stored_with_a_live_start_and_a_replay_origin_that_is_not_a_time_fails_when_read() {
        Document document = MongoCommons.generateGenericCheckpointDocument("subscription-1", "2026-01-01T10:00:05.123Z");
        document.put(MongoCommons.CATCHUP_LIVE_FROM, new Document(MongoCommons.OPERATION_TIME, new BsonTimestamp(1735689600, 1)));
        document.put(MongoCommons.CATCHUP_REPLAY_ORIGIN, "garbage");

        assertThatThrownBy(() -> MongoCommons.calculateCheckpointFromMongoStreamPositionDocument(asStored(document))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void a_change_stream_given_a_catch_up_time_opens_at_the_present() {
        CatchupTimeCheckpoint checkpoint = CatchupTimeCheckpoint.of("2026-01-01T10:00:05.123Z", new MongoOperationTimeCheckpoint(new BsonTimestamp(1735689600, 1)), "2026-01-01T10:00:00Z");

        assertThat(MongoCommons.opensAtThePresent(StartAt.checkpoint(checkpoint))).isTrue();
        assertThat(MongoCommons.opensAtThePresent(StartAt.checkpoint(new StringBasedCheckpoint(checkpoint.asString())))).isTrue();
    }

    // What a storage reads back once MongoDB has the document, where a nested BsonDocument comes back as a Document
    private static Document asStored(Document document) {
        return Document.parse(document.toJson());
    }

    private static Checkpoint checkpointOf(StartAt startAt) {
        return ((StartAt.StartAtCheckpoint) startAt).checkpoint;
    }
}
