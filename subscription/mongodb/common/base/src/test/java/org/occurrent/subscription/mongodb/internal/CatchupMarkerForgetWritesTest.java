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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.CATCHUP_MARKER_FORGET_WRITE_CONCERN;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.CATCHUP_MARKER_FORGET_WRITE_CONCERN_WITHOUT_JOURNAL;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupMarkerForgetWritesTest {

    private static final ServerAddress ADDRESS = new ServerAddress();

    private final CatchupMarkerForgetWrites writes = new CatchupMarkerForgetWrites();
    private final List<WriteConcern> sent = new ArrayList<>();
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final Logger logger = (Logger) LoggerFactory.getLogger(CatchupMarkerForgetWrites.class);

    @BeforeEach
    void capture_the_log_of_the_writes() {
        logged.start();
        logger.addAppender(logged);
    }

    @AfterEach
    void stop_capturing_the_log_of_the_writes() {
        logger.detachAppender(logged);
        logged.stop();
    }

    @Test
    void a_write_is_sent_with_journal_and_nothing_else_when_the_server_accepts_it() {
        writes.write(sent::add);
        writes.write(sent::add);

        assertThat(sent).containsExactly(CATCHUP_MARKER_FORGET_WRITE_CONCERN, CATCHUP_MARKER_FORGET_WRITE_CONCERN);
        assertThat(CATCHUP_MARKER_FORGET_WRITE_CONCERN.getJournal()).isTrue();
    }

    @Test
    void a_write_the_server_refuses_for_lack_of_journaling_is_sent_again_without_journal_on_the_same_call() {
        writes.write(refusingJournal(commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE)));

        assertThat(sent).containsExactly(CATCHUP_MARKER_FORGET_WRITE_CONCERN, CATCHUP_MARKER_FORGET_WRITE_CONCERN_WITHOUT_JOURNAL);
        assertThat(CATCHUP_MARKER_FORGET_WRITE_CONCERN_WITHOUT_JOURNAL.getJournal()).isNull();
    }

    @Test
    void after_the_first_refusal_every_later_write_skips_journal() {
        writes.write(refusingJournal(commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE)));
        sent.clear();

        writes.write(refusingJournal(commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE)));
        writes.write(refusingJournal(commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE)));

        assertThat(sent).containsExactly(CATCHUP_MARKER_FORGET_WRITE_CONCERN_WITHOUT_JOURNAL, CATCHUP_MARKER_FORGET_WRITE_CONCERN_WITHOUT_JOURNAL);
    }

    @Test
    void a_refusal_wrapped_as_the_cause_of_another_exception_also_falls_back() {
        RuntimeException translated = new RuntimeException("translated", commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE));

        writes.write(refusingJournal(translated));

        assertThat(sent).containsExactly(CATCHUP_MARKER_FORGET_WRITE_CONCERN, CATCHUP_MARKER_FORGET_WRITE_CONCERN_WITHOUT_JOURNAL);
    }

    @Test
    void error_code_2_with_another_message_is_rethrown_as_the_same_instance_and_the_next_write_still_asks_for_journal() {
        MongoCommandException failure = commandFailure(2, "unrelated bad value");

        assertThatThrownBy(() -> writes.write(refusingJournal(failure))).isSameAs(failure);
        sent.clear();
        writes.write(sent::add);

        assertThat(sent).containsExactly(CATCHUP_MARKER_FORGET_WRITE_CONCERN);
    }

    @Test
    void the_journal_message_with_another_error_code_is_rethrown_as_the_same_instance() {
        MongoCommandException failure = commandFailure(50, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE);

        assertThatThrownBy(() -> writes.write(refusingJournal(failure))).isSameAs(failure);

        assertThat(sent).containsExactly(CATCHUP_MARKER_FORGET_WRITE_CONCERN);
    }

    @Test
    void a_failure_of_the_write_without_journal_is_not_swallowed() {
        IllegalStateException failure = new IllegalStateException("boom");
        Consumer<WriteConcern> alwaysFailing = writeConcern -> {
            sent.add(writeConcern);
            if (writeConcern.getJournal() == null) {
                throw failure;
            }
            throw commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE);
        };

        assertThatThrownBy(() -> writes.write(alwaysFailing)).isSameAs(failure);
    }

    @Test
    void the_first_refusal_for_lack_of_journaling_logs_exactly_one_warning_that_names_journaling() {
        writes.write(refusingJournal(commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE)));

        assertThat(warnings()).hasSize(1);
        assertThat(warnings().get(0).getFormattedMessage()).containsIgnoringCase("journaling");
    }

    @Test
    void writes_after_the_fallback_log_no_further_warning() {
        writes.write(refusingJournal(commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE)));
        writes.write(refusingJournal(commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE)));
        writes.write(refusingJournal(commandFailure(2, JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE)));

        assertThat(warnings()).hasSize(1);
    }

    @Test
    void error_code_2_with_another_message_logs_no_warning() {
        MongoCommandException failure = commandFailure(2, "unrelated bad value");

        assertThatThrownBy(() -> writes.write(refusingJournal(failure))).isSameAs(failure);

        assertThat(warnings()).isEmpty();
    }

    private List<ILoggingEvent> warnings() {
        return logged.list.stream().filter(event -> event.getLevel() == Level.WARN).collect(Collectors.toList());
    }

    private Consumer<WriteConcern> refusingJournal(RuntimeException refusal) {
        return writeConcern -> {
            sent.add(writeConcern);
            if (Boolean.TRUE.equals(writeConcern.getJournal())) {
                throw refusal;
            }
        };
    }

    private static MongoCommandException commandFailure(int code, String message) {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(code))
                .append("errmsg", new BsonString(message));
        return new MongoCommandException(response, ADDRESS);
    }
}
