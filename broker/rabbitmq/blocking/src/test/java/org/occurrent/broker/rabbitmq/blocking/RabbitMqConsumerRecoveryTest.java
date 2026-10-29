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

package org.occurrent.broker.rabbitmq.blocking;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ChannelContinuationTimeoutException;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.client.impl.recovery.ConsumerRecoveryListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.occurrent.broker.rabbitmq.blocking.RabbitMqConsumerRecovery.UNFINISHED_RECOVERY_WARNING_NANOS;

/**
 * Unit tests for {@link RabbitMqConsumerRecovery} against a mocked {@link AutorecoveringConnection}, driving the two
 * listeners it registers in the order the client calls them. The real client is covered against a broker by the
 * consumer recovery tests of the two sources.
 */
class RabbitMqConsumerRecoveryTest {

    private static final String QUEUE = "the-queue";

    private final AtomicReference<String> consumerTag = new AtomicReference<>();
    private final AutorecoveringConnection connection = mock(AutorecoveringConnection.class);
    private final Channel channel = mock(Channel.class);
    private final ch.qos.logback.classic.Logger log = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("consumer-recovery-test");
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final RabbitMqConsumerRecovery recovery = new RabbitMqConsumerRecovery(consumerTag, QUEUE, log);

    @BeforeEach
    void attachLogAppender() {
        logged.start();
        log.addAppender(logged);
    }

    @AfterEach
    void detachLogAppender() {
        log.detachAppender(logged);
    }

    @Test
    void topology_recovery_off_clears_the_tag_when_the_recovery_finishes_without_re_registering_the_consumer() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        recoveryListener.handleRecovery(connection);

        assertThat(consumerTag.get()).isNull();
    }

    @Test
    void the_tag_is_still_held_until_the_recovery_has_finished() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);

        assertThat(consumerTag.get()).isEqualTo("tag-1");
    }

    @Test
    void clearing_the_tag_logs_once_at_info_naming_the_queue_and_the_tag() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        recoveryListener.handleRecovery(connection);

        assertThat(logged.list).hasSize(1);
        ILoggingEvent event = logged.list.get(0);
        assertAll(
                () -> assertThat(event.getLevel()).isEqualTo(Level.INFO),
                () -> assertThat(event.getFormattedMessage()).contains(QUEUE).contains("tag-1"));
    }

    @Test
    void topology_recovery_on_updates_the_tag_to_the_one_the_client_registered_and_does_not_clear_it() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        ConsumerRecoveryListener consumerListener = capturedConsumerRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        consumerListener.consumerRecovered("tag-1", "tag-2");
        recoveryListener.handleRecovery(connection);

        assertAll(
                () -> assertThat(consumerTag.get()).isEqualTo("tag-2"),
                () -> assertThat(logged.list).isEmpty());
    }

    @Test
    void a_recovered_consumer_that_is_not_this_sources_does_not_count_as_this_sources_being_re_registered() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        ConsumerRecoveryListener consumerListener = capturedConsumerRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        consumerListener.consumerRecovered("another-consumers-old-tag", "another-consumers-new-tag");
        recoveryListener.handleRecovery(connection);

        assertThat(consumerTag.get()).isNull();
    }

    @Test
    void a_recovered_consumer_that_is_not_this_sources_leaves_the_tag_alone_while_the_recovery_runs() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        ConsumerRecoveryListener consumerListener = capturedConsumerRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        consumerListener.consumerRecovered("another-consumers-old-tag", "another-consumers-new-tag");

        assertThat(consumerTag.get()).isEqualTo("tag-1");
    }

    @Test
    void a_consumer_registered_after_the_recovery_started_is_not_cleared() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        consumerTag.set("tag-registered-by-the-poll");
        recoveryListener.handleRecovery(connection);

        assertAll(
                () -> assertThat(consumerTag.get()).isEqualTo("tag-registered-by-the-poll"),
                () -> assertThat(logged.list).isEmpty());
    }

    @Test
    void a_source_without_a_tag_when_the_recovery_started_still_has_none_after_it_and_logs_nothing() {
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        recoveryListener.handleRecovery(connection);

        assertAll(
                () -> assertThat(consumerTag.get()).isNull(),
                () -> assertThat(logged.list).isEmpty());
    }

    @Test
    void a_source_without_a_tag_when_the_recovery_started_keeps_one_registered_during_the_recovery() {
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        consumerTag.set("tag-registered-by-the-poll");
        recoveryListener.handleRecovery(connection);

        assertThat(consumerTag.get()).isEqualTo("tag-registered-by-the-poll");
    }

    @Test
    void a_recovery_that_re_registered_the_consumer_does_not_stop_the_next_recovery_from_clearing_a_tag_that_was_not() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        ConsumerRecoveryListener consumerListener = capturedConsumerRecoveryListener();
        recoveryListener.handleRecoveryStarted(connection);
        consumerListener.consumerRecovered("tag-1", "tag-2");
        recoveryListener.handleRecovery(connection);

        recoveryListener.handleRecoveryStarted(connection);
        recoveryListener.handleRecovery(connection);

        assertThat(consumerTag.get()).isNull();
    }

    @Test
    void a_second_handleRecovery_without_a_new_start_does_not_clear_a_tag_registered_in_between() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        recoveryListener.handleRecoveryStarted(connection);
        recoveryListener.handleRecovery(connection);
        consumerTag.set("tag-registered-by-the-poll");

        recoveryListener.handleRecovery(connection);

        assertAll(
                () -> assertThat(consumerTag.get()).isEqualTo("tag-registered-by-the-poll"),
                () -> assertThat(logged.list).hasSize(1));
    }

    /**
     * Topology recovery registers the consumer again under its recorded tag, and the client keeps reporting the tag
     * {@code basicConsume} first returned as the old one, also on the second recovery. By then the source holds the
     * tag the first recovery reported as the new one, which the client decoded from a frame of its own, so it is equal
     * to the old one without being the same {@code String}.
     */
    @Test
    void topology_recovery_on_keeps_the_tag_across_two_recoveries_that_report_it_as_separate_strings() throws Exception {
        String registered = new String("amq.ctag-1");
        consumerTag.set(registered);
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        ConsumerRecoveryListener consumerListener = capturedConsumerRecoveryListener();

        for (int recoveries = 1; recoveries <= 2; recoveries++) {
            recoveryListener.handleRecoveryStarted(connection);
            consumerListener.consumerRecovered(registered, new String("amq.ctag-1"));
            recoveryListener.handleRecovery(connection);
        }
        boolean noneLeft = recovery.cancelRetired(channel);

        assertAll(
                () -> assertThat(consumerTag.get()).isEqualTo("amq.ctag-1"),
                () -> assertThat(logged.list).isEmpty(),
                () -> assertThat(noneLeft).isTrue(),
                () -> verifyNoInteractions(channel));
    }

    @Test
    void a_consumer_whose_registration_a_recovery_started_during_is_not_held_and_the_next_cancel_removes_it() throws Exception {
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();

        recovery.consume(() -> {
            recoveryListener.handleRecoveryStarted(connection);
            return "tag-registered-as-the-recovery-started";
        });
        recoveryListener.handleRecovery(connection);
        boolean noneLeft = recovery.cancelRetired(channel);

        assertAll(
                () -> assertThat(consumerTag.get()).isNull(),
                () -> assertThat(noneLeft).isTrue(),
                () -> verify(channel).basicCancel("tag-registered-as-the-recovery-started"));
    }

    @Test
    void consume_holds_the_tag_when_no_recovery_overlapped_it() throws Exception {
        recovery.attachTo(connection, channel);

        recovery.consume(() -> "tag-1");

        assertThat(consumerTag.get()).isEqualTo("tag-1");
    }

    @Test
    void consume_registers_nothing_while_a_recovery_runs() throws Exception {
        recovery.attachTo(connection, channel);
        capturedRecoveryListener().handleRecoveryStarted(connection);

        recovery.consume(() -> {
            throw new AssertionError("registered a consumer while a recovery ran");
        });

        assertThat(consumerTag.get()).isNull();
    }

    @Test
    void a_tag_cleared_by_a_recovery_with_topology_recovery_off_is_cancelled_by_the_next_cancel() throws Exception {
        consumerTag.set("tag-1");
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        recoveryListener.handleRecoveryStarted(connection);
        recoveryListener.handleRecovery(connection);

        recovery.cancelRetired(channel);

        verify(channel).basicCancel("tag-1");
    }

    @Test
    void a_consumer_retired_while_a_recovery_runs_is_cancelled_under_the_tag_the_recovery_registered_it_with() throws Exception {
        consumerTag.set("tag-1");
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        ConsumerRecoveryListener consumerListener = capturedConsumerRecoveryListener();
        recoveryListener.handleRecoveryStarted(connection);

        recovery.retire("tag-1");
        boolean noneLeftDuringRecovery = recovery.cancelRetired(channel);
        consumerListener.consumerRecovered("tag-1", "tag-2");
        recoveryListener.handleRecovery(connection);
        boolean noneLeftAfterRecovery = recovery.cancelRetired(channel);

        assertAll(
                () -> assertThat(noneLeftDuringRecovery).isFalse(),
                () -> assertThat(noneLeftAfterRecovery).isTrue(),
                () -> assertThat(consumerTag.get()).isNull(),
                () -> verify(channel).basicCancel("tag-2"),
                () -> verify(channel, never()).basicCancel("tag-1"));
    }

    @Test
    void a_cancel_that_a_recovery_overlapped_is_made_again_after_the_recovery() throws Exception {
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        recovery.retire("tag-1");
        doAnswer(invocation -> {
            recoveryListener.handleRecoveryStarted(connection);
            return null;
        }).doNothing().when(channel).basicCancel("tag-1");

        boolean noneLeftAfterOverlap = recovery.cancelRetired(channel);
        recoveryListener.handleRecovery(connection);
        boolean noneLeftAfterRecovery = recovery.cancelRetired(channel);

        assertAll(
                () -> assertThat(noneLeftAfterOverlap).isFalse(),
                () -> assertThat(noneLeftAfterRecovery).isTrue(),
                () -> verify(channel, times(2)).basicCancel("tag-1"));
    }

    @Test
    void a_cancel_for_a_consumer_the_channel_no_longer_has_forgets_it() throws Exception {
        recovery.retire("tag-1");
        doThrow(new IOException("Unknown consumerTag")).when(channel).basicCancel("tag-1");

        boolean noneLeft = recovery.cancelRetired(channel);
        recovery.cancelRetired(channel);

        assertAll(
                () -> assertThat(noneLeft).isTrue(),
                () -> verify(channel).basicCancel("tag-1"));
    }

    @Test
    void a_cancel_whose_reply_timed_out_keeps_the_consumer_and_throws() throws Exception {
        recovery.retire("tag-1");
        ChannelContinuationTimeoutException timeout = new ChannelContinuationTimeoutException(new TimeoutException("no reply"), channel, 1, null);
        doThrow(timeout).doNothing().when(channel).basicCancel("tag-1");

        Throwable thrown = catchThrowable(() -> recovery.cancelRetired(channel));
        boolean noneLeft = recovery.cancelRetired(channel);

        assertAll(
                () -> assertThat(thrown).isSameAs(timeout),
                () -> assertThat(noneLeft).isTrue(),
                () -> verify(channel, times(2)).basicCancel("tag-1"));
    }

    @Test
    void a_cancel_that_fails_in_a_way_that_says_nothing_about_the_consumer_keeps_it_and_throws() throws Exception {
        recovery.retire("tag-1");
        IllegalStateException failure = new IllegalStateException("thrown by a TrafficListener");
        doThrow(failure).doNothing().when(channel).basicCancel("tag-1");

        Throwable thrown = catchThrowable(() -> recovery.cancelRetired(channel));
        boolean noneLeft = recovery.cancelRetired(channel);

        assertAll(
                () -> assertThat(thrown).isSameAs(failure),
                () -> assertThat(noneLeft).isTrue(),
                () -> verify(channel, times(2)).basicCancel("tag-1"));
    }

    @Test
    void retire_clears_a_tag_equal_to_the_held_one_that_is_a_separate_string() throws Exception {
        consumerTag.set(new String("amq.ctag-1"));

        recovery.retire(new String("amq.ctag-1"));
        recovery.cancelRetired(channel);

        assertAll(
                () -> assertThat(consumerTag.get()).isNull(),
                () -> verify(channel).basicCancel("amq.ctag-1"));
    }

    @Test
    void retire_clears_the_tag_only_when_the_source_holds_that_one() {
        consumerTag.set("tag-2");

        recovery.retire("tag-1");

        assertThat(consumerTag.get()).isEqualTo("tag-2");
    }

    @Test
    void attachTo_registers_one_recovery_listener_and_one_consumer_recovery_listener_that_are_the_same_instance() {
        recovery.attachTo(connection, channel);

        assertThat(capturedRecoveryListener()).isSameAs(capturedConsumerRecoveryListener());
    }

    @Test
    void attachTo_twice_registers_only_once() {
        recovery.attachTo(connection, channel);
        recovery.attachTo(connection, channel);

        verify(connection, times(1)).addRecoveryListener(any());
        verify(connection, times(1)).addConsumerRecoveryListener(any());
    }

    @Test
    void attachTo_registers_nothing_on_a_connection_that_does_not_recover_by_itself() {
        Connection plainConnection = mock(Connection.class);

        assertThatCode(() -> recovery.attachTo(plainConnection, channel)).doesNotThrowAnyException();

        verifyNoInteractions(plainConnection);
        verifyNoInteractions(connection);
    }

    @Test
    void attachTo_logs_nothing_for_a_connection_that_does_not_recover() {
        recovery.attachTo(mock(Connection.class), channel);

        assertThat(logged.list).isEmpty();
    }

    /**
     * A {@code Connection} that wraps an {@code AutorecoveringConnection}, for metrics or tracing, hands out the
     * client's recovering channels but calls no recovery listener, so with topology recovery off the source would stop
     * consuming after a recovery with nothing logged.
     */
    @Test
    void attachTo_logs_a_warning_naming_the_queue_for_a_connection_that_wraps_a_recovering_one() {
        Channel recoveringChannel = mock(Channel.class, withSettings().extraInterfaces(Recoverable.class));

        recovery.attachTo(mock(Connection.class), recoveringChannel);

        assertThat(warnings()).singleElement().satisfies(event -> assertThat(event.getFormattedMessage()).contains(QUEUE));
    }

    /**
     * The client stops calling {@code handleRecovery} at the first listener that throws, and the source then waits for a
     * recovery that never ends.
     */
    @Test
    void a_recovery_the_client_never_reports_ended_is_logged_at_warn_once_after_the_channel_has_been_open_for_a_minute() throws Exception {
        AtomicLong now = new AtomicLong();
        RabbitMqConsumerRecovery clocked = new RabbitMqConsumerRecovery(consumerTag, QUEUE, log, now::get);
        clocked.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        when(channel.isOpen()).thenReturn(true);

        recoveryListener.handleRecoveryStarted(connection);
        clocked.cancelRetired(channel);
        now.addAndGet(UNFINISHED_RECOVERY_WARNING_NANOS - 1);
        clocked.cancelRetired(channel);
        List<ILoggingEvent> beforeAMinute = List.copyOf(warnings());
        now.addAndGet(1);
        clocked.cancelRetired(channel);
        clocked.cancelRetired(channel);

        assertAll(
                () -> assertThat(beforeAMinute).isEmpty(),
                () -> assertThat(warnings()).singleElement().satisfies(event -> assertThat(event.getFormattedMessage()).contains(QUEUE)));
    }

    @Test
    void a_recovery_that_waits_for_the_broker_logs_nothing_however_long_it_takes() throws Exception {
        AtomicLong now = new AtomicLong();
        RabbitMqConsumerRecovery clocked = new RabbitMqConsumerRecovery(consumerTag, QUEUE, log, now::get);
        clocked.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        for (int minute = 0; minute < 10; minute++) {
            clocked.cancelRetired(channel);
            now.addAndGet(UNFINISHED_RECOVERY_WARNING_NANOS);
        }

        assertThat(warnings()).isEmpty();
    }

    @Test
    void recoveries_changes_when_a_recovery_starts_and_when_it_ends() {
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        long before = recovery.recoveries();

        recoveryListener.handleRecoveryStarted(connection);
        long during = recovery.recoveries();
        recoveryListener.handleRecovery(connection);

        assertAll(
                () -> assertThat(during).isNotEqualTo(before),
                () -> assertThat(recovery.recoveries()).isNotEqualTo(during).isNotEqualTo(before));
    }

    @Test
    void attachTo_throws_NullPointerException_for_a_null_connection() {
        assertThatThrownBy(() -> recovery.attachTo(null, channel)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void detach_removes_the_two_listeners_that_attachTo_registered_and_nothing_else() {
        recovery.attachTo(connection, channel);
        RecoveryListener registeredRecoveryListener = capturedRecoveryListener();
        ConsumerRecoveryListener registeredConsumerListener = capturedConsumerRecoveryListener();

        recovery.detach();

        ArgumentCaptor<RecoveryListener> removedRecoveryListener = ArgumentCaptor.forClass(RecoveryListener.class);
        ArgumentCaptor<ConsumerRecoveryListener> removedConsumerListener = ArgumentCaptor.forClass(ConsumerRecoveryListener.class);
        verify(connection, times(1)).removeRecoveryListener(removedRecoveryListener.capture());
        verify(connection, times(1)).removeConsumerRecoveryListener(removedConsumerListener.capture());
        assertAll(
                () -> assertThat(removedRecoveryListener.getValue()).isSameAs(registeredRecoveryListener),
                () -> assertThat(removedConsumerListener.getValue()).isSameAs(registeredConsumerListener));
    }

    @Test
    void detach_is_idempotent() {
        recovery.attachTo(connection, channel);

        recovery.detach();
        recovery.detach();

        verify(connection, times(1)).removeRecoveryListener(any());
        verify(connection, times(1)).removeConsumerRecoveryListener(any());
    }

    @Test
    void detach_still_removes_the_consumer_recovery_listener_when_removing_the_recovery_listener_throws() {
        recovery.attachTo(connection, channel);
        ConsumerRecoveryListener registeredConsumerListener = capturedConsumerRecoveryListener();
        doThrow(new IllegalStateException("connection closed")).when(connection).removeRecoveryListener(any());

        catchThrowable(recovery::detach);

        verify(connection, times(1)).removeConsumerRecoveryListener(registeredConsumerListener);
    }

    @Test
    void detach_throws_the_failure_of_the_removal_that_failed() {
        recovery.attachTo(connection, channel);
        IllegalStateException failure = new IllegalStateException("connection closed");
        doThrow(failure).when(connection).removeRecoveryListener(any());

        Throwable thrown = catchThrowable(recovery::detach);

        assertThat(thrown).isSameAs(failure);
    }

    @Test
    void a_second_detach_after_a_failed_one_does_nothing() {
        recovery.attachTo(connection, channel);
        doThrow(new IllegalStateException("connection closed")).when(connection).removeRecoveryListener(any());
        catchThrowable(recovery::detach);

        assertThatCode(recovery::detach).doesNotThrowAnyException();

        verify(connection, times(1)).removeRecoveryListener(any());
        verify(connection, times(1)).removeConsumerRecoveryListener(any());
    }

    @Test
    void detach_before_attachTo_does_nothing_and_does_not_throw() {
        assertThatCode(recovery::detach).doesNotThrowAnyException();

        verifyNoInteractions(connection);
    }

    @Test
    void attachTo_after_detach_registers_nothing() {
        recovery.detach();

        recovery.attachTo(connection, channel);

        verify(connection, never()).addRecoveryListener(any());
        verify(connection, never()).addConsumerRecoveryListener(any());
    }

    @Test
    void a_late_recovery_callback_from_a_listener_that_was_detached_does_not_throw() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection, channel);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        ConsumerRecoveryListener consumerListener = capturedConsumerRecoveryListener();
        recoveryListener.handleRecoveryStarted(connection);
        recovery.detach();

        assertThatCode(() -> {
            consumerListener.consumerRecovered("tag-1", "tag-2");
            recoveryListener.handleRecovery(connection);
            recoveryListener.handleRecoveryStarted(connection);
            recoveryListener.handleRecovery(connection);
        }).doesNotThrowAnyException();
    }

    private List<ILoggingEvent> warnings() {
        return logged.list.stream().filter(event -> event.getLevel() == Level.WARN).toList();
    }

    private RecoveryListener capturedRecoveryListener() {
        ArgumentCaptor<RecoveryListener> captor = ArgumentCaptor.forClass(RecoveryListener.class);
        verify(connection).addRecoveryListener(captor.capture());
        return captor.getValue();
    }

    private ConsumerRecoveryListener capturedConsumerRecoveryListener() {
        ArgumentCaptor<ConsumerRecoveryListener> captor = ArgumentCaptor.forClass(ConsumerRecoveryListener.class);
        verify(connection).addConsumerRecoveryListener(captor.capture());
        return captor.getValue();
    }
}
