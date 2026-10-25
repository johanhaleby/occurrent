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
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.client.impl.recovery.ConsumerRecoveryListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Unit tests for {@link RabbitMqConsumerRecovery} against a mocked {@link AutorecoveringConnection}, driving the two
 * listeners it registers in the order the client calls them. The real client is covered against a broker by the
 * consumer recovery tests of the two sources.
 */
class RabbitMqConsumerRecoveryTest {

    private static final String QUEUE = "the-queue";

    private final AtomicReference<String> consumerTag = new AtomicReference<>();
    private final AutorecoveringConnection connection = mock(AutorecoveringConnection.class);
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
        recovery.attachTo(connection);
        RecoveryListener recoveryListener = capturedRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        recoveryListener.handleRecovery(connection);

        assertThat(consumerTag.get()).isNull();
    }

    @Test
    void the_tag_is_still_held_until_the_recovery_has_finished() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection);
        RecoveryListener recoveryListener = capturedRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);

        assertThat(consumerTag.get()).isEqualTo("tag-1");
    }

    @Test
    void clearing_the_tag_logs_once_at_info_naming_the_queue_and_the_tag() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection);
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
        recovery.attachTo(connection);
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
        recovery.attachTo(connection);
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
        recovery.attachTo(connection);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        ConsumerRecoveryListener consumerListener = capturedConsumerRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        consumerListener.consumerRecovered("another-consumers-old-tag", "another-consumers-new-tag");

        assertThat(consumerTag.get()).isEqualTo("tag-1");
    }

    @Test
    void a_consumer_registered_after_the_recovery_started_is_not_cleared() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection);
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
        recovery.attachTo(connection);
        RecoveryListener recoveryListener = capturedRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        recoveryListener.handleRecovery(connection);

        assertAll(
                () -> assertThat(consumerTag.get()).isNull(),
                () -> assertThat(logged.list).isEmpty());
    }

    @Test
    void a_source_without_a_tag_when_the_recovery_started_keeps_one_registered_during_the_recovery() {
        recovery.attachTo(connection);
        RecoveryListener recoveryListener = capturedRecoveryListener();

        recoveryListener.handleRecoveryStarted(connection);
        consumerTag.set("tag-registered-by-the-poll");
        recoveryListener.handleRecovery(connection);

        assertThat(consumerTag.get()).isEqualTo("tag-registered-by-the-poll");
    }

    @Test
    void a_recovery_that_re_registered_the_consumer_does_not_stop_the_next_recovery_from_clearing_a_tag_that_was_not() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection);
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
        recovery.attachTo(connection);
        RecoveryListener recoveryListener = capturedRecoveryListener();
        recoveryListener.handleRecoveryStarted(connection);
        recoveryListener.handleRecovery(connection);
        consumerTag.set("tag-registered-by-the-poll");

        recoveryListener.handleRecovery(connection);

        assertAll(
                () -> assertThat(consumerTag.get()).isEqualTo("tag-registered-by-the-poll"),
                () -> assertThat(logged.list).hasSize(1));
    }

    @Test
    void attachTo_registers_one_recovery_listener_and_one_consumer_recovery_listener_that_are_the_same_instance() {
        recovery.attachTo(connection);

        assertThat(capturedRecoveryListener()).isSameAs(capturedConsumerRecoveryListener());
    }

    @Test
    void attachTo_twice_registers_only_once() {
        recovery.attachTo(connection);
        recovery.attachTo(connection);

        verify(connection, times(1)).addRecoveryListener(any());
        verify(connection, times(1)).addConsumerRecoveryListener(any());
    }

    @Test
    void attachTo_registers_nothing_on_a_connection_that_does_not_recover_by_itself() {
        Connection plainConnection = mock(Connection.class);

        assertThatCode(() -> recovery.attachTo(plainConnection)).doesNotThrowAnyException();

        verifyNoInteractions(plainConnection);
        verifyNoInteractions(connection);
    }

    @Test
    void attachTo_throws_NullPointerException_for_a_null_connection() {
        assertThatThrownBy(() -> recovery.attachTo(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void detach_removes_the_two_listeners_that_attachTo_registered_and_nothing_else() {
        recovery.attachTo(connection);
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
        recovery.attachTo(connection);

        recovery.detach();
        recovery.detach();

        verify(connection, times(1)).removeRecoveryListener(any());
        verify(connection, times(1)).removeConsumerRecoveryListener(any());
    }

    @Test
    void detach_still_removes_the_consumer_recovery_listener_when_removing_the_recovery_listener_throws() {
        recovery.attachTo(connection);
        ConsumerRecoveryListener registeredConsumerListener = capturedConsumerRecoveryListener();
        doThrow(new IllegalStateException("connection closed")).when(connection).removeRecoveryListener(any());

        catchThrowable(recovery::detach);

        verify(connection, times(1)).removeConsumerRecoveryListener(registeredConsumerListener);
    }

    @Test
    void detach_throws_the_failure_of_the_removal_that_failed() {
        recovery.attachTo(connection);
        IllegalStateException failure = new IllegalStateException("connection closed");
        doThrow(failure).when(connection).removeRecoveryListener(any());

        Throwable thrown = catchThrowable(recovery::detach);

        assertThat(thrown).isSameAs(failure);
    }

    @Test
    void a_second_detach_after_a_failed_one_does_nothing() {
        recovery.attachTo(connection);
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

        recovery.attachTo(connection);

        verify(connection, never()).addRecoveryListener(any());
        verify(connection, never()).addConsumerRecoveryListener(any());
    }

    @Test
    void a_late_recovery_callback_from_a_listener_that_was_detached_does_not_throw() {
        consumerTag.set("tag-1");
        recovery.attachTo(connection);
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
