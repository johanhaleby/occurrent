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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.MongoInterruptedException;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class KnownClusterTimeTest {

    private static final BsonTimestamp CLUSTER_TIME = new BsonTimestamp(1_700_000_000, 7);

    private final Logger logger = (Logger) LoggerFactory.getLogger(KnownClusterTime.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    @BeforeEach
    void capture_the_log() {
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void stop_capturing_the_log() {
        logger.detachAppender(appender);
        Thread.interrupted();
    }

    @Test
    void a_clock_that_answers_a_cluster_time_is_read() {
        KnownClusterTime knownClusterTime = KnownClusterTime.of(new Clock(() -> CLUSTER_TIME), getClusterTime(Clock.class));

        assertThat(knownClusterTime.read()).isEqualTo(CLUSTER_TIME);
        assertThat(warnings()).isEmpty();
    }

    @Test
    void a_clock_that_answers_another_type_is_not_read_and_a_warning_is_logged_even_when_it_has_no_cluster_time_yet() {
        KnownClusterTime knownClusterTime = KnownClusterTime.of(new ClockOfAnotherType(), getClusterTime(ClockOfAnotherType.class));

        assertThat(knownClusterTime.isReadable()).isFalse();
        assertThat(knownClusterTime.read()).isNull();
        assertThat(warnings()).hasSize(1);
    }

    @Test
    void a_read_that_fails_logs_a_warning_the_first_time_only() {
        Queue<Supplier<BsonTimestamp>> answers = new ArrayDeque<>(List.of(
                () -> CLUSTER_TIME,
                () -> {
                    throw new IllegalStateException("can't read");
                },
                () -> {
                    throw new IllegalStateException("can't read");
                },
                () -> CLUSTER_TIME));
        KnownClusterTime knownClusterTime = KnownClusterTime.of(new Clock(() -> answers.remove().get()), getClusterTime(Clock.class));

        assertThat(knownClusterTime.read()).isNull();
        assertThat(knownClusterTime.read()).isNull();
        assertThat(knownClusterTime.read()).isEqualTo(CLUSTER_TIME);
        assertThat(warnings()).hasSize(1);
    }

    @Test
    void a_read_on_an_interrupted_thread_answers_null_keeps_the_interrupt_and_logs_no_warning() {
        Queue<Supplier<BsonTimestamp>> answers = new ArrayDeque<>(List.of(
                () -> CLUSTER_TIME,
                () -> {
                    throw new MongoInterruptedException("Interrupted waiting for lock", new InterruptedException());
                },
                () -> CLUSTER_TIME));
        KnownClusterTime knownClusterTime = KnownClusterTime.of(new Clock(() -> answers.remove().get()), getClusterTime(Clock.class));

        assertThat(knownClusterTime.read()).isNull();
        assertThat(Thread.interrupted()).isTrue();
        assertThat(knownClusterTime.read()).isEqualTo(CLUSTER_TIME);
        assertThat(warnings()).isEmpty();
    }

    @Test
    void a_clock_whose_first_read_is_interrupted_is_still_read_afterwards_and_the_interrupt_is_kept() {
        Queue<Supplier<BsonTimestamp>> answers = new ArrayDeque<>(List.of(
                () -> {
                    throw new MongoInterruptedException("Interrupted waiting for lock", new InterruptedException());
                },
                () -> CLUSTER_TIME));
        KnownClusterTime knownClusterTime = KnownClusterTime.of(new Clock(() -> answers.remove().get()), getClusterTime(Clock.class));

        assertThat(Thread.interrupted()).isTrue();
        assertThat(knownClusterTime.isReadable()).isTrue();
        assertThat(knownClusterTime.read()).isEqualTo(CLUSTER_TIME);
        assertThat(warnings()).isEmpty();
    }

    @Test
    void a_clock_whose_first_read_fails_otherwise_is_not_read_and_a_warning_is_logged() {
        KnownClusterTime knownClusterTime = KnownClusterTime.of(new Clock(() -> {
            throw new IllegalStateException("can't read");
        }), getClusterTime(Clock.class));

        assertThat(knownClusterTime.isReadable()).isFalse();
        assertThat(warnings()).hasSize(1);
    }

    private List<ILoggingEvent> warnings() {
        return appender.list.stream().filter(event -> event.getLevel() == Level.WARN).toList();
    }

    private static Method getClusterTime(Class<?> clockType) {
        try {
            return clockType.getMethod("getClusterTime");
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    static final class Clock {
        private final Supplier<BsonTimestamp> clusterTime;

        Clock(Supplier<BsonTimestamp> clusterTime) {
            this.clusterTime = clusterTime;
        }

        public BsonTimestamp getClusterTime() {
            return clusterTime.get();
        }
    }

    static final class ClockOfAnotherType {
        public Object getClusterTime() {
            return null;
        }
    }
}
