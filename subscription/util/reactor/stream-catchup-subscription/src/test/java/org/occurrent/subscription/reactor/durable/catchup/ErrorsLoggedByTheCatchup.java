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

package org.occurrent.subscription.reactor.durable.catchup;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The errors the catch-up classes log from when this is created until it is closed.
 */
final class ErrorsLoggedByTheCatchup implements AutoCloseable {
    private final List<Logger> loggers = List.of((Logger) LoggerFactory.getLogger(NamedCatchupSupport.class), (Logger) LoggerFactory.getLogger(PositionCatchupPipeline.class));
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    ErrorsLoggedByTheCatchup() {
        appender.start();
        loggers.forEach(logger -> logger.addAppender(appender));
    }

    /**
     * The formatted messages logged at error level.
     */
    List<String> errors() {
        // The appender appends while holding its own monitor
        synchronized (appender) {
            return appender.list.stream()
                    .filter(event -> event.getLevel().equals(Level.ERROR))
                    .map(ILoggingEvent::getFormattedMessage)
                    .toList();
        }
    }

    @Override
    public void close() {
        loggers.forEach(logger -> logger.detachAppender(appender));
    }
}
