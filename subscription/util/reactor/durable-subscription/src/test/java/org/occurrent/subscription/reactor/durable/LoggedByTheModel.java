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

package org.occurrent.subscription.reactor.durable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * What {@link ReactorDurableSubscriptionModel} logs from when this is created until it is closed.
 */
final class LoggedByTheModel implements AutoCloseable {
    private final Logger logger = (Logger) LoggerFactory.getLogger(ReactorDurableSubscriptionModel.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    LoggedByTheModel() {
        appender.start();
        logger.addAppender(appender);
    }

    /**
     * The formatted messages logged at exactly this level.
     */
    List<String> at(Level level) {
        // The appender appends while holding its own monitor
        synchronized (appender) {
            return appender.list.stream()
                    .filter(event -> event.getLevel().equals(level))
                    .map(ILoggingEvent::getFormattedMessage)
                    .toList();
        }
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
    }
}
