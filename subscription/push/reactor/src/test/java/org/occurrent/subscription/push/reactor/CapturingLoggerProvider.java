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

package org.occurrent.subscription.push.reactor;

import org.slf4j.ILoggerFactory;
import org.slf4j.IMarkerFactory;
import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.helpers.BasicMarkerFactory;
import org.slf4j.helpers.LegacyAbstractLogger;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.helpers.NOPMDCAdapter;
import org.slf4j.spi.MDCAdapter;
import org.slf4j.spi.SLF4JServiceProvider;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The SLF4J binding of this module's tests, registered in {@code META-INF/services}, which keeps what a class logs so a
 * test can assert it. The module has no logging backend of its own to capture from, so without a binding every log is
 * dropped and the message a failed catch-up tells an operator is never seen by any test.
 */
public final class CapturingLoggerProvider implements SLF4JServiceProvider {

    /**
     * A message that was logged, formatted the way a log backend would print it.
     */
    public record Logged(String loggerName, Level level, String message) {
    }

    private static final List<Logged> LOGGED = new CopyOnWriteArrayList<>();

    /**
     * What was logged at {@code level} by the logger named after {@code loggerClass}, in the order it was logged.
     */
    public static List<Logged> logged(Class<?> loggerClass, Level level) {
        return LOGGED.stream().filter(logged -> logged.loggerName().equals(loggerClass.getName()) && logged.level() == level).toList();
    }

    public static void clear() {
        LOGGED.clear();
    }

    private final ILoggerFactory loggerFactory = CapturingLogger::new;
    private final IMarkerFactory markerFactory = new BasicMarkerFactory();
    private final MDCAdapter mdcAdapter = new NOPMDCAdapter();

    @Override
    public ILoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    @Override
    public IMarkerFactory getMarkerFactory() {
        return markerFactory;
    }

    @Override
    public MDCAdapter getMDCAdapter() {
        return mdcAdapter;
    }

    @Override
    public String getRequestedApiVersion() {
        return "2.0.99";
    }

    @Override
    public void initialize() {
    }

    private static final class CapturingLogger extends LegacyAbstractLogger {
        private final String loggerName;

        private CapturingLogger(String loggerName) {
            this.name = loggerName;
            this.loggerName = loggerName;
        }

        @Override
        public boolean isTraceEnabled() {
            return false;
        }

        @Override
        public boolean isDebugEnabled() {
            return false;
        }

        @Override
        public boolean isInfoEnabled() {
            return true;
        }

        @Override
        public boolean isWarnEnabled() {
            return true;
        }

        @Override
        public boolean isErrorEnabled() {
            return true;
        }

        @Override
        protected String getFullyQualifiedCallerName() {
            return null;
        }

        @Override
        protected void handleNormalizedLoggingCall(Level level, Marker marker, String messagePattern, Object[] arguments, Throwable throwable) {
            LOGGED.add(new Logged(loggerName, level, MessageFormatter.arrayFormat(messagePattern, arguments, throwable).getMessage()));
        }
    }
}
