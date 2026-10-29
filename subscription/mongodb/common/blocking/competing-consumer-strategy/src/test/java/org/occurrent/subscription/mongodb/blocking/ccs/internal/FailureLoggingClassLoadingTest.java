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

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The first failure a node logs can be an {@link OutOfMemoryError}, and loading a class needs memory. A class that a
 * catch loads only when the failure arrives can then throw from that catch. When that happens in a refresh round, the
 * executor cancels every later round. {@code org.slf4j.event.Level} is such a class when nothing read it before the
 * failure. These tests load the classes under test in a class loader that, once a test arms it, throws an
 * {@link OutOfMemoryError} for {@code Level} and for every class of this package it has not loaded yet.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@DisplayName("FailureLoggingClassLoading")
class FailureLoggingClassLoadingTest {

    private static final String LEVEL = "org.slf4j.event.Level";

    @Test
    void a_failed_refresh_round_does_not_throw_when_no_class_can_be_loaded_any_more() throws Exception {
        // Given
        NoNewClassesOnceArmed loader = new NoNewClassesOnceArmed();
        Class.forName(ScheduledRefresh.class.getName(), true, loader);
        Method logEveryThrowable = Class.forName(ScheduledRefresh.Scheduler.class.getName(), true, loader)
                .getDeclaredMethod("logEveryThrowable", Runnable.class);
        logEveryThrowable.setAccessible(true);
        Runnable round = (Runnable) logEveryThrowable.invoke(null, (Runnable) () -> {
            throw new OutOfMemoryError("Java heap space");
        });
        loader.arm();

        // When
        Throwable thrown = catchThrowable(round::run);

        // Then
        assertThat(thrown).isNull();
    }

    @ParameterizedTest
    @ValueSource(classes = {ScheduledRefresh.class, MongoLeaseCompetingConsumerStrategySupport.class})
    void initializing_a_class_that_logs_failures_loads_failure_logging_with_it(Class<?> logsFailures) throws Exception {
        // Given
        NoNewClassesOnceArmed loader = new NoNewClassesOnceArmed();

        // When
        Class.forName(logsFailures.getName(), true, loader);

        // Then
        assertThat(loader.hasLoaded(FailureLogging.class.getName())).isTrue();
    }

    // Covers the catches the class loader test above does not reach
    @ParameterizedTest
    @ValueSource(classes = {ScheduledRefresh.class, ScheduledRefresh.Scheduler.class, MongoLeaseCompetingConsumerStrategySupport.class, FailureLogging.class})
    void a_class_that_logs_failures_never_reads_the_slf4j_level(Class<?> logsFailures) throws IOException {
        String classFile = new String(classFileOf(logsFailures.getName()), StandardCharsets.ISO_8859_1);

        assertThat(classFile.contains(LEVEL.replace('.', '/'))).as("%s names %s", logsFailures.getName(), LEVEL).isFalse();
    }

    private static byte[] classFileOf(String className) throws IOException {
        try (InputStream in = FailureLoggingClassLoadingTest.class.getClassLoader().getResourceAsStream(className.replace('.', '/') + ".class")) {
            if (in == null) {
                throw new IOException("No class file for " + className);
            }
            return in.readAllBytes();
        }
    }

    private static final class NoNewClassesOnceArmed extends ClassLoader {
        private static final String PACKAGE = FailureLogging.class.getPackageName() + ".";
        private volatile boolean armed;

        NoNewClassesOnceArmed() {
            super(NoNewClassesOnceArmed.class.getClassLoader());
        }

        void arm() {
            armed = true;
        }

        boolean hasLoaded(String className) {
            return findLoadedClass(className) != null;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    boolean definedHere = name.startsWith(PACKAGE);
                    if (armed && (definedHere || name.equals(LEVEL))) {
                        throw new OutOfMemoryError("No memory left to load " + name);
                    }
                    loaded = definedHere ? defineFromTheClassFile(name) : getParent().loadClass(name);
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }

        private Class<?> defineFromTheClassFile(String name) throws ClassNotFoundException {
            try {
                byte[] bytes = classFileOf(name);
                return defineClass(name, bytes, 0, bytes.length);
            } catch (IOException e) {
                throw new ClassNotFoundException(name, e);
            }
        }
    }
}
