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

package org.occurrent.application.service.spring.reactor;

import com.mongodb.MongoException;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.AbstractReactiveTransactionManager;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The MongoDB driver is an optional dependency of the executor, so it has to load and keep retrying Spring's transient
 * exceptions when the driver is not on the classpath. The test loads a fresh copy of the executor through a class
 * loader that cannot see {@code com.mongodb}, which is what a classpath without the driver looks like to it, and
 * runs it. A copy that referred to the driver directly would fail here with a {@link NoClassDefFoundError}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class NoMongoDriverSpringReactiveTransactionExecutorTest {

    @Test
    void the_default_executor_loads_and_runs_the_action_again_on_a_transient_exception_when_the_mongo_driver_is_missing() throws Exception {
        // Given
        Object executor = executorLoadedWithoutTheMongoDriver();
        AtomicInteger attempts = new AtomicInteger();

        // When
        Object result = inTransaction(executor, () -> {
            if (attempts.incrementAndGet() <= 3) {
                throw new TransientDataAccessResourceException("conflict");
            }
            return "done";
        });

        // Then
        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(4);
    }

    @Test
    void the_default_executor_runs_the_action_once_on_other_failures_when_the_mongo_driver_is_missing() throws Exception {
        // Given
        Object executor = executorLoadedWithoutTheMongoDriver();
        AtomicInteger attempts = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("not a conflict");

        // When
        Throwable thrown = catchThrowable(() -> inTransaction(executor, () -> {
            attempts.incrementAndGet();
            throw failure;
        }));

        // Then
        assertThat(thrown).isSameAs(failure);
        assertThat(attempts).hasValue(1);
    }

    @Test
    void the_default_executor_does_not_look_for_the_label_when_the_mongo_driver_is_missing() throws Exception {
        // Given
        Object executor = executorLoadedWithoutTheMongoDriver();
        AtomicInteger attempts = new AtomicInteger();
        MongoException labelled = new MongoException("labelled");
        labelled.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);

        // When
        Throwable thrown = catchThrowable(() -> inTransaction(executor, () -> {
            attempts.incrementAndGet();
            throw labelled;
        }));

        // Then
        assertThat(thrown).isSameAs(labelled);
        assertThat(attempts).hasValue(1);
    }

    @Test
    void the_same_harness_runs_the_action_again_on_the_label_when_the_mongo_driver_is_visible() throws Exception {
        // The control: unless the driver is hidden, the harness gives the label back its meaning, so the test above
        // is not passing for a reason other than the missing driver.
        // Given
        Object executor = executorLoadedThrough(new CopyingClassLoader(false));
        AtomicInteger attempts = new AtomicInteger();

        // When
        Object result = inTransaction(executor, () -> {
            if (attempts.incrementAndGet() <= 3) {
                MongoException labelled = new MongoException("labelled");
                labelled.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
                throw labelled;
            }
            return "done";
        });

        // Then
        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(4);
    }

    private static Object executorLoadedWithoutTheMongoDriver() throws Exception {
        CopyingClassLoader classLoader = new CopyingClassLoader(true);
        assertThatThrownBy(() -> classLoader.loadClass("com.mongodb.MongoException")).isInstanceOf(ClassNotFoundException.class);
        return executorLoadedThrough(classLoader);
    }

    private static Object executorLoadedThrough(CopyingClassLoader classLoader) throws Exception {
        Class<?> type = classLoader.loadClass(SpringReactiveTransactionExecutor.class.getName());
        assertThat(type).isNotSameAs(SpringReactiveTransactionExecutor.class);
        assertThat(type.getClassLoader()).isSameAs(classLoader);
        return type.getConstructor(ReactiveTransactionManager.class).newInstance(new StubReactiveTransactionManager());
    }

    private static Object inTransaction(Object executor, Callable<Object> action) throws Exception {
        Method inTransaction = executor.getClass().getMethod("inTransaction", Supplier.class);
        Supplier<Mono<Object>> supplier = () -> Mono.fromCallable(action);
        try {
            return ((Mono<?>) inTransaction.invoke(executor, supplier)).block();
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
        }
    }

    private static Throwable catchThrowable(ThrowingRunnable runnable) {
        try {
            runnable.run();
            return null;
        } catch (Throwable throwable) {
            return throwable;
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * Defines its own copy of the executor, nested classes included, from the bytes the test classpath holds, and
     * passes everything else to its parent. With {@code hideMongoDriver} it cannot find {@code com.mongodb} or
     * {@code org.bson}, even though the test classpath has them.
     */
    private static final class CopyingClassLoader extends ClassLoader {
        private final boolean hideMongoDriver;

        CopyingClassLoader(boolean hideMongoDriver) {
            super(NoMongoDriverSpringReactiveTransactionExecutorTest.class.getClassLoader());
            this.hideMongoDriver = hideMongoDriver;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (hideMongoDriver && (name.startsWith("com.mongodb.") || name.startsWith("org.bson."))) {
                throw new ClassNotFoundException(name);
            }
            String executor = SpringReactiveTransactionExecutor.class.getName();
            if (name.equals(executor) || name.startsWith(executor + "$")) {
                synchronized (getClassLoadingLock(name)) {
                    Class<?> loaded = findLoadedClass(name);
                    if (loaded == null) {
                        byte[] bytes = bytesOf(name);
                        loaded = defineClass(name, bytes, 0, bytes.length);
                    }
                    return loaded;
                }
            }
            return super.loadClass(name, resolve);
        }

        private byte[] bytesOf(String name) throws ClassNotFoundException {
            try (InputStream stream = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                if (stream == null) {
                    throw new ClassNotFoundException(name);
                }
                return stream.readAllBytes();
            } catch (IOException e) {
                throw new ClassNotFoundException(name, e);
            }
        }
    }

    private static final class StubReactiveTransactionManager extends AbstractReactiveTransactionManager {

        @Override
        protected Object doGetTransaction(TransactionSynchronizationManager synchronizationManager) {
            return new Object();
        }

        @Override
        protected Mono<Void> doBegin(TransactionSynchronizationManager synchronizationManager, Object transaction, TransactionDefinition definition) {
            return Mono.empty();
        }

        @Override
        protected Mono<Void> doCommit(TransactionSynchronizationManager synchronizationManager, GenericReactiveTransaction status) {
            return Mono.empty();
        }

        @Override
        protected Mono<Void> doRollback(TransactionSynchronizationManager synchronizationManager, GenericReactiveTransaction status) {
            return Mono.empty();
        }
    }
}
