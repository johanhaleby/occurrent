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

package org.occurrent.application.service.reactor.dcb;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.ExecutorScope;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.application.service.reactor.ReactiveTransactionExecutor;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * The check {@code beforeWrite()} hands out makes a pause or a stop wait until it is closed, so a command that fails
 * while its pipeline is built must still close it. Container-free, since nothing reaches the event store.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveGenericDcbApplicationServiceWriteCheckTest {

    @Test
    void the_write_check_is_closed_once_when_building_the_pipeline_throws() {
        IllegalStateException failure = new IllegalStateException("hasSubscriptions failed");
        ClosesOnce check = new ClosesOnce();
        DcbApplicationService<String> applicationService = GenericDcbApplicationService.builder(unreachableEventStore(), new UnusedConverter())
                .synchronousSubscriptions(new ThrowingDispatcher(check, failure))
                .build();

        StepVerifier.create(applicationService.execute(tags(Tag.parse("a=b")), events -> List.of("event")))
                .expectErrorSatisfies(error -> assertThat(error).as("[error the Mono failed with]").isSameAs(failure))
                .verify();

        assertThat(check.closes).as("[times the write check was closed]").hasValue(1);
    }

    @Test
    void the_write_check_is_closed_once_when_the_transaction_executor_throws_while_the_pipeline_is_built() {
        IllegalStateException failure = new IllegalStateException("inTransactionTellingIfOpened failed");
        ClosesOnce check = new ClosesOnce();
        DcbApplicationService<String> applicationService = GenericDcbApplicationService.builder(unreachableEventStore(), new UnusedConverter())
                .synchronousSubscriptions(new SubscribedDispatcher(check))
                .transactionExecutor(new ThrowingTransactionExecutor(failure))
                .build();

        StepVerifier.create(applicationService.execute(tags(Tag.parse("a=b")), events -> List.of("event")))
                .expectErrorSatisfies(error -> assertThat(error).as("[error the Mono failed with]").isSameAs(failure))
                .verify();

        assertThat(check.closes).as("[times the write check was closed]").hasValue(1);
    }

    private static DcbEventStore unreachableEventStore() {
        return (DcbEventStore) Proxy.newProxyInstance(DcbEventStore.class.getClassLoader(), new Class<?>[]{DcbEventStore.class}, (proxy, method, args) -> {
            throw new AssertionError("The event store is not reached, but " + method.getName() + " was called");
        });
    }

    private static final class ClosesOnce implements ReactiveSynchronousEventDispatcher.WriteCheck {
        final AtomicInteger closes = new AtomicInteger();

        @Override
        public void check(List<CloudEvent> newEvents) {
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    private record ThrowingDispatcher(WriteCheck check, RuntimeException failure) implements ReactiveSynchronousEventDispatcher {
        @Override
        public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            return Mono.empty();
        }

        @Override
        public boolean hasSubscriptions() {
            throw failure;
        }

        @Override
        public WriteCheck beforeWrite() {
            return check;
        }
    }

    private record SubscribedDispatcher(WriteCheck check) implements ReactiveSynchronousEventDispatcher {
        @Override
        public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            return Mono.empty();
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }

        @Override
        public WriteCheck beforeWrite() {
            return check;
        }
    }

    private record ThrowingTransactionExecutor(RuntimeException failure) implements ReactiveTransactionExecutor {
        @Override
        public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
            throw failure;
        }

        @Override
        public <T> Mono<T> inTransactionTellingIfOpened(Function<ExecutorScope, Mono<T>> action) {
            throw failure;
        }
    }

    private static final class UnusedConverter implements CloudEventConverter<String> {
        @Override
        public CloudEvent toCloudEvent(String domainEvent) {
            throw new AssertionError("Not reached");
        }

        @Override
        public String toDomainEvent(CloudEvent cloudEvent) {
            throw new AssertionError("Not reached");
        }

        @Override
        public String getCloudEventType(Class<? extends String> type) {
            throw new AssertionError("Not reached");
        }
    }
}
