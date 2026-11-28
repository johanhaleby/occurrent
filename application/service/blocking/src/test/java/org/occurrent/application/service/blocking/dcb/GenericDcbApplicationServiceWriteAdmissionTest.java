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
package org.occurrent.application.service.blocking.dcb;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.DcbEventStore;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * A write asks {@code beforeWrite()} also when the dispatcher has no subscriptions, and a refusal stops it before the
 * event store is reached. Container-free, since nothing reaches the event store.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class GenericDcbApplicationServiceWriteAdmissionTest {

    @Test
    void a_write_is_refused_by_before_write_before_the_event_store_is_reached_when_the_dispatcher_has_no_subscriptions() {
        IllegalStateException refusal = new IllegalStateException("write refused");
        RefusingDispatcher dispatcher = new RefusingDispatcher(refusal);
        var applicationService = GenericDcbApplicationService.builder(unreachableEventStore(), new UnusedConverter())
                .synchronousSubscriptions(dispatcher)
                .build();

        Throwable thrown = catchThrowable(() -> applicationService.execute(tags(Tag.parse("a=b")), events -> List.of("event")));

        assertThat(thrown).as("[what the write threw]").isSameAs(refusal);
        assertThat(dispatcher.dispatches).as("[times dispatch was called]").hasValue(0);
    }

    private static DcbEventStore unreachableEventStore() {
        return (DcbEventStore) Proxy.newProxyInstance(DcbEventStore.class.getClassLoader(), new Class<?>[]{DcbEventStore.class}, (proxy, method, args) -> {
            throw new AssertionError("The event store is not reached, but " + method.getName() + " was called");
        });
    }

    private static final class RefusingDispatcher implements SynchronousEventDispatcher {
        final AtomicInteger dispatches = new AtomicInteger();
        private final RuntimeException refusal;

        RefusingDispatcher(RuntimeException refusal) {
            this.refusal = refusal;
        }

        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            dispatches.incrementAndGet();
        }

        @Override
        public boolean hasSubscriptions() {
            return false;
        }

        @Override
        public WriteCheck beforeWrite() {
            throw refusal;
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
