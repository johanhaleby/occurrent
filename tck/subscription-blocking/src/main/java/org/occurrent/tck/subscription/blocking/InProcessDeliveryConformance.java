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

package org.occurrent.tck.subscription.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.tck.ConformanceEvents;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.tck.ConformanceEvents.idsOf;

/**
 * The stronger promise a model makes when it dispatches in the publishing thread rather than handing the event to one of
 * its own.
 * <p>
 * Extend it only if your model calls the handler on the thread that published the event. A model that delivers
 * asynchronously declines by not extending it, and there is deliberately no fixture flag for this: ADR 94 records why a
 * flag would be a switch for turning off the only test of the property. Nothing guards the suite either, so a model that
 * does deliver asynchronously fails the first assertion rather than being turned away, which is what keeps the suite open
 * to an implementation from outside this repository.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the in-process delivery contract")
@Timeout(60)
public abstract class InProcessDeliveryConformance extends SubscriptionModelSuite {

    private static String subscriptionId() {
        return UUID.randomUUID().toString();
    }

    @Test
    void the_handler_has_already_run_when_publishing_returns() {
        List<String> received = new ArrayList<>();
        fixture().subscriptionModel().subscribe(subscriptionId(), cloudEvent -> received.add(cloudEvent.getId()));
        CloudEvent event = ConformanceEvents.event("1", "NameDefined");

        fixture().publish(List.of(event));

        assertThat(received)
                .as("this model delivers in the publishing thread, so a read straight after publishing already sees "
                        + "the result. That is the whole reason to choose it over an asynchronous model, and a "
                        + "regression to an executor would leave every other assertion in the TCK passing")
                .containsExactly(event.getId());
    }

    @Test
    void the_handler_runs_on_the_publishing_thread() {
        List<Thread> handlerThreads = new ArrayList<>();
        Thread publishingThread = Thread.currentThread();
        fixture().subscriptionModel().subscribe(subscriptionId(), cloudEvent -> handlerThreads.add(Thread.currentThread()));

        fixture().publish(List.of(ConformanceEvents.event("1", "NameDefined")));

        assertThat(handlerThreads)
                .as("not merely finished before publishing returned, but run by the publishing thread itself, which is "
                        + "what lets a caller wrap the write and the handler in one transaction")
                .containsExactly(publishingThread);
    }

    @Test
    void handlers_run_in_registration_order() {
        if (!fixture().acceptsSeveralSubscriptions()) {
            // A model feeding one subscription has no order to promise. The refusal is asserted by
            // SubscriptionModelConformance rather than restated here.
            return;
        }
        List<String> calls = new ArrayList<>();
        fixture().subscriptionModel().subscribe("first", cloudEvent -> calls.add("first:" + cloudEvent.getId()));
        fixture().subscriptionModel().subscribe("second", cloudEvent -> calls.add("second:" + cloudEvent.getId()));

        fixture().publish(List.of(ConformanceEvents.event("1", "NameDefined")));

        assertThat(calls)
                .as("registration order, so a caller can rely on one handler having run before the next")
                .containsExactly("first:1", "second:1");
    }

    @Test
    void every_event_in_one_publish_reaches_the_handler_in_order() {
        List<CloudEvent> received = new ArrayList<>();
        fixture().subscriptionModel().subscribe(subscriptionId(), received::add);
        CloudEvent first = ConformanceEvents.event("1", "NameDefined");
        CloudEvent second = ConformanceEvents.event("2", "NameWasChanged");

        fixture().publish(List.of(first, second));

        assertThat(idsOf(received)).containsExactly(first.getId(), second.getId());
    }
}
