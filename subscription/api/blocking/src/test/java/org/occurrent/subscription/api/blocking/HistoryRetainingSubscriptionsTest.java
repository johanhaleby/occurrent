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

package org.occurrent.subscription.api.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;

import java.net.URI;
import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Covers {@link HistoryRetainingSubscriptions#findIn(SubscriptionModelCapability)}, the lookup a caller makes before it
 * may hand an event back unprocessed. The saga executor is that caller. Quarantining acknowledges the failing event, so
 * it has to know the event is still somewhere before it does.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class HistoryRetainingSubscriptionsTest {

    @Test
    void finds_the_model_itself_when_it_retains() {
        RetainingModel model = new RetainingModel();

        Optional<HistoryRetainingSubscriptions> found = HistoryRetainingSubscriptions.findIn(model);

        assertThat(found).containsSame(model);
    }

    @Test
    void unwraps_a_delegating_model_to_reach_the_retaining_one() {
        RetainingModel inner = new RetainingModel();

        Optional<HistoryRetainingSubscriptions> found = HistoryRetainingSubscriptions.findIn(new Wrapper(inner));

        assertThat(found).containsSame(inner);
    }

    @Test
    void unwraps_through_several_layers_of_wrapping() {
        RetainingModel inner = new RetainingModel();

        Optional<HistoryRetainingSubscriptions> found = HistoryRetainingSubscriptions.findIn(new Wrapper(new Wrapper(inner)));

        assertThat(found).containsSame(inner);
    }

    @Test
    void is_empty_when_nothing_in_the_chain_retains() {
        Optional<HistoryRetainingSubscriptions> found = HistoryRetainingSubscriptions.findIn(new Wrapper(new PlainModel()));

        assertThat(found).isEmpty();
    }

    /**
     * The answer a caller has to read as the event being gone once it returns. Stated as its own test because empty is
     * the whole of the negative contract. There is no method to answer no with, so a model that keeps nothing says so
     * by not implementing the capability.
     */
    @Test
    void is_empty_for_a_model_that_declares_nothing() {
        assertThat(HistoryRetainingSubscriptions.findIn(new PlainModel())).isEmpty();
    }

    /**
     * A wrapper may still answer for itself, which is what the lookup order allows rather than what any model in this
     * repository does today. A model that reads a store on the way up but takes its live events from somewhere else is
     * exactly the case that must not try, because it cannot know the two hold the same events.
     */
    @Test
    void a_retaining_wrapper_answers_for_itself_over_a_delegate_that_retains_nothing() {
        RetainingWrapper wrapper = new RetainingWrapper(new PlainModel());

        Optional<HistoryRetainingSubscriptions> found = HistoryRetainingSubscriptions.findIn(wrapper);

        assertThat(found).containsSame(wrapper);
    }

    /**
     * Finding the capability is not the same as being told the event is there. A model that reads a store its live
     * source may not write to is present and still answers no, which is why the saga asks per event rather than once
     * at startup.
     */
    @Test
    void a_model_that_retains_some_events_is_still_found_and_still_answers_no_for_the_others() {
        SometimesRetainingModel model = new SometimesRetainingModel();

        HistoryRetainingSubscriptions found = HistoryRetainingSubscriptions.findIn(model).orElseThrow();

        assertAll(
                () -> assertThat(found.retains(event("kept"))).isTrue(),
                () -> assertThat(found.retains(event("gone"))).isFalse(),
                () -> assertThat(found.retainsEveryEvent()).isFalse()
        );
    }

    /**
     * The default is the answer that costs a startup message rather than the one that suppresses it, so a model
     * saying nothing is treated as one whose answer can vary.
     */
    @Test
    void a_model_that_does_not_say_is_not_taken_to_retain_everything() {
        assertThat(new SometimesRetainingModel().retainsEveryEvent()).isFalse();
        assertThat(new RetainingModel().retainsEveryEvent()).isTrue();
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test").build();
    }

    private static class PlainModel implements SubscriptionModel {
        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }
    }

    private static final class RetainingModel extends PlainModel implements HistoryRetainingSubscriptions {
        @Override
        public boolean retains(CloudEvent event) {
            return true;
        }

        @Override
        public boolean retainsEveryEvent() {
            return true;
        }
    }

    // Present, and still answers no for a given event, which is the shape of a model reading a store its live source
    // may not write to.
    private static final class SometimesRetainingModel extends PlainModel implements HistoryRetainingSubscriptions {
        @Override
        public boolean retains(CloudEvent event) {
            return "kept".equals(event.getId());
        }
    }

    // Both interfaces, the way every real wrapper in this repository is shaped.
    private static class Wrapper extends PlainModel implements SubscriptionModelWrapper {
        private final SubscriptionModel delegate;

        private Wrapper(SubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return delegate;
        }
    }

    // Declared here to cover the lookup order, not to model a real subscription model.
    private static final class RetainingWrapper extends Wrapper implements HistoryRetainingSubscriptions {
        private RetainingWrapper(SubscriptionModel delegate) {
            super(delegate);
        }

        @Override
        public boolean retains(CloudEvent event) {
            return true;
        }
    }
}
