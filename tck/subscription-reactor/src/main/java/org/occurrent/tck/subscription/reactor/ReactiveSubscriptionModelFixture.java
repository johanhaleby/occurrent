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

package org.occurrent.tck.subscription.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.api.reactor.SubscriptionModel;

import java.time.Duration;
import java.util.List;

/**
 * What a reactive subscription model hands {@link ReactiveSubscriptionModelConformance}.
 * <p>
 * Deliberately smaller than {@link org.occurrent.tck.subscription.blocking.SubscriptionModelFixture}, and not a
 * subtype of it. The behavioural contract is asserted once, through {@link BlockingSubscriptionOverReactive} and the
 * blocking suites, so an implementation supplies the blocking fixture for all of that. This one exists only for what
 * blocking on a result destroys.
 * <p>
 * As with the blocking fixture, a fresh fixture is created for every test method, the model handed back
 * <strong>must have no subscriptions</strong>, {@link #close()} must not drop a collection or database a live change
 * stream watches, and no event id is ever published twice.
 */
@NullMarked
public interface ReactiveSubscriptionModelFixture {

    /**
     * The model under test, with no subscriptions on it.
     */
    SubscriptionModel subscriptionModel();

    /**
     * Hands the events to whatever feeds this model, in order, exactly like the blocking fixture's
     * {@code publish(..)}.
     * <p>
     * <strong>This is allowed to throw.</strong> On a model that propagates a failed action rather than retrying it,
     * delivery happens inside this call, so an action whose {@code Mono} errors comes back out of here. The suite
     * accepts either, because which of the two a model does is the blocking fixture's declaration and is asserted
     * there. Here only the model's survival is asserted.
     *
     * @param events The events to feed in, in order. Never empty, and no id is ever repeated within a test.
     */
    void publish(List<CloudEvent> events);

    /**
     * The longest {@link ReactiveSubscriptionModelConformance} will wait for anything that must happen.
     * <p>
     * Twenty seconds by default, generous because it only has to exceed a working model's delivery latency and a model
     * that has genuinely stalled fails either way. A model that has to reach a broker before it can deliver declares
     * what it needs here.
     * <p>
     * <strong>This is a bound, not a delay</strong>, on the same terms as
     * {@link org.occurrent.tck.subscription.blocking.SubscriptionModelFixture#deliveryTimeout()}, which is the budget
     * for everything the blocking suites assert about this model through {@link BlockingSubscriptionOverReactive}. The
     * two are declared separately because they bound different things. This suite waits on the two publishers a bridge
     * destroys, and it carries no class timeout, so nothing here caps what you declare.
     */
    default Duration deliveryTimeout() {
        return Duration.ofSeconds(20);
    }

    /**
     * Releases whatever the fixture opened, and shuts the model down. Called after every test method, including a
     * failing one.
     */
    default void close() {
    }
}
