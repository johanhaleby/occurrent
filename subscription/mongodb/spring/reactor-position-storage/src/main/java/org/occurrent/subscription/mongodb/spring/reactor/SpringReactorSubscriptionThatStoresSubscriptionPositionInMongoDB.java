/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription.mongodb.spring.reactor;

import io.cloudevents.CloudEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.reactor.PositionAwareReactorSubscription;
import org.occurrent.subscription.api.reactor.ReactorSubscriptionPositionStorage;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link PositionAwareReactorSubscription} and adds persistent subscription position support. It adds some convenience methods that stores the subscription position
 * after an "action" (the "function" in this method {@link SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB#subscribe(String, Function)}) has completed successfully.
 * It stores the subscription position in MongoDB. Note that it doesn't have to be the same MongoDB database that stores the actual events.
 * <p>
 * Note that this implementation stores the subscription position after _every_ action. If you have a lot of events and duplication is not
 * that much of a deal consider cloning/extending this class and add your own customizations. Use the methods provided by a {@link ReactorSubscriptionPositionStorage}
 * implementation.
 */
public class SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB {
    private static final Logger log = LoggerFactory.getLogger(SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB.class);
    private final PositionAwareReactorSubscription subscription;
    private final ReactorSubscriptionPositionStorage storage;

    public SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB(PositionAwareReactorSubscription subscription, ReactorSubscriptionPositionStorage storage) {
        this.subscription = subscription;
        this.storage = storage;
        requireNonNull(subscription, PositionAwareReactorSubscription.class.getSimpleName() + " cannot be null");
        requireNonNull(storage, ReactorSubscriptionPositionStorage.class.getSimpleName() + " cannot be null");

    }

    /**
     * A convenience function that automatically starts from the latest persisted subscription position and saves the new position after each call to {@code action}
     * has completed successfully. If you don't want to save the position after every event then don't use this method and instead save the position yourself by calling
     * {@link SpringReactorSubscriptionPositionStorageForMongoDB#save(String, SubscriptionPosition)} when appropriate.
     * <p>
     * It's VERY important that side-effects take place within the <code>action</code> function
     * because if you perform side-effects on the returned <code>Mono<Void></code> stream then the subscription position
     * has already been stored in MongoDB and the <code>action</code> will not be re-run if side-effect fails.
     * </p>
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     * @return A stream of {@link CloudEvent}'s. The subscription position of the cloud event will already have been persisted when consumed by this stream so use <code>action</code> to perform side-effects.
     */
    public Mono<Void> subscribe(String subscriptionId, Function<CloudEvent, Mono<Void>> action) {
        return subscribe(subscriptionId, null, action);
    }

    /**
     * A convenience function that automatically starts from the latest persisted subscription position and saves the new position after each call to {@code action}
     * has completed successfully. If you don't want to save the position after every event then don't use this method and instead save the position yourself by calling
     * {@link SpringReactorSubscriptionPositionStorageForMongoDB#save(String, SubscriptionPosition)} when appropriate.
     *
     * <p>
     * It's VERY important that side-effects take place within the <code>action</code> function
     * because if you perform side-effects on the returned <code>Mono<Void></code> stream then the subscription position
     * has already been stored in MongoDB and the <code>action</code> will not be re-run if side-effect fails.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The {@link SubscriptionFilter} to use to limit the events receive by the event store
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     * @return A stream of {@link CloudEvent}'s. The subscription position of the cloud event will already have been persisted when consumed by this stream so use <code>action</code> to perform side-effects.
     */
    public Mono<Void> subscribe(String subscriptionId, SubscriptionFilter filter, Function<CloudEvent, Mono<Void>> action) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        return findStartAtForSubscription(subscriptionId)
                .doOnNext(startAt -> log.info("Starting subscription {} from subscription position {}", subscriptionId, startAt.toString()))
                .flatMapMany(startAt -> subscription.subscribe(filter, startAt))
                .flatMap(cloudEventWithStreamPosition -> action.apply(cloudEventWithStreamPosition).thenReturn(cloudEventWithStreamPosition))
                .flatMap(cloudEventWithStreamPosition -> storage.save(subscriptionId, cloudEventWithStreamPosition.getStreamPosition()).thenReturn(cloudEventWithStreamPosition))
                .then();
    }

    /**
     * Find the calculate the current {@link StartAt} value for the {@code subscriptionId}. It creates the {@link StartAt} instance from the
     * global subscription position (from {@link PositionAwareReactorSubscription#globalSubscriptionPosition()}) if no
     * {@code SubscriptionPosition} is found for the given subscription.
     *
     * @param subscriptionId The id of the subscription whose position to find
     * @return A Mono with the {@link SubscriptionPosition} data point for the supplied subscriptionId
     */
    public Mono<StartAt> findStartAtForSubscription(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        return storage.read(subscriptionId)
                .doOnNext(document -> log.info("Found subscription position: {}", document))
                .switchIfEmpty(Mono.defer(() -> {
                    log.info("No subscription position found for {}, will initialize a new one.", subscriptionId);
                    return subscription.globalSubscriptionPosition()
                            .flatMap(subscriptionPosition -> storage.save(subscriptionId, subscriptionPosition));
                }))
                .map(StartAt::subscriptionPosition);
    }
}