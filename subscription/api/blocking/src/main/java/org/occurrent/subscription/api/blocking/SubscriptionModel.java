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

package org.occurrent.subscription.api.blocking;

/**
 * Common interface for blocking subscription models. The purpose of a subscription is to read events from an event store
 * and react to these events.
 * <p>
 * A subscription may be used to create read models (such as views, projections, sagas, snapshots etc) or
 * forward the event to another piece of infrastructure such as a message bus or other eventing infrastructure.
 * <p>
 * A blocking subscription model also you to create and manage subscriptions that'll use blocking IO.
 */
public interface SubscriptionModel extends Subscribable, SubscriptionModelLifeCycle {
}