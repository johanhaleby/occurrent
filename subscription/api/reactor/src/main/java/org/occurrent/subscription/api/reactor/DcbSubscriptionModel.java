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

package org.occurrent.subscription.api.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.subscription.DcbStartAt;
import reactor.core.publisher.Flux;

/**
 * A typed reactive view over a {@link SubscriptionModel} that subscribes to DCB events selected by a {@link DcbQuery}.
 * <p>
 * It is the DCB counterpart to the reactive {@link SubscriptionModel}, accepting a {@link DcbQuery} and a
 * {@link DcbStartAt} rather than a stream filter and a generic start position. The {@link DcbStartAt} is passed through
 * to the underlying {@link SubscriptionModel}, so whether a replay-oriented start such as {@link DcbStartAt#beginning()}
 * or {@link DcbStartAt#afterPosition(long)} replays history depends on that model. The current reactive subscription
 * models have no DCB catch-up, so such a start behaves like a live start today.
 */
@NullMarked
public interface DcbSubscriptionModel {

    /**
     * Subscribe to DCB events matching {@code query}, starting at {@code startAt}.
     *
     * @return a {@link Flux} of the matching cloud events.
     */
    Flux<CloudEvent> subscribe(DcbQuery query, DcbStartAt startAt);

    /**
     * Subscribe to DCB events matching {@code query} at the subscription model default position.
     */
    default Flux<CloudEvent> subscribe(DcbQuery query) {
        return subscribe(query, DcbStartAt.subscriptionModelDefault());
    }

    /**
     * Subscribe to every DCB event at the subscription model default position.
     */
    default Flux<CloudEvent> subscribe() {
        return subscribe(DcbQuery.all());
    }

    /**
     * Create a DCB view over an existing reactive {@link SubscriptionModel}.
     */
    static DcbSubscriptionModel from(SubscriptionModel delegate) {
        return new DcbSubscriptionModelAdapter(delegate);
    }
}
