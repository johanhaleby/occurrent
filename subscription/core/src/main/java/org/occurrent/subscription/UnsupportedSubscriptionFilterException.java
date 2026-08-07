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

package org.occurrent.subscription;

/**
 * A subscription filter of a shape this model cannot apply, so the call was refused. Accepting it and ignoring it
 * would deliver events the caller asked not to receive.
 * <p>
 * This is about the shape of the filter that was passed. A model that only replays a stream refusing a
 * {@link DcbSubscriptionFilter} is this, and so is any {@link SubscriptionFilter} implementation Occurrent does not
 * ship. A model that could apply the filter but was built without the means to, such as one handed no
 * {@code DataFieldReader} and asked to filter on a payload field, throws {@link UnsupportedOperationException}
 * instead, because passing a different filter is not what fixes it.
 */
public final class UnsupportedSubscriptionFilterException extends SubscriptionRefusedException {

    private final Class<? extends SubscriptionFilter> filterType;

    /**
     * Creates an exception with the standard message. This is the message every Occurrent subscription model
     * produces, so prefer this constructor over supplying your own.
     *
     * @param filterType The type of the filter that was refused
     */
    public UnsupportedSubscriptionFilterException(Class<? extends SubscriptionFilter> filterType) {
        this(filterType, "Unsupported " + SubscriptionFilter.class.getSimpleName() + " type: " + filterType.getName() + ".");
    }

    /**
     * Creates an exception with a message of your own, which is how a model names the filter shapes it does accept.
     *
     * @param filterType The type of the filter that was refused
     * @param message    The message to report
     */
    public UnsupportedSubscriptionFilterException(Class<? extends SubscriptionFilter> filterType, String message) {
        super(message);
        this.filterType = filterType;
    }

    /**
     * @return The type of the subscription filter that was refused
     */
    public Class<? extends SubscriptionFilter> filterType() {
        return filterType;
    }
}
