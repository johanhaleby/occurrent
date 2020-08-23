package org.occurrent.subscription;

import org.occurrent.filter.Filter;

import java.util.Objects;

/**
 * An implementation of {@code SubscriptionFilter} for Occurrent {@link Filter}'s.
 */
public class OccurrentSubscriptionFilter implements SubscriptionFilter {

    public final Filter filter;

    public OccurrentSubscriptionFilter(Filter filter) {
        Objects.requireNonNull(filter, Filter.class.getSimpleName() + " cannot be null");
        this.filter = filter;
    }

    public static OccurrentSubscriptionFilter filter(Filter filter) {
        return new OccurrentSubscriptionFilter(filter);
    }
}