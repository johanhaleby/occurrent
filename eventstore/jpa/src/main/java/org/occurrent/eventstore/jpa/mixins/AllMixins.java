package org.occurrent.eventstore.jpa.mixins;

public interface AllMixins<T> extends EventLogFilterMixin<T>, EventLogSortingMixin<T> {}
