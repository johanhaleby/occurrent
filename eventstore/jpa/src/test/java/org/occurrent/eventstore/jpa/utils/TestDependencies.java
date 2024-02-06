package org.occurrent.eventstore.jpa.utils;

import org.occurrent.eventstore.api.blocking.EventStore;

public record TestDependencies<T extends EventStore>(T eventStore) {
}

