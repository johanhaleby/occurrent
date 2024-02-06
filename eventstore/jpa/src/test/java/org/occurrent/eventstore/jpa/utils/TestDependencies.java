package org.occurrent.eventstore.jpa.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.occurrent.eventstore.api.blocking.EventStore;

public record TestDependencies<T extends EventStore>(T eventStore, ObjectMapper objectMapper) {}
