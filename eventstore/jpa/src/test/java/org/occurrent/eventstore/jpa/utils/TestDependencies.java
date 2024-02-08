package org.occurrent.eventstore.jpa.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.occurrent.eventstore.jpa.CloudEventDaoTraits;
import org.occurrent.eventstore.jpa.JPAEventStore;

public record TestDependencies<T extends CloudEventDaoTraits, E extends JPAEventStore<T>>(
    E eventStore, ObjectMapper objectMapper) {}
