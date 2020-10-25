package org.occurrent.application.composition.command.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class CreateListFromVarArgs {

    @SuppressWarnings("unchecked")
    public static <T> List<T> createList(T first, T second, T[] additional) {
        requireNonNull(first, "First element to compose cannot be null");
        requireNonNull(second, "Second element to compose be null");
        T[] additionalElementsToUse = additional == null ? (T[]) Collections.emptyList().toArray() : additional;
        List<T> conditions = new ArrayList<>(2 + additionalElementsToUse.length);
        conditions.add(first);
        conditions.add(second);
        Collections.addAll(conditions, additionalElementsToUse);
        return conditions;
    }
}
