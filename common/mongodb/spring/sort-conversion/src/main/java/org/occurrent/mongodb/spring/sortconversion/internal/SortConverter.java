package org.occurrent.mongodb.spring.sortconversion.internal;

import org.occurrent.eventstore.api.SortBy;
import org.springframework.data.domain.Sort;

import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.springframework.data.domain.Sort.Direction.ASC;
import static org.springframework.data.domain.Sort.Direction.DESC;

/**
 * Convert Occurrent sort types to Spring sort types
 */
public class SortConverter {
    private static final String NATURAL = "$natural";

    /**
     * Convert {@link SortBy} to {@link Sort}
     *
     * @param sortBy The Occurrent {@code SortBy} instance to convert
     * @return A Spring {@code Sort} instance.
     */
    public static Sort convertToSpringSort(SortBy sortBy) {
        return switch (sortBy) {
            case SortBy.Unsorted ignored -> Sort.unsorted();
            case SortBy.NaturalImpl natural -> Sort.by(toDirection(natural.direction), NATURAL);
            case SortBy.SingleFieldImpl singleField -> Sort.by(toDirection(singleField.direction), singleField.fieldName);
            case SortBy.MultipleSortStepsImpl multipleSortSteps -> convertMultipleSteps(multipleSortSteps);
        };
    }

    // A natural sort step is already a total ordering, so combining it with other sort steps in a compound sort is
    // semantically incoherent, and Occurrent never builds such a sort itself. MongoDB 7.0+ also rejects $natural
    // inside a compound sort server-side (BadValue: "$natural sort cannot be set to a value other than -1 or 1"), so
    // reject it here instead of silently degrading it, which is what older MongoDB (4.x) did by applying pure
    // natural order and ignoring the other keys.
    private static Sort convertMultipleSteps(SortBy.MultipleSortStepsImpl multipleSortSteps) {
        if (multipleSortSteps.steps.stream().anyMatch(SortBy.NaturalImpl.class::isInstance)) {
            throw new IllegalArgumentException("A natural sort step cannot be combined with other sort steps, since natural order is already a total ordering. Use natural sort alone.");
        }
        return multipleSortSteps.steps.stream()
                .map(SortConverter::convertToSpringSort)
                .reduce(Sort::and)
                .orElseThrow(() -> new IllegalStateException("Internal error: Expecting " + SortBy.MultipleSortStepsImpl.class.getSimpleName() + " to have at least one step"));
    }

    private static Sort.Direction toDirection(SortBy.SortDirection sortDirection) {
        return sortDirection == ASCENDING ? ASC : DESC;
    }
}
