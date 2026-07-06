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
            case SortBy.MultipleSortStepsImpl multipleSortSteps -> multipleSortSteps.steps.stream()
                    .map(SortConverter::convertToSpringSort)
                    .reduce(Sort::and)
                    .orElseThrow(() -> new IllegalStateException("Internal error: Expecting " + SortBy.MultipleSortStepsImpl.class.getSimpleName() + " to have at least one step"));
        };
    }

    private static Sort.Direction toDirection(SortBy.SortDirection sortDirection) {
        return sortDirection == ASCENDING ? ASC : DESC;
    }
}
