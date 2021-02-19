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
        final Sort sort;
        if (sortBy instanceof SortBy.NaturalImpl) {
            sort = Sort.by(toDirection(((SortBy.NaturalImpl) sortBy).direction), NATURAL);
        } else if (sortBy instanceof SortBy.SingleFieldImpl) {
            SortBy.SingleFieldImpl singleField = (SortBy.SingleFieldImpl) sortBy;
            sort = Sort.by(toDirection(singleField.direction), singleField.fieldName);
        } else if (sortBy instanceof SortBy.MultipleSortStepsImpl) {
            sort = ((SortBy.MultipleSortStepsImpl) sortBy).steps.stream()
                    .map(SortConverter::convertToSpringSort)
                    .reduce(Sort::and)
                    .orElseThrow(() -> new IllegalStateException("Internal error: Expecting " + SortBy.MultipleSortStepsImpl.class.getSimpleName() + " to have at least one step"));
        } else {
            throw new IllegalArgumentException("Internal error: Unrecognized " + SortBy.class.getSimpleName() + " instance: " + sortBy.getClass().getSimpleName());
        }
        return sort;
    }

    private static Sort.Direction toDirection(SortBy.SortDirection sortDirection) {
        return sortDirection == ASCENDING ? ASC : DESC;
    }
}
