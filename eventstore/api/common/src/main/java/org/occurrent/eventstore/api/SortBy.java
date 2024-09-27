package org.occurrent.eventstore.api;

import io.cloudevents.core.v1.CloudEventV1;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.util.*;
import java.util.stream.Stream;

import static io.cloudevents.core.v1.CloudEventV1.TIME;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.occurrent.eventstore.api.SortBy.SortDirection.DESCENDING;

/**
 * A fluent API for specifying sort order for queries. For example:
 * <p></p>
 * <pre>
 * var allEvents = eventStoreQueries.all(SortBy.time(DESCENDING));
 * </pre>
 */
public sealed interface SortBy {
    /**
     * Use unsorted search order.
     */
    static Unsorted unsorted() {
        return new Unsorted();
    }

    /**
     * Sort by natural order in the supplied direction. This is typically the insertion order,
     * but it could also be undefined for certain datastores.
     *
     * @param direction The direction
     * @return A new instance of {@link SortBy}.
     */
    static Natural natural(SortDirection direction) {
        return new NaturalImpl(direction);
    }

    /**
     * Sort by time in the supplied direction.
     *
     * @param direction The direction
     * @return A new instance of {@link SortBy}.
     */
    static SingleField time(SortDirection direction) {
        return field(TIME, direction);
    }

    /**
     * Combines multiple fields in ascending order.
     *
     * @return A new instance of {@link SortBy}.
     */
    static ComposableSortStep ascending(String field1, String... fields) {
        return combine(field1, ASCENDING, fields);
    }

    /**
     * Combines multiple fields in descending order.
     *
     * @return A new instance of {@link SortBy}.
     */
    static ComposableSortStep descending(String field1, String... fields) {
        return combine(field1, DESCENDING, fields);
    }

    /**
     * Sort by {@value OccurrentCloudEventExtension#STREAM_VERSION} in the supplied direction.
     *
     * @param direction The direction
     * @return A new instance of {@link SortBy}.
     */
    static SingleField streamVersion(SortDirection direction) {
        return field(STREAM_VERSION, direction);
    }

    /**
     * Sort by the given field and direction. See {@link CloudEventV1} for valid fields.
     *
     * @param direction The direction
     * @return A new instance of {@link SortBy}.
     */
    static SingleField field(String fieldName, SortDirection direction) {
        return new SingleFieldImpl(fieldName, direction);
    }

    final class Unsorted implements SortBy {
    }

    sealed interface Natural extends SortBy {
    }

    final class NaturalImpl implements Natural {
        public final SortDirection direction;

        private NaturalImpl(SortDirection direction) {
            Objects.requireNonNull(direction, SortDirection.class.getSimpleName() + " cannot be null");
            this.direction = direction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof NaturalImpl natural)) return false;
            return direction == natural.direction;
        }

        @Override
        public int hashCode() {
            return Objects.hash(direction);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", NaturalImpl.class.getSimpleName() + "[", "]")
                    .add("direction=" + direction)
                    .toString();
        }
    }

    sealed interface SingleField extends ComposableSortStep {

        /**
         * Combines this sorting step and with another step such that the latter is applied only when the former considered values equal.
         *
         * @param direction The direction
         * @return A new instance of {@link SortBy}.
         */
        default SortBy thenNatural(SortDirection direction) {
            return then(new NaturalImpl(direction));
        }

        @Override
        default MultipleSortStepsImpl then(ComposableSortStep next) {
            return then((SortBy) next);
        }

        private MultipleSortStepsImpl then(SortBy other) {
            List<SortBy> list = Arrays.asList(this, other);
            return new MultipleSortStepsImpl(list);
        }
    }


    final class SingleFieldImpl implements SingleField {
        public final String fieldName;
        public final SortDirection direction;

        private SingleFieldImpl(String fieldName, SortDirection direction) {
            Objects.requireNonNull(fieldName, "Field name cannot be null");
            Objects.requireNonNull(direction, SortDirection.class.getSimpleName() + " cannot be null");
            this.fieldName = fieldName;
            this.direction = direction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SingleFieldImpl that)) return false;
            return Objects.equals(fieldName, that.fieldName) && direction == that.direction;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, direction);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", SingleFieldImpl.class.getSimpleName() + "[", "]")
                    .add("fieldName='" + fieldName + "'")
                    .add("direction=" + direction)
                    .toString();
        }
    }

    sealed interface MultipleSortSteps extends ComposableSortStep {

        /**
         * Combines this sorting step and with another step such that the latter is applied only when the former considered values equal.
         *
         * @param direction The direction
         * @return A new instance of {@link SortBy}.
         */
        default SortBy thenNatural(SortDirection direction) {
            return thenMerge(new NaturalImpl(direction));
        }

        @Override
        default MultipleSortSteps then(ComposableSortStep next) {
            return thenMerge(next);
        }

        MultipleSortSteps thenMerge(SortBy next);
    }

    final class MultipleSortStepsImpl implements MultipleSortSteps {
        public List<SortBy> steps;

        private MultipleSortStepsImpl(List<SortBy> steps) {
            Objects.requireNonNull(steps, "Steps cannot be null");
            if (steps.stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException("Steps cannot contain null step");
            }
            this.steps = Collections.unmodifiableList(steps);
        }

        @Override
        public MultipleSortSteps thenMerge(SortBy next) {
            Objects.requireNonNull(next, ComposableSortStep.class.getSimpleName() + " cannot be null");
            List<SortBy> newSteps = new ArrayList<>(steps);
            if (next instanceof MultipleSortStepsImpl) {
                newSteps.addAll(((MultipleSortStepsImpl) next).steps);
            } else {
                newSteps.add(next);
            }
            return new MultipleSortStepsImpl(newSteps);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MultipleSortStepsImpl that)) return false;
            return Objects.equals(steps, that.steps);
        }

        @Override
        public int hashCode() {
            return Objects.hash(steps);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", MultipleSortStepsImpl.class.getSimpleName() + "[", "]")
                    .add("steps=" + steps)
                    .toString();
        }
    }

    enum SortDirection {
        ASCENDING, DESCENDING
    }

    sealed interface ComposableSortStep extends SortBy {

        /**
         * Combines this field and the given field such that the latter is applied only when the former considered values equal.
         *
         * @param fieldName The other field name to sort
         * @param direction The direction
         * @return CompositeSortSteps
         */
        default MultipleSortSteps then(String fieldName, SortDirection direction) {
            return then(field(fieldName, direction));
        }

        /**
         * Combines this field and the stream version field such that the latter is applied only when the former considered values equal.
         *
         * @param direction The direction
         * @return CompositeSortSteps
         */
        default MultipleSortSteps thenStreamVersion(SortDirection direction) {
            return then(field(STREAM_VERSION, direction));
        }

        /**
         * Combines this field and the time version field such that the latter is applied only when the former considered values equal.
         *
         * @param direction The direction
         * @return CompositeSortSteps
         */
        default MultipleSortSteps thenTime(SortDirection direction) {
            return then(field(STREAM_VERSION, direction));
        }

        /**
         * Combines this sorting step and with another step such that the latter is applied only when the former considered values equal.
         *
         * @param next The next step.
         * @return A new instance of {@link SortBy}.
         */
        MultipleSortSteps then(ComposableSortStep next);
    }

    private static MultipleSortSteps combine(String field1, SortDirection direction, String[] fields) {
        Objects.requireNonNull(field1, "field1 cannot be null");
        MultipleSortSteps initialSortSteps = new MultipleSortStepsImpl(Collections.singletonList(SortBy.field(field1, direction)));
        final MultipleSortSteps stepsToUse;
        if (fields != null && fields.length >= 1) {
            stepsToUse = Stream.of(fields).map(f -> SortBy.field(f, direction))
                    .map(ComposableSortStep.class::cast)
                    .reduce(ComposableSortStep::then)
                    .map(initialSortSteps::thenMerge)
                    .orElseThrow(() -> new IllegalArgumentException("Internal error: Failed to "));
        } else {
            stepsToUse = initialSortSteps;
        }

        return stepsToUse;
    }
}