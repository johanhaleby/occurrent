package org.occurrent.eventstore.api;

import io.cloudevents.core.v1.CloudEventV1;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.util.*;

import static io.cloudevents.core.v1.CloudEventV1.TIME;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;

/**
 * A fluent API for specifying sort order for queries. For example:
 * <p></p>
 * <pre>
 * var allEvents = eventStoreQueries.all(SortBy.time(DESCENDING));
 * </pre>
 */
public abstract class SortBy {
    private SortBy() {
    }

    /**
     * Sort by natural order in the supplied direction. This is typically the insertion order,
     * but it could also be undefined for certain datastores.
     *
     * @param direction The direction
     * @return A new instance of {@link SortBy}.
     */
    public static Natural natural(SortDirection direction) {
        return new Natural(direction);
    }

    /**
     * Sort by time in the supplied direction.
     *
     * @param direction The direction
     * @return A new instance of {@link SortBy}.
     */
    public static SingleField time(SortDirection direction) {
        return field(TIME, direction);
    }

    /**
     * Sort by {@value OccurrentCloudEventExtension#STREAM_VERSION} in the supplied direction.
     *
     * @param direction The direction
     * @return A new instance of {@link SortBy}.
     */
    public static SingleField streamVersion(SortDirection direction) {
        return field(STREAM_VERSION, direction);
    }

    /**
     * Sort by the given field and direction. See {@link CloudEventV1} for valid fields.
     *
     * @param direction The direction
     * @return A new instance of {@link SortBy}.
     */
    public static SingleField field(String fieldName, SortDirection direction) {
        return new SingleField(fieldName, direction);
    }

    public static final class Natural extends SortBy {
        public final SortDirection direction;

        private Natural(SortDirection direction) {
            Objects.requireNonNull(direction, SortDirection.class.getSimpleName() + " cannot be null");
            this.direction = direction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Natural)) return false;
            Natural natural = (Natural) o;
            return direction == natural.direction;
        }

        @Override
        public int hashCode() {
            return Objects.hash(direction);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Natural.class.getSimpleName() + "[", "]")
                    .add("direction=" + direction)
                    .toString();
        }


    }

    public static final class SingleField extends ComposableSortStep {
        public final String fieldName;
        public final SortDirection direction;

        private SingleField(String fieldName, SortDirection direction) {
            Objects.requireNonNull(fieldName, "Field name cannot be null");
            Objects.requireNonNull(direction, SortDirection.class.getSimpleName() + " cannot be null");
            this.fieldName = fieldName;
            this.direction = direction;
        }

        /**
         * Combines this sorting step and with another step such that the latter is applied only when the former considered values equal.
         *
         * @param direction The direction
         * @return A new instance of {@link SortBy}.
         */
        public SortBy thenNatural(SortDirection direction) {
            return then(new Natural(direction));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SingleField)) return false;
            SingleField that = (SingleField) o;
            return Objects.equals(fieldName, that.fieldName) && direction == that.direction;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, direction);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", SingleField.class.getSimpleName() + "[", "]")
                    .add("fieldName='" + fieldName + "'")
                    .add("direction=" + direction)
                    .toString();
        }

        @Override
        public MultipleSortSteps then(ComposableSortStep next) {
            return then((SortBy) next);
        }

        private MultipleSortSteps then(SortBy other) {
            List<SortBy> list = Arrays.asList(this, other);
            return new MultipleSortSteps(list);
        }
    }

    public static final class MultipleSortSteps extends ComposableSortStep {
        public List<SortBy> steps;

        public MultipleSortSteps(List<SortBy> steps) {
            Objects.requireNonNull(steps, "Steps cannot be null");
            if (steps.stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException("Steps cannot contain null step");
            }
            this.steps = Collections.unmodifiableList(steps);
        }

        /**
         * Combines this sorting step and with another step such that the latter is applied only when the former considered values equal.
         *
         * @param direction The direction
         * @return A new instance of {@link SortBy}.
         */
        public SortBy thenNatural(SortDirection direction) {
            return then(new Natural(direction));
        }

        @Override
        public MultipleSortSteps then(ComposableSortStep next) {
            return then((SortBy) next);
        }

        private MultipleSortSteps then(SortBy next) {
            Objects.requireNonNull(next, ComposableSortStep.class.getSimpleName() + " cannot be null");
            List<SortBy> newSteps = new ArrayList<>(steps);
            if (next instanceof MultipleSortSteps) {
                newSteps.addAll(((MultipleSortSteps) next).steps);
            } else {
                newSteps.add(next);
            }
            return new MultipleSortSteps(newSteps);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MultipleSortSteps)) return false;
            MultipleSortSteps that = (MultipleSortSteps) o;
            return Objects.equals(steps, that.steps);
        }

        @Override
        public int hashCode() {
            return Objects.hash(steps);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", MultipleSortSteps.class.getSimpleName() + "[", "]")
                    .add("steps=" + steps)
                    .toString();
        }
    }

    public enum SortDirection {
        ASCENDING, DESCENDING
    }

    public abstract static class ComposableSortStep extends SortBy {
        private ComposableSortStep() {
        }

        /**
         * Combines this field and the given field such that the latter is applied only when the former considered values equal.
         *
         * @param fieldName The other field name to sort
         * @param direction The direction
         * @return CompositeSortSteps
         */
        public MultipleSortSteps then(String fieldName, SortDirection direction) {
            return then(field(fieldName, direction));
        }

        /**
         * Combines this sorting step and with another step such that the latter is applied only when the former considered values equal.
         *
         * @param next The next step.
         * @return A new instance of {@link SortBy}.
         */
        public abstract MultipleSortSteps then(ComposableSortStep next);
    }
}