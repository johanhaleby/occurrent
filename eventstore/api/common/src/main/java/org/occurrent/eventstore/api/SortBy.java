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
        return new NaturalImpl(direction);
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
        return new SingleFieldImpl(fieldName, direction);
    }

    public static abstract class Natural extends SortBy {
        private Natural() {
        }
    }

    public static final class NaturalImpl extends Natural {
        public final SortDirection direction;

        private NaturalImpl(SortDirection direction) {
            Objects.requireNonNull(direction, SortDirection.class.getSimpleName() + " cannot be null");
            this.direction = direction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof NaturalImpl)) return false;
            NaturalImpl natural = (NaturalImpl) o;
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

    public static abstract class SingleField extends ComposableSortStep {
        private SingleField() {
        }

        /**
         * Combines this sorting step and with another step such that the latter is applied only when the former considered values equal.
         *
         * @param direction The direction
         * @return A new instance of {@link SortBy}.
         */
        public SortBy thenNatural(SortDirection direction) {
            return then(new NaturalImpl(direction));
        }

        @Override
        public MultipleSortStepsImpl then(ComposableSortStep next) {
            return then((SortBy) next);
        }

        private MultipleSortStepsImpl then(SortBy other) {
            List<SortBy> list = Arrays.asList(this, other);
            return new MultipleSortStepsImpl(list);
        }
    }


    public static final class SingleFieldImpl extends SingleField {
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
            if (!(o instanceof SingleFieldImpl)) return false;
            SingleFieldImpl that = (SingleFieldImpl) o;
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

    public static abstract class MultipleSortSteps extends ComposableSortStep {
        private MultipleSortSteps() {
        }

        /**
         * Combines this sorting step and with another step such that the latter is applied only when the former considered values equal.
         *
         * @param direction The direction
         * @return A new instance of {@link SortBy}.
         */
        public SortBy thenNatural(SortDirection direction) {
            return thenMerge(new NaturalImpl(direction));
        }

        @Override
        public MultipleSortSteps then(ComposableSortStep next) {
            return thenMerge(next);
        }

        protected abstract MultipleSortSteps thenMerge(SortBy next);
    }

    public static final class MultipleSortStepsImpl extends MultipleSortSteps {
        public List<SortBy> steps;

        private MultipleSortStepsImpl(List<SortBy> steps) {
            Objects.requireNonNull(steps, "Steps cannot be null");
            if (steps.stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException("Steps cannot contain null step");
            }
            this.steps = Collections.unmodifiableList(steps);
        }

        @Override
        protected MultipleSortSteps thenMerge(SortBy next) {
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
            if (!(o instanceof MultipleSortStepsImpl)) return false;
            MultipleSortStepsImpl that = (MultipleSortStepsImpl) o;
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