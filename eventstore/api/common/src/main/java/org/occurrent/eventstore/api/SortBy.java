package org.occurrent.eventstore.api;

import java.util.*;

import static io.cloudevents.core.v1.CloudEventV1.TIME;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;

public abstract class SortBy {
    private SortBy() {
    }

    public static Natural natural(SortOrder order) {
        return new Natural(order);
    }

    public static SingleField time(SortOrder order) {
        return field(TIME, order);
    }

    public static SingleField streamVersion(SortOrder order) {
        return field(STREAM_VERSION, order);
    }

    public static SingleField field(String fieldName, SortOrder order) {
        return new SingleField(fieldName, order);
    }

    public static final class Natural extends SortBy {
        public final SortOrder order;

        private Natural(SortOrder order) {
            Objects.requireNonNull(order, SortOrder.class.getSimpleName() + " cannot be null");
            this.order = order;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Natural)) return false;
            Natural natural = (Natural) o;
            return order == natural.order;
        }

        @Override
        public int hashCode() {
            return Objects.hash(order);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Natural.class.getSimpleName() + "[", "]")
                    .add("order=" + order)
                    .toString();
        }


    }

    public static final class SingleField extends ComposableSortStep {
        public final String fieldName;
        public final SortOrder order;

        private SingleField(String fieldName, SortOrder order) {
            Objects.requireNonNull(fieldName, "Field name cannot be null");
            Objects.requireNonNull(order, SortOrder.class.getSimpleName() + " cannot be null");
            this.fieldName = fieldName;
            this.order = order;
        }

        public SortBy thenNatural(SortOrder order) {
            return then(new Natural(order));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SingleField)) return false;
            SingleField that = (SingleField) o;
            return Objects.equals(fieldName, that.fieldName) && order == that.order;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, order);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", SingleField.class.getSimpleName() + "[", "]")
                    .add("fieldName='" + fieldName + "'")
                    .add("order=" + order)
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

        public SortBy thenNatural(SortOrder order) {
            return then(new Natural(order));
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

    public enum SortOrder {
        ASCENDING, DESCENDING
    }

    public abstract static class ComposableSortStep extends SortBy {
        private ComposableSortStep() {
        }

        /**
         * Combines this field and the given field such that the latter is applied only when the former considered values equal.
         *
         * @param fieldName The other field name to sort
         * @param order     The order
         * @return CompositeSortSteps
         */
        public MultipleSortSteps then(String fieldName, SortOrder order) {
            return then(field(fieldName, order));
        }

        public abstract MultipleSortSteps then(ComposableSortStep next);
    }
}