package se.haleby.occurrent.eventstore.api;

import se.haleby.occurrent.condition.Condition;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.condition.Condition.eq;

public abstract class WriteCondition {

    private WriteCondition() {
    }

    public static WriteCondition anyStreamVersion() {
        return StreamVersionWriteCondition.any();
    }

    public static WriteCondition streamVersionEq(long version) {
        return streamVersion(eq(version));
    }

    public static WriteCondition streamVersion(Condition<Long> condition) {
        return StreamVersionWriteCondition.streamVersion(condition);
    }

    public boolean isAnyStreamVersion() {
        return this instanceof StreamVersionWriteCondition && ((StreamVersionWriteCondition) this).isAny();
    }

    public static class StreamVersionWriteCondition extends WriteCondition {
        public final Condition<Long> condition;

        private StreamVersionWriteCondition(Condition<Long> condition) {
            this.condition = condition;
        }

        public static StreamVersionWriteCondition streamVersion(Condition<Long> condition) {
            requireNonNull(condition, "Stream version condition cannot be null");
            return new StreamVersionWriteCondition(condition);
        }

        public static StreamVersionWriteCondition any() {
            return new StreamVersionWriteCondition(null);
        }

        @Override
        public String toString() {
            return condition.description;
        }

        public boolean isAny() {
            return condition == null;
        }
    }
}