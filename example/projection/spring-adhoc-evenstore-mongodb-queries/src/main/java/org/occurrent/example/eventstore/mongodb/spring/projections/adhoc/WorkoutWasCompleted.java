package org.occurrent.example.eventstore.mongodb.spring.projections.adhoc;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

public class WorkoutWasCompleted {

    private final UUID eventId;
    private final UUID workoutId;
    private final String completedBy;
    private final String activity;
    private final LocalDateTime completedAt;

    public WorkoutWasCompleted(UUID workoutId, LocalDateTime completedAt, String activity, String completedBy) {
        this.eventId = UUID.randomUUID();
        this.workoutId = workoutId;
        this.completedBy = completedBy;
        this.completedAt = completedAt;
        this.activity = activity;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getCompletedBy() {
        return completedBy;
    }

    public LocalDateTime getCompletedAt() {
        return completedAt;
    }

    public String getActivity() {
        return activity;
    }

    public UUID getWorkoutId() {
        return workoutId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkoutWasCompleted)) return false;
        WorkoutWasCompleted that = (WorkoutWasCompleted) o;
        return Objects.equals(eventId, that.eventId) &&
                Objects.equals(workoutId, that.workoutId) &&
                Objects.equals(completedBy, that.completedBy) &&
                Objects.equals(activity, that.activity) &&
                Objects.equals(completedAt, that.completedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, workoutId, completedBy, activity, completedAt);
    }

    @Override
    public String toString() {
        return "WorkoutWasCompletedEvent{" +
                "eventId=" + eventId +
                ", workoutId=" + workoutId +
                ", completedBy='" + completedBy + '\'' +
                ", activity='" + activity + '\'' +
                ", completedAt=" + completedAt +
                '}';
    }
}