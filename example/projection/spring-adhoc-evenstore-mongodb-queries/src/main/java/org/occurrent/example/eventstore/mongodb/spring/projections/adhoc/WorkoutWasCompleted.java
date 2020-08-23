/*
 * Copyright 2020 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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