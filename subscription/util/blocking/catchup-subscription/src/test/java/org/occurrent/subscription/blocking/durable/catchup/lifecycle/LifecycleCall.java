package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import org.jspecify.annotations.Nullable;

import java.util.List;

record LifecycleCall(Kind kind, @Nullable String id, StartFailure failure) {

    enum Kind {SUBSCRIBE, CANCEL, PAUSE, RESUME, START_FALSE, START_TRUE, STOP}

    enum StartFailure {NONE, BEFORE_EFFECT, AFTER_EFFECT}

    static LifecycleCall of(Kind kind, @Nullable String id) {
        return new LifecycleCall(kind, id, StartFailure.NONE);
    }

    boolean perId() {
        return id != null;
    }

    @Override
    public String toString() {
        String name = switch (kind) {
            case SUBSCRIBE -> "subscribe";
            case CANCEL -> "cancel";
            case PAUSE -> "pause";
            case RESUME -> "resume";
            case START_FALSE -> "start(false)";
            case START_TRUE -> "start(true)";
            case STOP -> "stop";
        };
        return name + (id == null ? "" : "(" + id + ")") + (failure == StartFailure.NONE ? "" : "!" + failure);
    }

    record Scenario(boolean composite, int historyDelayMicros, int startDelayMicros, List<List<LifecycleCall>> threads) {
        @Override
        public String toString() {
            return (composite ? "composite" : "plain") + " history=" + historyDelayMicros + "us start=" + startDelayMicros + "us " + threads;
        }
    }
}
