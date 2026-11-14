package org.occurrent.subscription.api.blocking.internal;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * What a handover owes whatever order its calls come in, read off the trace of one run.
 */
final class BlockingHandoverSpec {

    /** An answer to a payload offered through one of the accept calls, null while it has none. */
    record Offer(String payload, String call, String answer) {
    }

    record Trace(List<String> history, List<String> delivered, List<Offer> offers, List<String> markerFindings, List<String> notes, int replayPasses) {
    }

    private BlockingHandoverSpec() {
    }

    /** The marker is written only for a catch-up that nothing stopped or cancelled before the marker step */
    static List<String> markerViolations(Trace trace) {
        return trace.markerFindings();
    }

    static List<String> deliveryViolations(Trace trace) {
        List<String> violations = new ArrayList<>(trace.notes());
        // A catch-up that was stopped replays the whole history again on the next one, so history repeats once per pass, and a live copy of it may repeat it too
        for (String item : trace.history()) {
            long times = trace.delivered().stream().filter(item::equals).count();
            long liveCopies = trace.offers().stream().filter(offer -> offer.payload().equals(item)).count();
            if (times > trace.replayPasses() + liveCopies) {
                violations.add(item + " was delivered " + times + " times by " + trace.replayPasses() + " replay passes, in " + trace.delivered());
            }
        }
        Set<String> seen = new LinkedHashSet<>();
        for (String payload : trace.delivered()) {
            if (!trace.history().contains(payload) && !seen.add(payload)) {
                violations.add(payload + " was delivered twice, in " + trace.delivered());
            }
        }
        for (Offer offer : trace.offers()) {
            if (offer.answer() == null) {
                violations.add(offer.payload() + " offered through " + offer.call() + " got no answer");
            } else if (offer.call().equals("accept") && !trace.history().contains(offer.payload())) {
                boolean delivered = trace.delivered().contains(offer.payload());
                if (offer.answer().equals("applied") && !delivered) {
                    violations.add(offer.payload() + " was answered as applied but never delivered");
                }
                if (offer.answer().startsWith("refused") && delivered) {
                    violations.add(offer.payload() + " was refused but delivered");
                }
            }
        }
        return violations;
    }
}
