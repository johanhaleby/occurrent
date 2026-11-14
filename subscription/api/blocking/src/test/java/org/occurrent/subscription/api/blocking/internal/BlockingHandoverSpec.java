package org.occurrent.subscription.api.blocking.internal;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * What a handover owes whatever order its calls come in, read off the trace of one run.
 */
final class BlockingHandoverSpec {

    /**
     * An answer to a payload offered through one of the accept calls, null while it has none. failureFiredAfterInItsCatchUp
     * is true when an injected failure fired, after the offer was answered, in a catch-up that ended after it, up to
     * and including the first of those that did not return false. It is the only failure that can explain a reported
     * payload being lost.
     */
    record Offer(String payload, String call, String answer, boolean failureFiredAfterInItsCatchUp) {
    }

    record Trace(List<String> history, List<String> delivered, List<Offer> offers, List<String> markerFindings, List<String> notes, List<String> replayed) {
    }

    private BlockingHandoverSpec() {
    }

    /** The marker is written only for a catch-up that nothing stopped or cancelled before the marker step */
    static List<String> markerViolations(Trace trace) {
        return trace.markerFindings();
    }

    /**
     * acceptReportingDelivery reports a buffered payload before the drain applies it, so a failed fold or a failed
     * catch-up after that can lose it. acceptIfLive reports true only for a payload that landed.
     */
    static List<String> deliveryViolations(Trace trace) {
        List<String> violations = new ArrayList<>(trace.notes());
        // Each replay that reads an item may deliver it, and a live copy of it may too
        for (String item : trace.history()) {
            long times = trace.delivered().stream().filter(item::equals).count();
            long replays = trace.replayed().stream().filter(item::equals).count();
            long liveCopies = trace.offers().stream().filter(offer -> offer.payload().equals(item)).count();
            if (times > replays + liveCopies) {
                violations.add(item + " was delivered " + times + " times by " + replays + " replay reads and " + liveCopies + " live copies, in " + trace.delivered());
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
            } else if (!trace.history().contains(offer.payload())) {
                boolean delivered = trace.delivered().contains(offer.payload());
                boolean refused = offer.answer().startsWith("refused");
                if (refused && delivered) {
                    violations.add(offer.payload() + " was refused by " + offer.call() + " but delivered");
                }
                if (offer.call().equals("accept")) {
                    if (offer.answer().equals("applied") && !delivered) {
                        violations.add(offer.payload() + " was answered as applied but never delivered");
                    }
                } else if (offer.answer().equals("reported:true") && !delivered && (offer.call().equals("acceptIfLive") || !offer.failureFiredAfterInItsCatchUp())) {
                    violations.add(offer.payload() + " was reported delivered or buffered by " + offer.call() + " but never delivered");
                } else if (offer.answer().equals("reported:false") && delivered) {
                    violations.add(offer.payload() + " was reported not delivered by " + offer.call() + " but delivered");
                }
            }
        }
        return violations;
    }
}
