package org.occurrent.application.service.blocking.implementation.support;

import org.junit.jupiter.api.Disabled;
import org.occurrent.domain.NameDefined;

import java.util.concurrent.atomic.AtomicInteger;

@Disabled
public class CountNumberOfNamesDefinedPolicy {

    AtomicInteger numberOfNamesDefined = new AtomicInteger();

    public void whenNameDefinedThenCountHowManyNamesThatHaveBeenDefined(NameDefined __) {
        numberOfNamesDefined.incrementAndGet();
    }

    public int getNumberOfNamesDefined() {
        return numberOfNamesDefined.get();
    }
}