package org.occurrent.eventstore.jpa.utils;

import io.vavr.CheckedFunction0;

public class Sneaky {
  public static <T> T sneaky(CheckedFunction0<T> fn) {
    return fn.unchecked().apply();
  }
}
